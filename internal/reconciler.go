package internal

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	pipelinev1 "pipeline-controller/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	DefaultTimeout = 300 * time.Second
	RetryBudget    = 2
)

// WorkerConfig describes how the worker pods are built.
type WorkerConfig struct {
	Image           string
	ImagePullPolicy corev1.PullPolicy
	// MinioEndpoint is the address the worker pod uses to reach MinIO/S3. It is
	// separate from the controller's own MINIO_ENDPOINT because the controller may
	// run outside the cluster (localhost) while workers must use the in-cluster address.
	MinioEndpoint string
	AccessKey     string
	SecretKey     string
	// TerminationGracePeriodSeconds is how long a worker pod gets between SIGTERM and
	// SIGKILL. The worker script runs as PID 1 and does not react to SIGTERM, so the
	// full period is always spent; keeping it short is what makes an evicted job's
	// capacity come back quickly. Zero leaves the Kubernetes default (30s).
	TerminationGracePeriodSeconds int64
}

// DefaultWorkerTerminationGrace is the worker pod's SIGTERM-to-SIGKILL window in seconds.
const DefaultWorkerTerminationGrace = 5

// NewWorkerConfigFromEnv reads:
//
//	WORKER_IMAGE              (default "pipeline-job:latest")
//	WORKER_IMAGE_PULL_POLICY  (default "IfNotPresent")
//	WORKER_MINIO_ENDPOINT     (default "http://minio.minio:9000")
//	MINIO_ACCESS_KEY / MINIO_SECRET_KEY (default "minioadmin")
//	WORKER_TERMINATION_GRACE_SECONDS (default 5)
func NewWorkerConfigFromEnv() WorkerConfig {
	grace := int64(DefaultWorkerTerminationGrace)
	if v := getEnv("WORKER_TERMINATION_GRACE_SECONDS", ""); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			grace = n
		} else {
			ctrl.Log.Info("ignoring invalid WORKER_TERMINATION_GRACE_SECONDS", "value", v)
		}
	}
	return WorkerConfig{
		TerminationGracePeriodSeconds: grace,
		Image:                         getEnv("WORKER_IMAGE", "pipeline-job:latest"),
		ImagePullPolicy:               corev1.PullPolicy(getEnv("WORKER_IMAGE_PULL_POLICY", string(corev1.PullIfNotPresent))),
		MinioEndpoint:                 getEnv("WORKER_MINIO_ENDPOINT", "http://minio.minio:9000"),
		AccessKey:                     getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		SecretKey:                     getEnv("MINIO_SECRET_KEY", "minioadmin"),
	}
}

type PipelineJobReconciler struct {
	client.Client
	// APIReader reads straight from the API server, bypassing the informer cache.
	// It is used where a stale read would be harmful (checking whether the worker
	// Job already exists). When nil, the cached client is used.
	APIReader   client.Reader
	DAG         *DAGRegistry
	Admission   *AdmissionChecker
	Eviction    *EvictionManager
	Storage     *StorageChecker
	Worker      WorkerConfig
	admissionMu sync.Mutex
}

func (r *PipelineJobReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// jobKey namespaces a job name so same-named jobs in different namespaces
// do not collide in the DAG registry.
func jobKey(namespace, name string) string {
	return namespace + "/" + name
}

func nowTime() *metav1.Time {
	t := metav1.Now()
	return &t
}

// since returns how long ago t was, or 0 when it is unset.
func since(t *metav1.Time) time.Duration {
	if t == nil {
		return 0
	}
	return time.Since(t.Time)
}

func (r *PipelineJobReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	job := &pipelinev1.PipelineJob{}
	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		if apierrors.IsNotFound(err) {
			// The PipelineJob was deleted; its underlying Job is garbage-collected
			// through the OwnerReference, so only the in-memory DAG needs cleaning.
			r.DAG.Remove(jobKey(req.Namespace, req.Name))
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	ctrl.Log.Info("reconciling", "job", job.Name, "state", job.Status.State)

	switch job.Status.State {
	case "":
		return r.initialize(ctx, job)
	case pipelinev1.StateWaiting:
		return r.handleWaiting(ctx, job)
	case pipelinev1.StateReady:
		return r.handleReady(ctx, job)
	case pipelinev1.StateSubmitted, pipelinev1.StateRunning:
		return r.handleRunning(ctx, job)
	case pipelinev1.StateKilling:
		return r.handleKilling(ctx, job)
	case pipelinev1.StateTimedOut:
		return r.handleTimedOut(ctx, job)
	}

	return reconcile.Result{}, nil
}

// initialize gives a brand-new job an explicit WAITING state so that
// `kubectl get pipelinejobs` never shows an empty STATE column.
func (r *PipelineJobReconciler) initialize(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	job.Status.State = pipelinev1.StateWaiting
	job.Status.LastUpdated = nowTime()
	if err := r.Status().Update(ctx, job); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{Requeue: true}, nil
}

// fail moves the job to the terminal FAILED state and records why.
func (r *PipelineJobReconciler) fail(ctx context.Context, job *pipelinev1.PipelineJob, reason string) (reconcile.Result, error) {
	ctrl.Log.Info("job_event", "job", job.Name, "event", "FAILED", "reason", reason, "ts", time.Now().UnixMilli())
	job.Status.State = pipelinev1.StateFailed
	job.Status.Reason = reason
	job.Status.LastUpdated = nowTime()
	if err := r.Status().Update(ctx, job); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// validateSpec rejects specs the controller could never run, so they fail fast
// with a clear reason instead of panicking or hanging later.
func validateSpec(job *pipelinev1.PipelineJob) error {
	if _, err := parseRequest(job.Spec.RequestCPU, job.Spec.RequestMemory); err != nil {
		return fmt.Errorf("invalid requestCPU/requestMemory: %w", err)
	}
	if _, _, err := parseS3Path(job.Spec.StorageMarker); err != nil {
		return fmt.Errorf("invalid storageMarker: %w", err)
	}
	switch job.Spec.Priority {
	case pipelinev1.PriorityRealtime, pipelinev1.PriorityBatch:
	default:
		return fmt.Errorf("invalid priority %q (want realtime or batch)", job.Spec.Priority)
	}
	return nil
}

// waitingReason explains what a WAITING job is waiting for, e.g. a dependency
// name with a typo that will never exist.
func waitingReason(job *pipelinev1.PipelineJob, states map[string]pipelinev1.JobState) string {
	for _, dep := range job.Spec.Dependencies {
		state, ok := states[jobKey(job.Namespace, dep)]
		if !ok {
			return fmt.Sprintf("waiting: upstream job %q not found in namespace %q", dep, job.Namespace)
		}
		if state != pipelinev1.StateDone {
			shown := string(state)
			if shown == "" {
				shown = string(pipelinev1.StateWaiting)
			}
			return fmt.Sprintf("waiting for upstream job %q (%s)", dep, shown)
		}
	}
	return ""
}

func (r *PipelineJobReconciler) handleWaiting(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	if err := validateSpec(job); err != nil {
		ctrl.Log.Error(err, "invalid job spec", "job", job.Name)
		return r.fail(ctx, job, err.Error())
	}

	key := jobKey(job.Namespace, job.Name)
	deps := make([]string, 0, len(job.Spec.Dependencies))
	for _, d := range job.Spec.Dependencies {
		deps = append(deps, jobKey(job.Namespace, d))
	}

	// DAG.Add rejects cycles; a job that would create one can never run.
	if err := r.DAG.Add(key, deps); err != nil {
		ctrl.Log.Error(err, "cycle detected in dependency graph", "job", job.Name)
		return r.fail(ctx, job, err.Error())
	}

	states, err := r.listStates(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}

	// A failed upstream will never become DONE, so fail instead of waiting forever.
	for i, dep := range deps {
		if states[dep] == pipelinev1.StateFailed {
			return r.fail(ctx, job, fmt.Sprintf("upstream job %q failed", job.Spec.Dependencies[i]))
		}
	}

	doneSet := make(map[string]bool)
	for k, s := range states {
		if s == pipelinev1.StateDone {
			doneSet[k] = true
		}
	}

	if r.DAG.AllUpstreamDone(key, doneSet) {
		job.Status.State = pipelinev1.StateReady
		job.Status.Reason = ""
		job.Status.LastUpdated = nowTime()
		ctrl.Log.Info("job_event", "job", job.Name, "event", "READY", "ts", time.Now().UnixMilli())
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}

	// Still waiting: say what for (and flag upstreams that do not exist).
	if reason := waitingReason(job, states); reason != job.Status.Reason {
		job.Status.Reason = reason
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
}

func (r *PipelineJobReconciler) handleReady(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()

	latest := &pipelinev1.PipelineJob{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(job), latest); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	job = latest

	ok, err := r.Admission.HasCapacity(ctx, job)
	if err != nil {
		return reconcile.Result{}, err
	}

	if ok {
		ctrl.Log.Info("creating underlying job", "job", job.Name)
		if err := r.ensureUnderlyingJob(ctx, job); err != nil {
			ctrl.Log.Error(err, "failed to create underlying job", "job", job.Name)
			return reconcile.Result{}, err
		}
		job.Status.State = pipelinev1.StateSubmitted
		job.Status.Reason = ""
		job.Status.LastUpdated = nowTime()
		ctrl.Log.Info("job_event", "job", job.Name, "event", "SUBMITTED", "ts", time.Now().UnixMilli())
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if job.Spec.Priority == pipelinev1.PriorityRealtime && job.Status.LastUpdated != nil {
		waited := since(job.Status.LastUpdated)
		ctrl.Log.Info("eviction check", "job", job.Name, "waited", waited, "threshold", EvictionThreshold, "shouldEvict", ShouldEvict(job.Status.LastUpdated.Time))
		if ShouldEvict(job.Status.LastUpdated.Time) {
			if err := r.evictFor(ctx, job); err != nil {
				return reconcile.Result{}, err
			}
		}
	}

	return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
}

// evictFor preempts a batch job on behalf of a waiting realtime job, but only a
// victim whose removal actually lets the realtime job fit on a node.
func (r *PipelineJobReconciler) evictFor(ctx context.Context, waiting *pipelinev1.PipelineJob) error {
	victims, err := r.Eviction.EligibleVictims(ctx, waiting)
	if err != nil {
		return err
	}
	victim, err := r.Admission.PickVictim(ctx, waiting, victims)
	if err != nil {
		return err
	}
	if victim == nil {
		ctrl.Log.Info("no eviction would make room", "job", waiting.Name, "candidates", len(victims))
		return nil
	}

	ctrl.Log.Info("evicting", "candidate", victim.Name, "for", waiting.Name)
	ctrl.Log.Info("job_event", "job", victim.Name, "event", "EVICTED", "ts", time.Now().UnixMilli())
	// Evict deletes the K8s Job and moves the victim to KILLING.
	return r.Eviction.Evict(ctx, victim)
}

// underlyingJobFailed reports whether the batch Job has terminally failed.
func underlyingJobFailed(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// retryOrFail deletes the underlying Job and either schedules a retry
// (TIMED_OUT -> WAITING) or, once RetryBudget is used up, fails permanently.
func (r *PipelineJobReconciler) retryOrFail(ctx context.Context, job *pipelinev1.PipelineJob, reason string) (reconcile.Result, error) {
	// The Job must really be gone before the job is retried, otherwise the retry
	// could adopt the old, failed Job instead of starting a fresh one.
	if err := r.Eviction.DeleteUnderlyingJob(ctx, job); err != nil {
		ctrl.Log.Error(err, "failed to delete underlying job", "job", job.Name)
		return reconcile.Result{}, err
	}

	if job.Status.RetryCount < RetryBudget {
		job.Status.State = pipelinev1.StateTimedOut
	} else {
		job.Status.State = pipelinev1.StateFailed
	}
	job.Status.Reason = reason
	job.Status.LastUpdated = nowTime()
	ctrl.Log.Info("job_event", "job", job.Name, "event", string(job.Status.State), "reason", reason, "ts", time.Now().UnixMilli())
	if err := r.Status().Update(ctx, job); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *PipelineJobReconciler) handleRunning(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	// A storage error (network, credentials, ...) is returned, not treated as
	// "not finished", so it shows up in logs/backoff instead of silently timing out.
	markerExists, err := r.Storage.MarkerExists(ctx, job.Spec.StorageMarker)
	if err != nil {
		ctrl.Log.Error(err, "storage check failed", "job", job.Name, "marker", job.Spec.StorageMarker)
		return reconcile.Result{}, err
	}
	ctrl.Log.Info("storage check", "job", job.Name, "marker", job.Spec.StorageMarker, "exists", markerExists)
	if markerExists {
		// Fetch the latest copy to avoid a resourceVersion conflict.
		latest := &pipelinev1.PipelineJob{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(job), latest); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
		ctrl.Log.Info("job_event", "job", latest.Name, "event", "DONE", "ts", time.Now().UnixMilli())
		latest.Status.State = pipelinev1.StateDone
		latest.Status.Reason = ""
		latest.Status.LastUpdated = nowTime()
		latest.Status.EvictionCount = 0
		if err := r.Status().Update(ctx, latest); err != nil {
			return reconcile.Result{RequeueAfter: time.Second}, err
		}
		return reconcile.Result{}, nil
	}

	underlying := &batchv1.Job{}
	err = r.Get(ctx, types.NamespacedName{Name: "job-" + job.Name, Namespace: job.Namespace}, underlying)
	switch {
	case err == nil:
		// The worker exited with an error: its marker will never appear.
		if underlyingJobFailed(underlying) {
			return r.retryOrFail(ctx, job, "underlying job failed")
		}
		// SUBMITTED -> RUNNING once the underlying K8s Job has an active pod.
		// LastUpdated is left untouched so the timeout still counts from submission.
		if job.Status.State == pipelinev1.StateSubmitted && underlying.Status.Active > 0 {
			job.Status.State = pipelinev1.StateRunning
			ctrl.Log.Info("job_event", "job", job.Name, "event", "RUNNING", "ts", time.Now().UnixMilli())
			if err := r.Status().Update(ctx, job); err != nil {
				return reconcile.Result{RequeueAfter: time.Second}, err
			}
			return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
		}
	case !apierrors.IsNotFound(err):
		return reconcile.Result{}, err
	}

	// Without a timestamp there is nothing to measure the timeout from; stamp it
	// now rather than treating the job as instantly overdue.
	if job.Status.LastUpdated == nil {
		job.Status.LastUpdated = nowTime()
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
	}

	timeout := DefaultTimeout
	if job.Spec.TimeoutSeconds > 0 {
		timeout = time.Duration(job.Spec.TimeoutSeconds) * time.Second
	}

	if since(job.Status.LastUpdated) > timeout {
		return r.retryOrFail(ctx, job, "timed out")
	}

	return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
}

func (r *PipelineJobReconciler) handleKilling(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	// Delete the K8s Job (a no-op if Evict already removed it).
	if err := r.Eviction.DeleteUnderlyingJob(ctx, job); err != nil {
		ctrl.Log.Error(err, "delete underlying job in killing state failed", "job", job.Name)
	}

	// Re-queue once the kill is confirmed, or after KillConfirmTimeout at the latest.
	confirmed, err := r.Eviction.ConfirmKilled(ctx, job)
	if err != nil {
		return reconcile.Result{}, err
	}

	if confirmed || since(job.Status.LastUpdated) > KillConfirmTimeout {
		job.Status.State = pipelinev1.StateWaiting
		job.Status.LastUpdated = nowTime()
		ctrl.Log.Info("job_event", "job", job.Name, "event", "KILL_CONFIRMED", "confirmed", confirmed, "ts", time.Now().UnixMilli())
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *PipelineJobReconciler) handleTimedOut(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	job.Status.RetryCount++
	job.Status.State = pipelinev1.StateWaiting
	job.Status.LastUpdated = nowTime()
	if err := r.Status().Update(ctx, job); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// listStates returns the state of every PipelineJob, keyed by namespace/name.
func (r *PipelineJobReconciler) listStates(ctx context.Context) (map[string]pipelinev1.JobState, error) {
	jobList := &pipelinev1.PipelineJobList{}
	if err := r.List(ctx, jobList); err != nil {
		return nil, err
	}

	states := make(map[string]pipelinev1.JobState, len(jobList.Items))
	for _, j := range jobList.Items {
		states[jobKey(j.Namespace, j.Name)] = j.Status.State
	}
	return states, nil
}

// ensureUnderlyingJob makes sure the worker Job for this run exists.
//
//   - A live Job already owned by this PipelineJob (created by an earlier attempt
//     whose status update failed) is adopted as is. The marker is NOT cleared in
//     that case: the worker may already have finished and written it.
//   - Otherwise any stale marker from an earlier run is cleared and a new Job is created.
func (r *PipelineJobReconciler) ensureUnderlyingJob(ctx context.Context, job *pipelinev1.PipelineJob) error {
	existing := &batchv1.Job{}
	err := r.reader().Get(ctx, types.NamespacedName{Name: "job-" + job.Name, Namespace: job.Namespace}, existing)
	switch {
	case err == nil:
		return r.checkExistingJob(ctx, job, existing)
	case !apierrors.IsNotFound(err):
		return err
	}

	// No Job yet: drop any marker left by a previous run (re-applied pipeline,
	// retry) so it cannot make this run look finished.
	if err := r.Storage.ClearMarker(ctx, job.Spec.StorageMarker); err != nil {
		return err
	}
	return r.createUnderlyingJob(ctx, job)
}

// checkExistingJob decides what to do with a Job that already has our name: one
// that is still terminating or that belongs to an older PipelineJob with the
// same name must not be reused, so ask for a retry once it is gone.
func (r *PipelineJobReconciler) checkExistingJob(ctx context.Context, job *pipelinev1.PipelineJob, existing *batchv1.Job) error {
	if existing.DeletionTimestamp != nil {
		return fmt.Errorf("previous underlying job %q is still terminating", existing.Name)
	}
	if !metav1.IsControlledBy(existing, job) {
		propagation := metav1.DeletePropagationBackground
		if err := r.Delete(ctx, existing, &client.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return fmt.Errorf("removed stale underlying job %q left by an earlier PipelineJob, retrying", existing.Name)
	}
	return nil
}

func (r *PipelineJobReconciler) createUnderlyingJob(ctx context.Context, job *pipelinev1.PipelineJob) error {
	duration := job.Spec.JobDurationSeconds
	if duration == 0 {
		duration = 30
	}

	bucket, key, err := parseS3Path(job.Spec.StorageMarker)
	if err != nil {
		return err
	}
	cpu, err := resource.ParseQuantity(job.Spec.RequestCPU)
	if err != nil {
		return err
	}
	mem, err := resource.ParseQuantity(job.Spec.RequestMemory)
	if err != nil {
		return err
	}

	w := r.Worker
	backoffLimit := int32(0)
	var grace *int64
	if w.TerminationGracePeriodSeconds > 0 {
		grace = &w.TerminationGracePeriodSeconds
	}
	underlyingJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-" + job.Name,
			Namespace: job.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					TerminationGracePeriodSeconds: grace,
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           w.Image,
							ImagePullPolicy: w.ImagePullPolicy,
							// Real requests: the K8s scheduler enforces what admission control accounts for.
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    cpu,
									corev1.ResourceMemory: mem,
								},
							},
							Env: []corev1.EnvVar{
								{Name: "JOB_NAME", Value: job.Name},
								{Name: "DURATION", Value: fmt.Sprintf("%d", duration)},
								{Name: "BUCKET", Value: bucket},
								{Name: "MARKER_PATH", Value: bucket + "/" + key},
								{Name: "STAGE", Value: job.Spec.Stage},
								{Name: "MINIO_ENDPOINT", Value: w.MinioEndpoint},
								{Name: "MINIO_ACCESS_KEY", Value: w.AccessKey},
								{Name: "MINIO_SECRET_KEY", Value: w.SecretKey},
							},
						},
					},
				},
			},
		},
	}

	// Garbage-collect the Job when the PipelineJob is deleted, and map Job events back to it.
	if err := controllerutil.SetControllerReference(job, underlyingJob, r.Scheme()); err != nil {
		return err
	}

	// AlreadyExists here means another reconcile created the Job between our
	// check and this call; return the error so the next attempt re-checks it.
	return r.Create(ctx, underlyingJob)
}

// upstreamFinished reports whether a PipelineJob update moved it into DONE or
// FAILED, the two states that unblock (or fail) the jobs depending on it.
func upstreamFinished(oldJob, newJob *pipelinev1.PipelineJob) bool {
	if oldJob.Status.State == newJob.Status.State {
		return false
	}
	return newJob.Status.State == pipelinev1.StateDone || newJob.Status.State == pipelinev1.StateFailed
}

// dependentsOf maps a finished PipelineJob to the jobs in its namespace that list
// it as a dependency, so they are reconciled immediately instead of on the next poll.
func (r *PipelineJobReconciler) dependentsOf(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &pipelinev1.PipelineJobList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		ctrl.Log.Error(err, "failed to list dependents", "job", obj.GetName())
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		j := &list.Items[i]
		for _, dep := range j.Spec.Dependencies {
			if dep == obj.GetName() {
				reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
				break
			}
		}
	}
	return reqs
}

func (r *PipelineJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pipelinev1.PipelineJob{}).
		// Re-reconcile when an owned Job changes (pod started, job failed) instead of waiting for the poll.
		Owns(&batchv1.Job{}).
		// Wake downstream jobs as soon as an upstream finishes; the 3s requeue stays as a fallback.
		Watches(&pipelinev1.PipelineJob{}, handler.EnqueueRequestsFromMapFunc(r.dependentsOf),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return false },
				DeleteFunc:  func(event.DeleteEvent) bool { return false },
				GenericFunc: func(event.GenericEvent) bool { return false },
				UpdateFunc: func(e event.UpdateEvent) bool {
					o, ok1 := e.ObjectOld.(*pipelinev1.PipelineJob)
					n, ok2 := e.ObjectNew.(*pipelinev1.PipelineJob)
					return ok1 && ok2 && upstreamFinished(o, n)
				},
			})).
		Complete(r)
}
