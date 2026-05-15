package internal

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pipelinev1 "pipeline-controller/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	GracePeriod    = 60 * time.Second
	DefaultTimeout = 300 * time.Second
	RetryBudget    = 2
)

type PipelineJobReconciler struct {
	client.Client
	DAG         *DAGRegistry
	Admission   *AdmissionChecker
	Eviction    *EvictionManager
	Storage     *StorageChecker
	admissionMu sync.Mutex
}

func (r *PipelineJobReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	job := &pipelinev1.PipelineJob{}
	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	ctrl.Log.Info("reconciling", "job", job.Name, "state", job.Status.State)

	switch job.Status.State {
	case "", pipelinev1.StateWaiting:
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

func (r *PipelineJobReconciler) handleWaiting(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	// DAG.Add cycle detection， cycle FAILED
	if err := r.DAG.Add(job.Name, job.Spec.Dependencies); err != nil {
		ctrl.Log.Error(err, "cycle detected in dependency graph", "job", job.Name)
		job.Status.State = pipelinev1.StateFailed
		job.Status.LastUpdated = time.Now().Format(time.RFC3339)
		if updateErr := r.Status().Update(ctx, job); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, nil
	}

	doneSet, err := r.buildDoneSet(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}

	if r.DAG.AllUpstreamDone(job.Name, doneSet) {
		job.Status.State = pipelinev1.StateReady
		job.Status.LastUpdated = time.Now().Format(time.RFC3339)
		ctrl.Log.Info("job_event", "job", job.Name, "event", "READY", "ts", time.Now().UnixMilli())
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
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

	ok, err := r.Admission.HasCapacity(ctx, job.Spec.RequestCPU, job.Spec.RequestMemory)
	if err != nil {
		return reconcile.Result{}, err
	}

	if ok {
		ctrl.Log.Info("creating underlying job", "job", job.Name)
		if err := r.createUnderlyingJob(ctx, job); err != nil {
			ctrl.Log.Error(err, "failed to create underlying job", "job", job.Name)
			return reconcile.Result{}, err
		}
		job.Status.State = pipelinev1.StateSubmitted
		job.Status.LastUpdated = time.Now().Format(time.RFC3339)
		ctrl.Log.Info("job_event", "job", job.Name, "event", "SUBMITTED", "ts", time.Now().UnixMilli())
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if job.Spec.Priority == pipelinev1.PriorityRealtime {
		waitSince, err := time.Parse(time.RFC3339, job.Status.LastUpdated)
		if err != nil {
			ctrl.Log.Info("waitSince parse error", "job", job.Name, "lastUpdated", job.Status.LastUpdated, "err", err)
		} else {
			waited := time.Since(waitSince)
			ctrl.Log.Info("eviction check", "job", job.Name, "waited", waited, "threshold", EvictionThreshold, "shouldEvict", ShouldEvict(waitSince))
			if ShouldEvict(waitSince) {
				candidate, err := r.Eviction.FindEvictionCandidate(ctx)
				if err != nil {
					return reconcile.Result{}, err
				}
				if candidate != nil {
					ctrl.Log.Info("evicting", "candidate", candidate.Name)
					ctrl.Log.Info("job_event", "job", candidate.Name, "event", "EVICTED", "ts", time.Now().UnixMilli())
					// Evict Delete K8s Job Update KILLING
					if err := r.Eviction.Evict(ctx, candidate); err != nil {
						return reconcile.Result{}, err
					}
				} else {
					ctrl.Log.Info("no eviction candidate found")
				}
			}
		}
	}

	return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
}

func (r *PipelineJobReconciler) handleRunning(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	markerExists, err := r.Storage.MarkerExists(ctx, job.Spec.StorageMarker)
	if err != nil {
		ctrl.Log.Error(err, "storage check failed", "job", job.Name, "marker", job.Spec.StorageMarker)
		return reconcile.Result{}, err
	}
	ctrl.Log.Info("storage check", "job", job.Name, "marker", job.Spec.StorageMarker, "exists", markerExists)
	if markerExists {
		// fetch ，Prevent resourceVersion
		latest := &pipelinev1.PipelineJob{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(job), latest); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
		ctrl.Log.Info("job_event", "job", latest.Name, "event", "DONE", "ts", time.Now().UnixMilli())
		latest.Status.State = pipelinev1.StateDone
		latest.Status.LastUpdated = time.Now().Format(time.RFC3339)
		latest.Status.EvictionCount = 0
		if err := r.Status().Update(ctx, latest); err != nil {
			return reconcile.Result{RequeueAfter: time.Second}, err
		}
		return reconcile.Result{}, nil
	}

	startTime, _ := time.Parse(time.RFC3339, job.Status.LastUpdated)
	timeout := DefaultTimeout
	if job.Spec.TimeoutSeconds > 0 {
		timeout = time.Duration(job.Spec.TimeoutSeconds) * time.Second
	}

	if time.Since(startTime) > timeout {
		// Delete K8s Job，Prevent zombie job Resources
		if err := r.Eviction.DeleteUnderlyingJob(ctx, job); err != nil {
			ctrl.Log.Error(err, "failed to delete underlying job on timeout", "job", job.Name)
			// ，log
		}

		if job.Status.RetryCount < RetryBudget {
			job.Status.State = pipelinev1.StateTimedOut
		} else {
			job.Status.State = pipelinev1.StateFailed
		}
		job.Status.LastUpdated = time.Now().Format(time.RFC3339)
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
}

func (r *PipelineJobReconciler) handleKilling(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	killTime, _ := time.Parse(time.RFC3339, job.Status.LastUpdated)

	// Delete K8s Job（ Evict not found，）
	if err := r.Eviction.DeleteUnderlyingJob(ctx, job); err != nil {
		ctrl.Log.Error(err, "delete underlying job in killing state failed", "job", job.Name)
	}

	if time.Since(killTime) > KillConfirmTimeout {
		job.Status.State = pipelinev1.StateWaiting
		job.Status.LastUpdated = time.Now().Format(time.RFC3339)
		ctrl.Log.Info("job_event", "job", job.Name, "event", "KILL_CONFIRMED", "ts", time.Now().UnixMilli())
		if err := r.Status().Update(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if err := r.Eviction.ConfirmKilled(ctx, job); err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *PipelineJobReconciler) handleTimedOut(ctx context.Context, job *pipelinev1.PipelineJob) (reconcile.Result, error) {
	job.Status.RetryCount++
	job.Status.State = pipelinev1.StateWaiting
	job.Status.LastUpdated = time.Now().Format(time.RFC3339)
	if err := r.Status().Update(ctx, job); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *PipelineJobReconciler) buildDoneSet(ctx context.Context) (map[string]bool, error) {
	jobList := &pipelinev1.PipelineJobList{}
	if err := r.List(ctx, jobList); err != nil {
		return nil, err
	}

	doneSet := make(map[string]bool)
	for _, j := range jobList.Items {
		if j.Status.State == pipelinev1.StateDone {
			doneSet[j.Name] = true
		}
	}
	return doneSet, nil
}

func (r *PipelineJobReconciler) createUnderlyingJob(ctx context.Context, job *pipelinev1.PipelineJob) error {
	duration := job.Spec.JobDurationSeconds
	if duration == 0 {
		duration = 30
	}

	markerPath := fmt.Sprintf("test-bucket/%s", job.Spec.StorageMarker[len("s3://test-bucket/"):])

	backoffLimit := int32(0)
	underlyingJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-" + job.Name,
			Namespace: job.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           "pipeline-job:latest",
							ImagePullPolicy: corev1.PullNever,
							Env: []corev1.EnvVar{
								{Name: "JOB_NAME", Value: job.Name},
								{Name: "DURATION", Value: fmt.Sprintf("%d", duration)},
								{Name: "MARKER_PATH", Value: markerPath},
								{Name: "STAGE", Value: job.Spec.Stage},
							},
						},
					},
				},
			},
		},
	}

	err := r.Create(ctx, underlyingJob)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

func (r *PipelineJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pipelinev1.PipelineJob{}).
		Complete(r)
}