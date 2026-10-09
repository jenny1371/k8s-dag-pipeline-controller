package internal

import (
	"context"
	"time"

	pipelinev1 "pipeline-controller/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	EvictionThreshold  = 5 * time.Second
	MaxEvictionCount   = 3
	KillConfirmTimeout = 120 * time.Second
)

type EvictionManager struct {
	client client.Client
}

func NewEvictionManager(c client.Client) *EvictionManager {
	return &EvictionManager{client: c}
}

// EligibleVictims returns the batch jobs that may be preempted on behalf of the
// waiting realtime job: running or submitted, in the same namespace, and not yet
// evicted MaxEvictionCount times. It returns nothing while another eviction is
// still in progress (a job is KILLING): its resources are not free yet, so
// evicting more would over-evict.
//
// Which of these victims is actually worth evicting is decided by
// AdmissionChecker.PickVictim, which checks that the waiting job would fit afterwards.
func (e *EvictionManager) EligibleVictims(ctx context.Context, waiting *pipelinev1.PipelineJob) ([]*pipelinev1.PipelineJob, error) {
	jobList := &pipelinev1.PipelineJobList{}
	if err := e.client.List(ctx, jobList); err != nil {
		return nil, err
	}

	for i := range jobList.Items {
		if jobList.Items[i].Status.State == pipelinev1.StateKilling {
			return nil, nil
		}
	}

	var victims []*pipelinev1.PipelineJob
	for i := range jobList.Items {
		job := &jobList.Items[i]
		if job.Namespace != waiting.Namespace || job.Spec.Priority != pipelinev1.PriorityBatch {
			continue
		}
		if job.Status.State != pipelinev1.StateRunning && job.Status.State != pipelinev1.StateSubmitted {
			continue
		}
		if job.Status.EvictionCount >= MaxEvictionCount {
			continue
		}
		victims = append(victims, job)
	}
	return victims, nil
}

// Evict preempts a job: it deletes the underlying K8s Job and moves the
// PipelineJob to KILLING.
func (e *EvictionManager) Evict(ctx context.Context, job *pipelinev1.PipelineJob) error {
	if err := e.deleteUnderlyingJob(ctx, job); err != nil {
		return err
	}

	job.Status.State = pipelinev1.StateKilling
	job.Status.EvictionCount++
	// KillConfirmTimeout is measured from this moment.
	now := metav1.Now()
	job.Status.LastUpdated = &now
	return e.client.Status().Update(ctx, job)
}

// ConfirmKilled reports whether the underlying K8s Job and all of its pods are gone.
// The caller moves the job back to WAITING once this returns true, or after
// KillConfirmTimeout has elapsed.
// evictionCount is not reset here; it is reset when the job completes (see reconciler.go handleRunning).
func (e *EvictionManager) ConfirmKilled(ctx context.Context, job *pipelinev1.PipelineJob) (bool, error) {
	jobName := "job-" + job.Name

	err := e.client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: job.Namespace}, &batchv1.Job{})
	if err == nil {
		return false, nil
	}
	if client.IgnoreNotFound(err) != nil {
		return false, err
	}

	pods := &corev1.PodList{}
	if err := e.client.List(ctx, pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"job-name": jobName},
	); err != nil {
		return false, err
	}
	return len(pods.Items) == 0, nil
}

// DeleteUnderlyingJob deletes the K8s batch/v1 Job (used by the reconciler's timeout and failure paths).
func (e *EvictionManager) DeleteUnderlyingJob(ctx context.Context, job *pipelinev1.PipelineJob) error {
	return e.deleteUnderlyingJob(ctx, job)
}

// deleteUnderlyingJob deletes the job-{name} K8s Job; a missing Job is not an error.
func (e *EvictionManager) deleteUnderlyingJob(ctx context.Context, job *pipelinev1.PipelineJob) error {
	underlyingJob := &batchv1.Job{}
	jobName := "job-" + job.Name
	err := e.client.Get(ctx, types.NamespacedName{
		Name:      jobName,
		Namespace: job.Namespace,
	}, underlyingJob)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			return nil
		}
		return err
	}

	propagation := metav1.DeletePropagationBackground
	if err := e.client.Delete(ctx, underlyingJob, &client.DeleteOptions{
		PropagationPolicy: &propagation,
	}); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return nil
		}
		ctrl.Log.Error(err, "failed to delete underlying job", "job", jobName)
		return err
	}

	ctrl.Log.Info("deleted underlying job", "job", jobName)
	return nil
}

// ShouldEvict reports whether a realtime job has waited long enough to trigger eviction.
func ShouldEvict(waitSince time.Time) bool {
	return time.Since(waitSince) > EvictionThreshold
}
