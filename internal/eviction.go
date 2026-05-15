package internal

import (
	"context"
	"time"

	pipelinev1 "pipeline-controller/api/v1"
	batchv1 "k8s.io/api/batch/v1"
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

// FindEvictionCandidate Eviction batch job（）
func (e *EvictionManager) FindEvictionCandidate(ctx context.Context) (*pipelinev1.PipelineJob, error) {
	jobList := &pipelinev1.PipelineJobList{}
	if err := e.client.List(ctx, jobList); err != nil {
		return nil, err
	}

	var candidate *pipelinev1.PipelineJob
	for i := range jobList.Items {
		job := &jobList.Items[i]
		if job.Spec.Priority != pipelinev1.PriorityBatch {
			continue
		}
		if job.Status.State != pipelinev1.StateRunning && job.Status.State != pipelinev1.StateSubmitted {
			continue
		}
		if job.Status.EvictionCount >= MaxEvictionCount {
			continue
		}
		// TODO: Resources priority ，
		candidate = job
		break
	}

	return candidate, nil
}

// Evict Eviction job：Delete K8s Job， PipelineJob KILLING
func (e *EvictionManager) Evict(ctx context.Context, job *pipelinev1.PipelineJob) error {
	if err := e.deleteUnderlyingJob(ctx, job); err != nil {
		return err
	}

	job.Status.State = pipelinev1.StateKilling
	job.Status.EvictionCount++
	job.Status.LastUpdated = time.Now().Format(time.RFC3339) // eviction ， KillConfirmTimeout
	return e.client.Status().Update(ctx, job)
}

// ConfirmKilled Verify job ， WAITING
// evictionCount reset，Completed reset（ reconciler.go handleRunning）
func (e *EvictionManager) ConfirmKilled(ctx context.Context, job *pipelinev1.PipelineJob) error {
	job.Status.State = pipelinev1.StateWaiting
	return e.client.Status().Update(ctx, job)
}

// ResetEvictionCount job Completed eviction ，Eviction
func (e *EvictionManager) ResetEvictionCount(ctx context.Context, job *pipelinev1.PipelineJob) error {
	job.Status.EvictionCount = 0
	return e.client.Status().Update(ctx, job)
}

// DeleteUnderlyingJob Delete K8s batch/v1 Job（ reconciler timeout Path）
func (e *EvictionManager) DeleteUnderlyingJob(ctx context.Context, job *pipelinev1.PipelineJob) error {
	return e.deleteUnderlyingJob(ctx, job)
}

// deleteUnderlyingJob ：Delete job-{name} K8s Job
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

// ShouldEvict Check realtime job Wait
func ShouldEvict(waitSince time.Time) bool {
	return time.Since(waitSince) > EvictionThreshold
}