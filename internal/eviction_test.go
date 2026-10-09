package internal

import (
	"context"
	"testing"
	"time"

	pipelinev1 "pipeline-controller/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newKillingFixture(t *testing.T, killedAgo time.Duration, withLingeringPod bool) (*PipelineJobReconciler, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pipelinev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	job := &pipelinev1.PipelineJob{
		ObjectMeta: metav1.ObjectMeta{Name: "victim", Namespace: "default"},
		Status: pipelinev1.PipelineJobStatus{
			State:         pipelinev1.StateKilling,
			EvictionCount: 1,
			LastUpdated:   time.Now().Add(-killedAgo).Format(time.RFC3339),
		},
	}
	objs := []client.Object{job}
	if withLingeringPod {
		// A pod of the already-deleted Job that has not terminated yet.
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-victim-abc",
				Namespace: "default",
				Labels:    map[string]string{"job-name": "job-victim"},
			},
		})
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&pipelinev1.PipelineJob{}).
		Build()

	return &PipelineJobReconciler{Client: c, Eviction: NewEvictionManager(c)}, c
}

func stateOf(t *testing.T, c client.Client) pipelinev1.JobState {
	t.Helper()
	got := &pipelinev1.PipelineJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "victim", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	return got.Status.State
}

func TestHandleKilling(t *testing.T) {
	tests := []struct {
		name         string
		killedAgo    time.Duration
		lingeringPod bool
		want         pipelinev1.JobState
	}{
		{"job and pods already gone: confirmed immediately", 2 * time.Second, false, pipelinev1.StateWaiting},
		{"pod still terminating, inside the window: keep waiting", 30 * time.Second, true, pipelinev1.StateKilling},
		{"pod still terminating, just inside the timeout", KillConfirmTimeout - 5*time.Second, true, pipelinev1.StateKilling},
		{"pod never goes away: forced to WAITING after KillConfirmTimeout", KillConfirmTimeout + 5*time.Second, true, pipelinev1.StateWaiting},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, c := newKillingFixture(t, tt.killedAgo, tt.lingeringPod)

			job := &pipelinev1.PipelineJob{}
			if err := c.Get(context.Background(), types.NamespacedName{Name: "victim", Namespace: "default"}, job); err != nil {
				t.Fatal(err)
			}
			if _, err := r.handleKilling(context.Background(), job); err != nil {
				t.Fatalf("handleKilling returned error: %v", err)
			}

			if got := stateOf(t, c); got != tt.want {
				t.Errorf("state = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestConfirmKilled_JobStillExists(t *testing.T) {
	r, c := newKillingFixture(t, time.Second, false)
	if err := c.Create(context.Background(), &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job-victim", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	job := &pipelinev1.PipelineJob{ObjectMeta: metav1.ObjectMeta{Name: "victim", Namespace: "default"}}
	confirmed, err := r.Eviction.ConfirmKilled(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed {
		t.Error("ConfirmKilled = true while the underlying Job still exists, want false")
	}
}

