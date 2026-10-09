package internal

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	pipelinev1 "pipeline-controller/api/v1"
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
			LastUpdated:   ago(killedAgo),
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

func TestEligibleVictims(t *testing.T) {
	batch := func(name, ns string, state pipelinev1.JobState) *pipelinev1.PipelineJob {
		return &pipelinev1.PipelineJob{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       pipelinev1.PipelineJobSpec{Priority: pipelinev1.PriorityBatch},
			Status:     pipelinev1.PipelineJobStatus{State: state},
		}
	}
	waiting := &pipelinev1.PipelineJob{
		ObjectMeta: metav1.ObjectMeta{Name: "urgent", Namespace: "default"},
		Spec:       pipelinev1.PipelineJobSpec{Priority: pipelinev1.PriorityRealtime},
	}
	names := func(js []*pipelinev1.PipelineJob) []string {
		var out []string
		for _, j := range js {
			out = append(out, j.Name)
		}
		return out
	}

	t.Run("only running or submitted batch jobs in the same namespace", func(t *testing.T) {
		maxed := batch("maxed", "default", pipelinev1.StateRunning)
		maxed.Status.EvictionCount = MaxEvictionCount
		rt := batch("rt", "default", pipelinev1.StateRunning)
		rt.Spec.Priority = pipelinev1.PriorityRealtime
		m := NewEvictionManager(newClient(t,
			batch("a", "default", pipelinev1.StateRunning),
			batch("b", "default", pipelinev1.StateSubmitted),
			batch("waiting-batch", "default", pipelinev1.StateReady),
			batch("other-namespace", "other", pipelinev1.StateRunning),
			maxed, rt,
		))
		got, err := m.EligibleVictims(context.Background(), waiting)
		if err != nil {
			t.Fatal(err)
		}
		if n := names(got); len(n) != 2 || !((n[0] == "a" && n[1] == "b") || (n[0] == "b" && n[1] == "a")) {
			t.Errorf("victims = %v, want [a b]", n)
		}
	})

	t.Run("no second eviction while one is in progress", func(t *testing.T) {
		m := NewEvictionManager(newClient(t,
			batch("running", "default", pipelinev1.StateRunning),
			batch("killing", "default", pipelinev1.StateKilling),
		))
		got, err := m.EligibleVictims(context.Background(), waiting)
		if err != nil || len(got) != 0 {
			t.Errorf("got (%v, %v), want no victims while a job is KILLING", names(got), err)
		}
	})
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

// ago returns a status timestamp d in the past.
func ago(d time.Duration) *metav1.Time {
	t := metav1.NewTime(time.Now().Add(-d))
	return &t
}
