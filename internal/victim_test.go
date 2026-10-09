package internal

import (
	"context"
	"strings"
	"testing"
	"time"

	pipelinev1 "pipeline-controller/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// workerPod is the pod the Job controller would create for the PipelineJob `name`.
func workerPod(name, nodeName, cpu, mem string) *corev1.Pod {
	p := pod("job-"+name+"-x", nodeName, cpu, mem, corev1.PodRunning)
	p.Labels = map[string]string{"job-name": "job-" + name}
	return p
}

func runningBatch(name, cpu, mem string) *pipelinev1.PipelineJob {
	j := readyJob(name, pipelinev1.PriorityBatch, cpu, mem)
	j.Status.State = pipelinev1.StateRunning
	return j
}

func TestPickVictim(t *testing.T) {
	realtime := readyJob("urgent", pipelinev1.PriorityRealtime, "3", "1Gi")

	tests := []struct {
		name    string
		objs    []client.Object
		victims []*pipelinev1.PipelineJob
		want    string // "" means no victim
	}{
		{
			name: "skips a victim whose eviction would not make room",
			objs: []client.Object{
				node("n1", "8", "16Gi"),
				workerPod("small", "n1", "200m", "256Mi"),
				workerPod("big", "n1", "3", "1Gi"),
				pod("someone-elses", "n1", "4", "1Gi", corev1.PodRunning), // 7.2 of 8 CPU used
			},
			victims: []*pipelinev1.PipelineJob{runningBatch("small", "200m", "256Mi"), runningBatch("big", "3", "1Gi")},
			want:    "big",
		},
		{
			name: "among sufficient victims, takes the smallest",
			objs: []client.Object{
				node("n1", "8", "16Gi"),
				workerPod("a", "n1", "3", "1Gi"),
				workerPod("b", "n1", "5", "1Gi"),
			},
			victims: []*pipelinev1.PipelineJob{runningBatch("b", "5", "1Gi"), runningBatch("a", "3", "1Gi")},
			want:    "a",
		},
		{
			name: "the freed space must be on one node",
			objs: []client.Object{
				node("n1", "4", "8Gi"), node("n2", "4", "8Gi"),
				workerPod("x", "n1", "1500m", "1Gi"),
				workerPod("y", "n2", "1500m", "1Gi"),
				pod("fill-1", "n1", "1500m", "1Gi", corev1.PodRunning),
				pod("fill-2", "n2", "1500m", "1Gi", corev1.PodRunning),
			},
			// Each node has 1 CPU free; freeing either victim gives 2.5 CPU, still < 3.
			victims: []*pipelinev1.PipelineJob{runningBatch("x", "1500m", "1Gi"), runningBatch("y", "1500m", "1Gi")},
			want:    "",
		},
		{
			name: "no single eviction helps",
			objs: []client.Object{
				node("n1", "4", "8Gi"),
				workerPod("tiny", "n1", "200m", "128Mi"),
				pod("someone-elses", "n1", "3500m", "1Gi", corev1.PodRunning),
			},
			victims: []*pipelinev1.PipelineJob{runningBatch("tiny", "200m", "128Mi")},
			want:    "",
		},
		{
			name: "a submitted job whose pod does not exist yet counts as a victim too",
			objs: []client.Object{
				node("n1", "4", "8Gi"),
				runningBatch("queued", "3", "1Gi"), // PipelineJob exists, pod not created yet
			},
			victims: []*pipelinev1.PipelineJob{runningBatch("queued", "3", "1Gi")},
			want:    "queued",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAdmissionChecker(newClient(t, tt.objs...))
			got, err := a.PickVictim(context.Background(), realtime, tt.victims)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tt.want == "" && got != nil:
				t.Errorf("victim = %s, want none", got.Name)
			case tt.want != "" && (got == nil || got.Name != tt.want):
				t.Errorf("victim = %v, want %s", got, tt.want)
			}
		})
	}
}

func TestHandleReady_EvictsOnlyAVictimThatMakesRoom(t *testing.T) {
	urgent := pj("urgent", pipelinev1.StateReady)
	urgent.Spec.Priority = pipelinev1.PriorityRealtime
	urgent.Spec.RequestCPU, urgent.Spec.RequestMemory = "3", "1Gi"
	urgent.Status.LastUpdated = ago(30 * time.Second) // waited well past EvictionThreshold

	small := runningBatchNamed("small", "200m")
	big := runningBatchNamed("big", "3")

	r, c, _ := newTestReconciler(t,
		node("n1", "8", "16Gi"),
		urgent, small, big,
		workerPod("small", "n1", "200m", "256Mi"),
		workerPod("big", "n1", "3", "256Mi"),
		pod("someone-elses", "n1", "4", "1Gi", corev1.PodRunning),
	)

	if _, err := r.handleReady(context.Background(), get(t, c, "urgent")); err != nil {
		t.Fatal(err)
	}

	if s := get(t, c, "big").Status.State; s != pipelinev1.StateKilling {
		t.Errorf("big state = %s, want KILLING", s)
	}
	if s := get(t, c, "small").Status.State; s != pipelinev1.StateRunning {
		t.Errorf("small state = %s, want RUNNING: evicting it would not have helped", s)
	}
}

func TestHandleReady_DoesNotEvictWhenNothingWouldHelp(t *testing.T) {
	urgent := pj("urgent", pipelinev1.StateReady)
	urgent.Spec.Priority = pipelinev1.PriorityRealtime
	urgent.Spec.RequestCPU, urgent.Spec.RequestMemory = "3", "1Gi"
	urgent.Status.LastUpdated = ago(30 * time.Second)

	r, c, _ := newTestReconciler(t,
		node("n1", "4", "8Gi"),
		urgent, runningBatchNamed("tiny", "200m"),
		workerPod("tiny", "n1", "200m", "128Mi"),
		pod("someone-elses", "n1", "3500m", "1Gi", corev1.PodRunning),
	)

	if _, err := r.handleReady(context.Background(), get(t, c, "urgent")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "tiny").Status.State; s != pipelinev1.StateRunning {
		t.Errorf("tiny state = %s, want RUNNING (killing it frees too little)", s)
	}
}

func runningBatchNamed(name, cpu string) *pipelinev1.PipelineJob {
	j := pj(name, pipelinev1.StateRunning)
	j.Spec.RequestCPU = cpu
	return j
}

func TestHandleWaiting_ReasonNamesMissingOrUnfinishedUpstream(t *testing.T) {
	r, c, _ := newTestReconciler(t,
		pj("typo-dep", pipelinev1.StateWaiting, "stge-1"), // misspelled upstream
		pj("running-up", pipelinev1.StateRunning),
		pj("down", pipelinev1.StateWaiting, "running-up"),
	)
	ctx := context.Background()

	if _, err := r.handleWaiting(ctx, get(t, c, "typo-dep")); err != nil {
		t.Fatal(err)
	}
	got := get(t, c, "typo-dep")
	if got.Status.State != pipelinev1.StateWaiting {
		t.Errorf("state = %s, want it to keep waiting (the upstream may still be applied later)", got.Status.State)
	}
	if !strings.Contains(got.Status.Reason, `"stge-1"`) || !strings.Contains(got.Status.Reason, "not found") {
		t.Errorf("reason = %q, want it to say upstream stge-1 was not found", got.Status.Reason)
	}

	if _, err := r.handleWaiting(ctx, get(t, c, "down")); err != nil {
		t.Fatal(err)
	}
	if reason := get(t, c, "down").Status.Reason; !strings.Contains(reason, "running-up") || !strings.Contains(reason, "RUNNING") {
		t.Errorf("reason = %q, want it to name the unfinished upstream and its state", reason)
	}
}

func TestHandleWaiting_ReasonClearedWhenReady(t *testing.T) {
	down := pj("down", pipelinev1.StateWaiting, "up")
	down.Status.Reason = "waiting for upstream job \"up\" (RUNNING)"
	r, c, _ := newTestReconciler(t, pj("up", pipelinev1.StateDone), down)

	if _, err := r.handleWaiting(context.Background(), get(t, c, "down")); err != nil {
		t.Fatal(err)
	}
	got := get(t, c, "down")
	if got.Status.State != pipelinev1.StateReady || got.Status.Reason != "" {
		t.Errorf("state=%s reason=%q, want READY with no reason", got.Status.State, got.Status.Reason)
	}
}

func TestReconcile_NewJobStartsInWaiting(t *testing.T) {
	j := pj("fresh", "")
	j.Status = pipelinev1.PipelineJobStatus{}
	r, c, _ := newTestReconciler(t, j)

	if _, err := r.Reconcile(context.Background(), reqFor("fresh")); err != nil {
		t.Fatal(err)
	}
	got := get(t, c, "fresh")
	if got.Status.State != pipelinev1.StateWaiting || got.Status.LastUpdated == nil {
		t.Errorf("state=%q lastUpdated=%v, want WAITING with a timestamp", got.Status.State, got.Status.LastUpdated)
	}
}

func TestHandleReady_AdoptsExistingJobWithoutClearingMarker(t *testing.T) {
	// A previous attempt created the Job but failed to record SUBMITTED; meanwhile
	// the worker already finished and wrote its marker. The retry must keep that marker.
	job := pj("x", pipelinev1.StateReady)
	r, c, s3 := newTestReconciler(t, node("n1", "4", "8Gi"), job)
	s3.objects["test-bucket/x/_SUCCESS"] = true

	existing := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job-x", Namespace: "default"}}
	if err := controllerutil.SetControllerReference(get(t, c, "x"), existing, c.Scheme()); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), existing); err != nil {
		t.Fatal(err)
	}

	if _, err := r.handleReady(context.Background(), get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	if len(s3.deleted) != 0 || !s3.objects["test-bucket/x/_SUCCESS"] {
		t.Errorf("marker was cleared (deleted=%v) although the worker may have finished", s3.deleted)
	}
	if s := get(t, c, "x").Status.State; s != pipelinev1.StateSubmitted {
		t.Errorf("state = %s, want SUBMITTED", s)
	}
}

func TestHandleRunning_MissingTimestampIsNotInstantTimeout(t *testing.T) {
	j := pj("x", pipelinev1.StateSubmitted)
	j.Spec.TimeoutSeconds = 1
	j.Status.LastUpdated = nil
	r, c, _ := newTestReconciler(t, j)

	if _, err := r.handleRunning(context.Background(), get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	got := get(t, c, "x")
	if got.Status.State != pipelinev1.StateSubmitted {
		t.Errorf("state = %s, want SUBMITTED (no timestamp must not mean overdue)", got.Status.State)
	}
	if got.Status.LastUpdated == nil {
		t.Error("LastUpdated was not stamped")
	}
}
