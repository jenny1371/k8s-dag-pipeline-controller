package internal

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	pipelinev1 "pipeline-controller/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func pj(name string, state pipelinev1.JobState, deps ...string) *pipelinev1.PipelineJob {
	return &pipelinev1.PipelineJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec: pipelinev1.PipelineJobSpec{
			Priority:      pipelinev1.PriorityBatch,
			RequestCPU:    "200m",
			RequestMemory: "256Mi",
			StorageMarker: "s3://test-bucket/" + name + "/_SUCCESS",
			Stage:         "background",
			Dependencies:  deps,
		},
		Status: pipelinev1.PipelineJobStatus{
			State:       state,
			LastUpdated: ago(0),
		},
	}
}

func newTestReconciler(t *testing.T, objs ...client.Object) (*PipelineJobReconciler, client.Client, *fakeS3) {
	t.Helper()
	f, storage := newFakeS3(t)
	c := newClient(t, objs...)
	return &PipelineJobReconciler{
		Client:    c,
		DAG:       NewDAGRegistry(),
		Admission: NewAdmissionChecker(c),
		Eviction:  NewEvictionManager(c),
		Storage:   storage,
		Worker: WorkerConfig{
			Image:           "worker:test",
			ImagePullPolicy: corev1.PullIfNotPresent,
			MinioEndpoint:   "http://minio.minio:9000",
			AccessKey:       "ak",
			SecretKey:       "sk",
		},
	}, c, f
}

func get(t *testing.T, c client.Client, name string) *pipelinev1.PipelineJob {
	t.Helper()
	j := &pipelinev1.PipelineJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, j); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestHandleWaiting_FailsWhenUpstreamFailed(t *testing.T) {
	up := pj("up", pipelinev1.StateFailed)
	down := pj("down", pipelinev1.StateWaiting, "up")
	r, c, _ := newTestReconciler(t, up, down)

	if _, err := r.handleWaiting(context.Background(), get(t, c, "down")); err != nil {
		t.Fatal(err)
	}

	got := get(t, c, "down")
	if got.Status.State != pipelinev1.StateFailed {
		t.Errorf("state = %s, want FAILED", got.Status.State)
	}
	if !strings.Contains(got.Status.Reason, "up") {
		t.Errorf("reason = %q, want it to mention the failed upstream", got.Status.Reason)
	}
}

func TestHandleWaiting_FailurePropagatesThroughChain(t *testing.T) {
	r, c, _ := newTestReconciler(t,
		pj("a", pipelinev1.StateFailed),
		pj("b", pipelinev1.StateWaiting, "a"),
		pj("c", pipelinev1.StateWaiting, "b"),
	)
	ctx := context.Background()

	for _, name := range []string{"b", "c"} {
		if _, err := r.handleWaiting(ctx, get(t, c, name)); err != nil {
			t.Fatal(err)
		}
	}
	if s := get(t, c, "c").Status.State; s != pipelinev1.StateFailed {
		t.Errorf("c state = %s, want FAILED once b has failed", s)
	}
}

func TestHandleWaiting_ReadyWhenUpstreamDone(t *testing.T) {
	r, c, _ := newTestReconciler(t, pj("up", pipelinev1.StateDone), pj("down", pipelinev1.StateWaiting, "up"))

	if _, err := r.handleWaiting(context.Background(), get(t, c, "down")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "down").Status.State; s != pipelinev1.StateReady {
		t.Errorf("state = %s, want READY", s)
	}
}

func TestHandleWaiting_SameNameInOtherNamespaceIsNotAnUpstream(t *testing.T) {
	other := pj("up", pipelinev1.StateDone)
	other.Namespace = "other"
	r, c, _ := newTestReconciler(t, other, pj("down", pipelinev1.StateWaiting, "up"))

	if _, err := r.handleWaiting(context.Background(), get(t, c, "down")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "down").Status.State; s != pipelinev1.StateWaiting {
		t.Errorf("state = %s, want WAITING (upstream only exists in another namespace)", s)
	}
}

func TestHandleWaiting_MutualDependencyFailsBothJobs(t *testing.T) {
	r, c, _ := newTestReconciler(t,
		pj("a", pipelinev1.StateWaiting, "b"),
		pj("b", pipelinev1.StateWaiting, "a"),
	)
	ctx := context.Background()

	// a arrives first and is accepted (b is not registered yet)...
	if _, err := r.handleWaiting(ctx, get(t, c, "a")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "a").Status.State; s != pipelinev1.StateWaiting {
		t.Fatalf("a state = %s, want WAITING", s)
	}
	// ...b closes the cycle and fails...
	if _, err := r.handleWaiting(ctx, get(t, c, "b")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "b").Status.State; s != pipelinev1.StateFailed {
		t.Fatalf("b state = %s, want FAILED", s)
	}
	// ...and a no longer waits forever on a failed upstream.
	if _, err := r.handleWaiting(ctx, get(t, c, "a")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "a").Status.State; s != pipelinev1.StateFailed {
		t.Errorf("a state = %s, want FAILED", s)
	}
}

func TestHandleWaiting_InvalidSpecFails(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*pipelinev1.PipelineJob)
	}{
		{"marker without s3 scheme", func(j *pipelinev1.PipelineJob) { j.Spec.StorageMarker = "test-bucket/x" }},
		{"marker without key", func(j *pipelinev1.PipelineJob) { j.Spec.StorageMarker = "s3://test-bucket" }},
		{"bad cpu", func(j *pipelinev1.PipelineJob) { j.Spec.RequestCPU = "lots" }},
		{"bad priority", func(j *pipelinev1.PipelineJob) { j.Spec.Priority = "urgent" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := pj("bad", pipelinev1.StateWaiting)
			tt.mutate(j)
			r, c, _ := newTestReconciler(t, j)

			if _, err := r.handleWaiting(context.Background(), get(t, c, "bad")); err != nil {
				t.Fatal(err)
			}
			got := get(t, c, "bad")
			if got.Status.State != pipelinev1.StateFailed || got.Status.Reason == "" {
				t.Errorf("state=%s reason=%q, want FAILED with a reason", got.Status.State, got.Status.Reason)
			}
		})
	}
}

func TestHandleReady_ClearsStaleMarkerAndSubmitsWithRealRequests(t *testing.T) {
	r, c, s3 := newTestReconciler(t, node("n1", "4", "8Gi"), pj("stage-1", pipelinev1.StateReady))
	s3.objects["test-bucket/stage-1/_SUCCESS"] = true // left over from a previous run

	if _, err := r.handleReady(context.Background(), get(t, c, "stage-1")); err != nil {
		t.Fatal(err)
	}

	if len(s3.deleted) != 1 || s3.deleted[0] != "test-bucket/stage-1/_SUCCESS" {
		t.Errorf("stale marker not cleared, deleted = %v", s3.deleted)
	}
	if s := get(t, c, "stage-1").Status.State; s != pipelinev1.StateSubmitted {
		t.Errorf("state = %s, want SUBMITTED", s)
	}

	underlying := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "job-stage-1", Namespace: "default"}, underlying); err != nil {
		t.Fatal(err)
	}
	ctr := underlying.Spec.Template.Spec.Containers[0]
	if got := ctr.Resources.Requests.Cpu().MilliValue(); got != 200 {
		t.Errorf("container cpu request = %dm, want 200m (the scheduler must see the real request)", got)
	}
	if got := ctr.Resources.Requests.Memory().Value(); got != 256*1024*1024 {
		t.Errorf("container memory request = %d, want 256Mi", got)
	}
	if !metav1.IsControlledBy(underlying, get(t, c, "stage-1")) {
		t.Error("underlying Job is not owned by the PipelineJob (it would not be garbage-collected)")
	}
	env := map[string]string{}
	for _, e := range ctr.Env {
		env[e.Name] = e.Value
	}
	if env["BUCKET"] != "test-bucket" || env["MARKER_PATH"] != "test-bucket/stage-1/_SUCCESS" {
		t.Errorf("bucket/marker env = %q / %q", env["BUCKET"], env["MARKER_PATH"])
	}
	if env["MINIO_ENDPOINT"] != "http://minio.minio:9000" || ctr.Image != "worker:test" {
		t.Errorf("worker config not applied: endpoint=%q image=%q", env["MINIO_ENDPOINT"], ctr.Image)
	}
}

func TestHandleReady_UsesBucketFromMarker(t *testing.T) {
	j := pj("x", pipelinev1.StateReady)
	j.Spec.StorageMarker = "s3://other-bucket/deep/path/_SUCCESS"
	r, c, _ := newTestReconciler(t, node("n1", "4", "8Gi"), j)

	if _, err := r.handleReady(context.Background(), get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	underlying := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "job-x", Namespace: "default"}, underlying); err != nil {
		t.Fatal(err)
	}
	for _, e := range underlying.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "MARKER_PATH" && e.Value != "other-bucket/deep/path/_SUCCESS" {
			t.Errorf("MARKER_PATH = %q", e.Value)
		}
	}
}

func TestHandleReady_NoCapacityStaysReadyAndKeepsMarker(t *testing.T) {
	big := pj("big", pipelinev1.StateReady)
	big.Spec.RequestCPU = "3"
	r, c, s3 := newTestReconciler(t, node("n1", "1", "8Gi"), big)

	res, err := r.handleReady(context.Background(), get(t, c, "big"))
	if err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "big").Status.State; s != pipelinev1.StateReady {
		t.Errorf("state = %s, want READY", s)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected a requeue to re-check capacity")
	}
	if len(s3.deleted) != 0 {
		t.Errorf("marker deleted without submitting: %v", s3.deleted)
	}
}

func TestHandleReady_RemovesStaleJobFromEarlierPipelineJob(t *testing.T) {
	stale := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job-x", Namespace: "default"}} // no owner
	r, c, _ := newTestReconciler(t, node("n1", "4", "8Gi"), pj("x", pipelinev1.StateReady), stale)

	if _, err := r.handleReady(context.Background(), get(t, c, "x")); err == nil {
		t.Fatal("want an error so the reconcile is retried once the stale Job is gone")
	}
	err := c.Get(context.Background(), types.NamespacedName{Name: "job-x", Namespace: "default"}, &batchv1.Job{})
	if err == nil {
		t.Error("stale Job still exists; the new run would have reused its old result")
	}
	if s := get(t, c, "x").Status.State; s != pipelinev1.StateReady {
		t.Errorf("state = %s, want READY (not submitted yet)", s)
	}
}

func TestHandleRunning_StorageErrorIsReturnedNotSwallowed(t *testing.T) {
	r, c, s3 := newTestReconciler(t, pj("x", pipelinev1.StateSubmitted))
	s3.headStatus = http.StatusForbidden

	if _, err := r.handleRunning(context.Background(), get(t, c, "x")); err == nil {
		t.Fatal("want the storage error to be returned")
	}
	if s := get(t, c, "x").Status.State; s != pipelinev1.StateSubmitted {
		t.Errorf("state = %s, want it unchanged", s)
	}
}

func TestHandleRunning_MarkerMeansDone(t *testing.T) {
	r, c, s3 := newTestReconciler(t, pj("x", pipelinev1.StateRunning))
	s3.objects["test-bucket/x/_SUCCESS"] = true

	if _, err := r.handleRunning(context.Background(), get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "x").Status.State; s != pipelinev1.StateDone {
		t.Errorf("state = %s, want DONE", s)
	}
}

func TestHandleRunning_ActivePodMovesSubmittedToRunning(t *testing.T) {
	underlying := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job-x", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	r, c, _ := newTestReconciler(t, pj("x", pipelinev1.StateSubmitted), underlying)

	if _, err := r.handleRunning(context.Background(), get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "x").Status.State; s != pipelinev1.StateRunning {
		t.Errorf("state = %s, want RUNNING", s)
	}
}

func TestHandleRunning_FailedWorkerRetriesThenFails(t *testing.T) {
	failedJob := func() *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "job-x", Namespace: "default"},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
			}},
		}
	}

	t.Run("retry budget left", func(t *testing.T) {
		r, c, _ := newTestReconciler(t, pj("x", pipelinev1.StateRunning), failedJob())
		if _, err := r.handleRunning(context.Background(), get(t, c, "x")); err != nil {
			t.Fatal(err)
		}
		got := get(t, c, "x")
		if got.Status.State != pipelinev1.StateTimedOut {
			t.Errorf("state = %s, want TIMED_OUT (retry)", got.Status.State)
		}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "job-x", Namespace: "default"}, &batchv1.Job{}); err == nil {
			t.Error("failed underlying Job was not deleted")
		}
	})

	t.Run("retry budget exhausted", func(t *testing.T) {
		j := pj("x", pipelinev1.StateRunning)
		j.Status.RetryCount = RetryBudget
		r, c, _ := newTestReconciler(t, j, failedJob())
		if _, err := r.handleRunning(context.Background(), get(t, c, "x")); err != nil {
			t.Fatal(err)
		}
		got := get(t, c, "x")
		if got.Status.State != pipelinev1.StateFailed || got.Status.Reason == "" {
			t.Errorf("state=%s reason=%q, want FAILED with a reason", got.Status.State, got.Status.Reason)
		}
	})
}

func TestHandleRunning_TimeoutRetries(t *testing.T) {
	j := pj("x", pipelinev1.StateRunning)
	j.Spec.TimeoutSeconds = 10
	j.Status.LastUpdated = ago(time.Minute)
	r, c, _ := newTestReconciler(t, j)

	if _, err := r.handleRunning(context.Background(), get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	if s := get(t, c, "x").Status.State; s != pipelinev1.StateTimedOut {
		t.Errorf("state = %s, want TIMED_OUT", s)
	}
}

func TestReconcile_DeletedJobIsRemovedFromDAG(t *testing.T) {
	r, c, _ := newTestReconciler(t, pj("x", pipelinev1.StateWaiting))
	ctx := context.Background()

	if _, err := r.handleWaiting(ctx, get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	if len(r.DAG.List()) != 1 {
		t.Fatalf("DAG = %v, want x registered", r.DAG.List())
	}

	if err := c.Delete(ctx, get(t, c, "x")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reqFor("x")); err != nil {
		t.Fatal(err)
	}
	if len(r.DAG.List()) != 0 {
		t.Errorf("DAG = %v, want it empty after the PipelineJob is deleted", r.DAG.List())
	}
}

func reqFor(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}
}

func TestUnderlyingJob_UsesShortTerminationGrace(t *testing.T) {
	r, c, _ := newTestReconciler(t, node("n1", "4", "8Gi"), pj("stage-1", pipelinev1.StateReady))
	r.Worker.TerminationGracePeriodSeconds = 5
	if _, err := r.handleReady(context.Background(), get(t, c, "stage-1")); err != nil {
		t.Fatal(err)
	}
	underlying := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "job-stage-1", Namespace: "default"}, underlying); err != nil {
		t.Fatal(err)
	}
	g := underlying.Spec.Template.Spec.TerminationGracePeriodSeconds
	if g == nil || *g != 5 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 5 (an evicted pod must free its node quickly)", g)
	}
}

func TestNewWorkerConfigFromEnv_TerminationGrace(t *testing.T) {
	if got := NewWorkerConfigFromEnv().TerminationGracePeriodSeconds; got != DefaultWorkerTerminationGrace {
		t.Errorf("default grace = %d, want %d", got, DefaultWorkerTerminationGrace)
	}
	t.Setenv("WORKER_TERMINATION_GRACE_SECONDS", "12")
	if got := NewWorkerConfigFromEnv().TerminationGracePeriodSeconds; got != 12 {
		t.Errorf("grace = %d, want 12", got)
	}
	t.Setenv("WORKER_TERMINATION_GRACE_SECONDS", "abc")
	if got := NewWorkerConfigFromEnv().TerminationGracePeriodSeconds; got != DefaultWorkerTerminationGrace {
		t.Errorf("invalid value should fall back to default, got %d", got)
	}
}

func TestUpstreamFinished(t *testing.T) {
	cases := []struct {
		from, to pipelinev1.JobState
		want     bool
	}{
		{pipelinev1.StateRunning, pipelinev1.StateDone, true},
		{pipelinev1.StateRunning, pipelinev1.StateFailed, true},
		{pipelinev1.StateWaiting, pipelinev1.StateReady, false},
		{pipelinev1.StateDone, pipelinev1.StateDone, false},
	}
	for _, tc := range cases {
		if got := upstreamFinished(pj("a", tc.from), pj("a", tc.to)); got != tc.want {
			t.Errorf("%s -> %s: got %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}

func TestDependentsOf_ReturnsOnlyDirectDependents(t *testing.T) {
	up := pj("up", pipelinev1.StateDone)
	d1 := pj("d1", pipelinev1.StateWaiting, "up")
	d2 := pj("d2", pipelinev1.StateWaiting, "other", "up")
	unrelated := pj("x", pipelinev1.StateWaiting, "other")
	r, _, _ := newTestReconciler(t, up, d1, d2, unrelated)

	got := map[string]bool{}
	for _, q := range r.dependentsOf(context.Background(), up) {
		got[q.Name] = true
	}
	if len(got) != 2 || !got["d1"] || !got["d2"] {
		t.Errorf("dependents = %v, want d1 and d2 only", got)
	}
}
