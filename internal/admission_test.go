package internal

import (
	"context"
	"testing"

	pipelinev1 "pipeline-controller/api/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func node(name, cpu, mem string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(mem),
		}},
	}
}

func pod(name, nodeName, cpu, mem string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name: "c",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(cpu),
					corev1.ResourceMemory: resource.MustParse(mem),
				}},
			}},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pipelinev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&pipelinev1.PipelineJob{}).
		Build()
}

func TestHasCapacity(t *testing.T) {
	tests := []struct {
		name string
		objs []client.Object
		cpu  string
		mem  string
		want bool
	}{
		{
			name: "empty node fits",
			objs: []client.Object{node("n1", "4", "8Gi")},
			cpu:  "3", mem: "4Gi", want: true,
		},
		{
			name: "free CPU is split across two nodes, so a 3 CPU pod does not fit",
			objs: []client.Object{
				node("n1", "4", "8Gi"), node("n2", "4", "8Gi"),
				pod("a", "n1", "2", "1Gi", corev1.PodRunning),
				pod("b", "n2", "2", "1Gi", corev1.PodRunning),
			},
			cpu: "3", mem: "1Gi", want: false,
		},
		{
			name: "same total free CPU but on one node fits",
			objs: []client.Object{
				node("n1", "4", "8Gi"), node("n2", "4", "8Gi"),
				pod("a", "n1", "4", "1Gi", corev1.PodRunning),
			},
			cpu: "3", mem: "1Gi", want: true,
		},
		{
			name: "pods outside any PipelineJob count against the node",
			objs: []client.Object{
				node("n1", "4", "8Gi"),
				pod("someone-elses", "n1", "3", "1Gi", corev1.PodRunning),
			},
			cpu: "2", mem: "1Gi", want: false,
		},
		{
			name: "finished pods release their resources",
			objs: []client.Object{
				node("n1", "4", "8Gi"),
				pod("done", "n1", "4", "6Gi", corev1.PodSucceeded),
			},
			cpu: "3", mem: "4Gi", want: true,
		},
		{
			name: "memory can be the limiting resource",
			objs: []client.Object{
				node("n1", "8", "4Gi"),
				pod("a", "n1", "1", "3Gi", corev1.PodRunning),
			},
			cpu: "1", mem: "2Gi", want: false,
		},
		{
			name: "an unscheduled pod already claims the free space",
			objs: []client.Object{
				node("n1", "4", "8Gi"),
				pod("queued", "", "3", "1Gi", corev1.PodPending),
			},
			cpu: "2", mem: "1Gi", want: false,
		},
		{
			name: "unschedulable node is ignored",
			objs: []client.Object{
				func() client.Object { n := node("n1", "8", "16Gi"); n.Spec.Unschedulable = true; return n }(),
			},
			cpu: "1", mem: "1Gi", want: false,
		},
		{
			name: "tainted node (e.g. control plane) is ignored",
			objs: []client.Object{
				func() client.Object {
					n := node("n1", "8", "16Gi")
					n.Spec.Taints = []corev1.Taint{{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule}}
					return n
				}(),
			},
			cpu: "1", mem: "1Gi", want: false,
		},
		{
			name: "submitted PipelineJob whose pod does not exist yet is still counted",
			objs: []client.Object{
				node("n1", "4", "8Gi"),
				&pipelinev1.PipelineJob{
					ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "default"},
					Spec:       pipelinev1.PipelineJobSpec{RequestCPU: "3", RequestMemory: "1Gi"},
					Status:     pipelinev1.PipelineJobStatus{State: pipelinev1.StateSubmitted},
				},
			},
			cpu: "2", mem: "1Gi", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAdmissionChecker(newClient(t, tt.objs...))
			req := &pipelinev1.PipelineJob{
				ObjectMeta: metav1.ObjectMeta{Name: "req", Namespace: "default"},
				Spec:       pipelinev1.PipelineJobSpec{Priority: pipelinev1.PriorityBatch, RequestCPU: tt.cpu, RequestMemory: tt.mem},
			}
			got, err := a.HasCapacity(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("HasCapacity(%s, %s) = %v, want %v", tt.cpu, tt.mem, got, tt.want)
			}
		})
	}
}

func TestHasCapacity_InvalidQuantity(t *testing.T) {
	a := NewAdmissionChecker(newClient(t, node("n1", "4", "8Gi")))
	req := &pipelinev1.PipelineJob{Spec: pipelinev1.PipelineJobSpec{RequestCPU: "lots", RequestMemory: "1Gi"}}
	if _, err := a.HasCapacity(context.Background(), req); err == nil {
		t.Error("want an error for an unparsable CPU quantity")
	}
}

func readyJob(name string, prio pipelinev1.PriorityClass, cpu, mem string) *pipelinev1.PipelineJob {
	return &pipelinev1.PipelineJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       pipelinev1.PipelineJobSpec{Priority: prio, RequestCPU: cpu, RequestMemory: mem},
		Status:     pipelinev1.PipelineJobStatus{State: pipelinev1.StateReady},
	}
}

func TestHasCapacity_BatchLeavesRoomForWaitingRealtime(t *testing.T) {
	// One 4 CPU node. A realtime job (3 CPU) is waiting; a batch job (3 CPU) must
	// not take the space the realtime job is about to use.
	objs := []client.Object{node("n1", "4", "8Gi"), readyJob("urgent", pipelinev1.PriorityRealtime, "3", "1Gi")}
	a := NewAdmissionChecker(newClient(t, objs...))

	batch := readyJob("hog", pipelinev1.PriorityBatch, "3", "1Gi")
	if ok, err := a.HasCapacity(context.Background(), batch); err != nil || ok {
		t.Errorf("batch: got (%v, %v), want (false, nil) while realtime is waiting", ok, err)
	}
	urgent := readyJob("urgent", pipelinev1.PriorityRealtime, "3", "1Gi")
	if ok, err := a.HasCapacity(context.Background(), urgent); err != nil || !ok {
		t.Errorf("realtime: got (%v, %v), want (true, nil)", ok, err)
	}
	small := readyJob("small", pipelinev1.PriorityBatch, "1", "1Gi")
	if ok, err := a.HasCapacity(context.Background(), small); err != nil || !ok {
		t.Errorf("small batch: got (%v, %v), want (true, nil); there is room beside the reservation", ok, err)
	}
}

func TestHasCapacity_UnschedulableRealtimeDoesNotStarveBatch(t *testing.T) {
	// The realtime job needs 16 CPU and fits on no node, so it reserves nothing.
	objs := []client.Object{node("n1", "4", "8Gi"), readyJob("huge", pipelinev1.PriorityRealtime, "16", "1Gi")}
	a := NewAdmissionChecker(newClient(t, objs...))

	batch := readyJob("hog", pipelinev1.PriorityBatch, "3", "1Gi")
	if ok, err := a.HasCapacity(context.Background(), batch); err != nil || !ok {
		t.Errorf("batch: got (%v, %v), want (true, nil)", ok, err)
	}
}
