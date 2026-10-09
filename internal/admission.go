package internal

import (
	"context"
	"sort"

	pipelinev1 "pipeline-controller/api/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AdmissionChecker decides whether a job fits on the cluster. It takes a
// client.Reader rather than the cached client: pass the manager's API reader so
// that a job submitted a moment ago is already visible and admission cannot
// over-commit on stale cache data.
type AdmissionChecker struct {
	reader client.Reader
}

func NewAdmissionChecker(r client.Reader) *AdmissionChecker {
	return &AdmissionChecker{reader: r}
}

// resources is a CPU (millicores) and memory (bytes) amount.
type resources struct {
	milliCPU int64
	memory   int64
}

func (r resources) fitsIn(free resources) bool {
	return r.milliCPU <= free.milliCPU && r.memory <= free.memory
}

// nodeFree is the capacity still available on one schedulable node.
type nodeFree struct {
	name string
	free resources
}

// snapshot is a consistent view of the cluster state admission decisions are based on.
type snapshot struct {
	nodes []corev1.Node
	pods  []corev1.Pod
	jobs  []pipelinev1.PipelineJob
}

func (a *AdmissionChecker) snapshot(ctx context.Context) (*snapshot, error) {
	nodeList := &corev1.NodeList{}
	if err := a.reader.List(ctx, nodeList); err != nil {
		return nil, err
	}
	podList := &corev1.PodList{}
	if err := a.reader.List(ctx, podList); err != nil {
		return nil, err
	}
	jobList := &pipelinev1.PipelineJobList{}
	if err := a.reader.List(ctx, jobList); err != nil {
		return nil, err
	}
	return &snapshot{nodes: nodeList.Items, pods: podList.Items, jobs: jobList.Items}, nil
}

// HasCapacity reports whether a single node can still fit the job's request.
// It uses the real cluster state: each node's allocatable minus the requests of
// every non-terminated pod already bound to it (so non-PipelineJob workloads
// count), and pods that exist but are not scheduled yet are placed greedily onto
// the emptiest nodes before checking.
//
// A batch job additionally leaves room for realtime jobs that are READY and
// waiting, so capacity freed by an eviction goes to the realtime job instead of
// being taken straight back by the batch job that was just evicted. A realtime
// job that fits on no node reserves nothing, so it cannot starve batch work.
func (a *AdmissionChecker) HasCapacity(ctx context.Context, job *pipelinev1.PipelineJob) (bool, error) {
	need, err := parseRequest(job.Spec.RequestCPU, job.Spec.RequestMemory)
	if err != nil {
		return false, err
	}
	snap, err := a.snapshot(ctx)
	if err != nil {
		return false, err
	}

	nodes := freeCapacityPerNode(snap.nodes, snap.pods)
	pending := pendingRequests(snap.pods, snap.jobs)
	if job.Spec.Priority == pipelinev1.PriorityBatch {
		pending = append(pending, waitingRealtimeRequests(job, snap.jobs)...)
	}

	return fitsOnSomeNode(nodes, pending, need), nil
}

// PickVictim chooses which job to evict so that the waiting job fits. A victim
// is only acceptable if, with its pod gone, the waiting job would fit on a
// single node; evicting anything else would kill work without helping. Among the
// acceptable victims the one holding the least resources is chosen, to waste as
// little work as possible. It returns nil when no single eviction would help.
func (a *AdmissionChecker) PickVictim(ctx context.Context, waiting *pipelinev1.PipelineJob, victims []*pipelinev1.PipelineJob) (*pipelinev1.PipelineJob, error) {
	need, err := parseRequest(waiting.Spec.RequestCPU, waiting.Spec.RequestMemory)
	if err != nil {
		return nil, err
	}
	snap, err := a.snapshot(ctx)
	if err != nil {
		return nil, err
	}

	var best *pipelinev1.PipelineJob
	var bestSize resources
	for _, v := range victims {
		size, err := parseRequest(v.Spec.RequestCPU, v.Spec.RequestMemory)
		if err != nil {
			continue
		}
		if !fitsAfterRemoving(snap, v, need) {
			continue
		}
		if best == nil || size.milliCPU < bestSize.milliCPU ||
			(size.milliCPU == bestSize.milliCPU && size.memory < bestSize.memory) {
			best, bestSize = v, size
		}
	}
	return best, nil
}

// fitsAfterRemoving simulates evicting the victim (its pods and its in-flight
// claim disappear) and checks whether need then fits on one node.
func fitsAfterRemoving(snap *snapshot, victim *pipelinev1.PipelineJob, need resources) bool {
	victimJob := "job-" + victim.Name

	pods := make([]corev1.Pod, 0, len(snap.pods))
	for _, p := range snap.pods {
		if p.Namespace == victim.Namespace && p.Labels["job-name"] == victimJob {
			continue
		}
		pods = append(pods, p)
	}
	jobs := make([]pipelinev1.PipelineJob, 0, len(snap.jobs))
	for _, j := range snap.jobs {
		if j.Namespace == victim.Namespace && j.Name == victim.Name {
			continue
		}
		jobs = append(jobs, j)
	}

	return fitsOnSomeNode(freeCapacityPerNode(snap.nodes, pods), pendingRequests(pods, jobs), need)
}

// waitingRealtimeRequests returns the requests of READY realtime jobs (other
// than self) that are waiting for capacity.
func waitingRealtimeRequests(self *pipelinev1.PipelineJob, jobs []pipelinev1.PipelineJob) []resources {
	var out []resources
	for i := range jobs {
		j := &jobs[i]
		if j.Namespace == self.Namespace && j.Name == self.Name {
			continue
		}
		if j.Status.State != pipelinev1.StateReady || j.Spec.Priority != pipelinev1.PriorityRealtime {
			continue
		}
		if r, err := parseRequest(j.Spec.RequestCPU, j.Spec.RequestMemory); err == nil {
			out = append(out, r)
		}
	}
	return out
}

func parseRequest(cpu, memory string) (resources, error) {
	c, err := resource.ParseQuantity(cpu)
	if err != nil {
		return resources{}, err
	}
	m, err := resource.ParseQuantity(memory)
	if err != nil {
		return resources{}, err
	}
	return resources{milliCPU: c.MilliValue(), memory: m.Value()}, nil
}

func isTerminated(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

func podRequests(p *corev1.Pod) resources {
	var r resources
	for _, c := range p.Spec.Containers {
		r.milliCPU += c.Resources.Requests.Cpu().MilliValue()
		r.memory += c.Resources.Requests.Memory().Value()
	}
	return r
}

// schedulable reports whether pods without tolerations could land on the node.
func schedulable(n *corev1.Node) bool {
	if n.Spec.Unschedulable {
		return false
	}
	for _, t := range n.Spec.Taints {
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	return true
}

// freeCapacityPerNode returns allocatable minus the requests of pods bound to each schedulable node.
func freeCapacityPerNode(nodes []corev1.Node, pods []corev1.Pod) []nodeFree {
	used := make(map[string]resources)
	for i := range pods {
		p := &pods[i]
		if p.Spec.NodeName == "" || isTerminated(p) {
			continue
		}
		r := podRequests(p)
		u := used[p.Spec.NodeName]
		u.milliCPU += r.milliCPU
		u.memory += r.memory
		used[p.Spec.NodeName] = u
	}

	var out []nodeFree
	for i := range nodes {
		n := &nodes[i]
		if !schedulable(n) {
			continue
		}
		u := used[n.Name]
		out = append(out, nodeFree{
			name: n.Name,
			free: resources{
				milliCPU: n.Status.Allocatable.Cpu().MilliValue() - u.milliCPU,
				memory:   n.Status.Allocatable.Memory().Value() - u.memory,
			},
		})
	}
	return out
}

// pendingRequests returns the requests of work that is on its way to a node but
// not bound yet: unscheduled pods, plus SUBMITTED/RUNNING PipelineJobs whose pod
// has not been created by the Job controller yet.
func pendingRequests(pods []corev1.Pod, jobs []pipelinev1.PipelineJob) []resources {
	var out []resources
	havePod := make(map[string]bool)

	for i := range pods {
		p := &pods[i]
		if isTerminated(p) {
			continue
		}
		if name := p.Labels["job-name"]; name != "" {
			havePod[p.Namespace+"/"+name] = true
		}
		if p.Spec.NodeName == "" {
			out = append(out, podRequests(p))
		}
	}

	for i := range jobs {
		j := &jobs[i]
		if j.Status.State != pipelinev1.StateSubmitted && j.Status.State != pipelinev1.StateRunning {
			continue
		}
		if havePod[j.Namespace+"/job-"+j.Name] {
			continue
		}
		if r, err := parseRequest(j.Spec.RequestCPU, j.Spec.RequestMemory); err == nil {
			out = append(out, r)
		}
	}
	return out
}

// fitsOnSomeNode places the pending requests greedily (largest first, onto the
// node with the most free CPU that fits) and then checks whether need still fits on one node.
func fitsOnSomeNode(nodes []nodeFree, pending []resources, need resources) bool {
	free := append([]nodeFree(nil), nodes...)

	sort.Slice(pending, func(i, j int) bool { return pending[i].milliCPU > pending[j].milliCPU })
	for _, p := range pending {
		best := -1
		for i := range free {
			if p.fitsIn(free[i].free) && (best == -1 || free[i].free.milliCPU > free[best].free.milliCPU) {
				best = i
			}
		}
		if best >= 0 {
			free[best].free.milliCPU -= p.milliCPU
			free[best].free.memory -= p.memory
		}
	}

	for _, n := range free {
		if need.fitsIn(n.free) {
			return true
		}
	}
	return false
}
