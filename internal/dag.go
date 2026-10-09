package internal

import (
	"fmt"
	"sort"
	"sync"
)

type JobNode struct {
	Name       string
	Upstream   []string
	Downstream []string
}

type DAGRegistry struct {
	mu   sync.RWMutex
	jobs map[string]*JobNode
}

func NewDAGRegistry() *DAGRegistry {
	return &DAGRegistry{
		jobs: make(map[string]*JobNode),
	}
}

// Add registers a job and its dependencies, rejecting the change if it would
// create a cycle. It is idempotent: re-adding a job with the same dependencies
// is a no-op, and re-adding it with different dependencies replaces the old
// edges. On rejection the registry is left exactly as it was.
func (d *DAGRegistry) Add(name string, dependencies []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	deps := dedupe(dependencies)
	prev, had := d.jobs[name]
	if had && sameSet(prev.Upstream, deps) {
		return nil
	}

	d.jobs[name] = &JobNode{Name: name, Upstream: deps}

	if d.hasCycle() {
		if had {
			d.jobs[name] = prev
		} else {
			delete(d.jobs, name)
		}
		d.rebuildDownstream()
		return fmt.Errorf("adding job %q would create a cycle in the dependency graph", name)
	}

	d.rebuildDownstream()
	return nil
}

// rebuildDownstream recomputes every Downstream list from the Upstream edges,
// so it is correct regardless of the order jobs were registered in.
func (d *DAGRegistry) rebuildDownstream() {
	for _, node := range d.jobs {
		node.Downstream = nil
	}
	for name, node := range d.jobs {
		for _, dep := range node.Upstream {
			if parent, ok := d.jobs[dep]; ok {
				parent.Downstream = append(parent.Downstream, name)
			}
		}
	}
	for _, node := range d.jobs {
		sort.Strings(node.Downstream)
	}
}

// hasCycle runs a DFS over the Upstream edges.
// Colors: 0 = unvisited, 1 = on the current DFS stack, 2 = finished.
func (d *DAGRegistry) hasCycle() bool {
	color := make(map[string]int, len(d.jobs))

	var dfs func(name string) bool
	dfs = func(name string) bool {
		color[name] = 1
		node, ok := d.jobs[name]
		if !ok {
			color[name] = 2
			return false
		}
		for _, dep := range node.Upstream {
			if color[dep] == 1 {
				return true
			}
			if color[dep] == 0 {
				if dfs(dep) {
					return true
				}
			}
		}
		color[name] = 2
		return false
	}

	for name := range d.jobs {
		if color[name] == 0 {
			if dfs(name) {
				return true
			}
		}
	}
	return false
}

// Remove drops a job from the registry (e.g. when its PipelineJob is deleted).
func (d *DAGRegistry) Remove(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.jobs, name)
	d.rebuildDownstream()
}

// AllUpstreamDone reports whether every upstream of the job is in doneSet.
// An unregistered job is never ready.
func (d *DAGRegistry) AllUpstreamDone(name string, doneSet map[string]bool) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	node, ok := d.jobs[name]
	if !ok {
		return false
	}

	for _, dep := range node.Upstream {
		if !doneSet[dep] {
			return false
		}
	}
	return true
}

// Downstream returns the jobs that directly depend on name.
func (d *DAGRegistry) Downstream(name string) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	node, ok := d.jobs[name]
	if !ok {
		return nil
	}
	return append([]string(nil), node.Downstream...)
}

// List returns the names of all registered jobs.
func (d *DAGRegistry) List() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	names := make([]string, 0, len(d.jobs))
	for name := range d.jobs {
		names = append(names, name)
	}
	return names
}

func dedupe(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}
