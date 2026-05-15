package internal

import (
	"fmt"
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

// Add job DAG， cycle error Write
func (d *DAGRegistry) Add(name string, dependencies []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// ， cycle Check
	d.jobs[name] = &JobNode{
		Name:     name,
		Upstream: dependencies,
	}

	// Create downstream
	for _, dep := range dependencies {
		if parent, ok := d.jobs[dep]; ok {
			parent.Downstream = append(parent.Downstream, name)
		}
	}

	// DFS cycle detection
	if d.hasCycle() {
		// ： node downstream
		for _, dep := range dependencies {
			if parent, ok := d.jobs[dep]; ok {
				filtered := parent.Downstream[:0]
				for _, dn := range parent.Downstream {
					if dn != name {
						filtered = append(filtered, dn)
					}
				}
				parent.Downstream = filtered
			}
		}
		delete(d.jobs, name)
		return fmt.Errorf("adding job %q would create a cycle in the dependency graph", name)
	}

	return nil
}

// hasCycle DFS Check cycle（，）
// ：0=, 1=( stack ), 2=Completed
func (d *DAGRegistry) hasCycle() bool {
	color := make(map[string]int, len(d.jobs))

	var dfs func(name string) bool
	dfs = func(name string) bool {
		color[name] = 1 // Internal controller logic
		node, ok := d.jobs[name]
		if !ok {
			color[name] = 2
			return false
		}
		for _, dep := range node.Upstream {
			if color[dep] == 1 {
				// Node → cycle
				return true
			}
			if color[dep] == 0 {
				if dfs(dep) {
					return true
				}
			}
		}
		color[name] = 2 // Completed
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

// Remove job
func (d *DAGRegistry) Remove(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.jobs, name)
}

// AllUpstreamDone job DONE
func (d *DAGRegistry) AllUpstreamDone(name string, doneSet map[string]bool) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	node, ok := d.jobs[name]
	if !ok {
		return false
	}

	if len(node.Upstream) == 0 {
		return true
	}

	for _, dep := range node.Upstream {
		if !doneSet[dep] {
			return false
		}
	}
	return true
}

// List job
func (d *DAGRegistry) List() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	names := make([]string, 0, len(d.jobs))
	for name := range d.jobs {
		names = append(names, name)
	}
	return names
}