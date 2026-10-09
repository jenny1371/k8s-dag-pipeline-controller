package internal

import (
	"sort"
	"testing"
)

func TestAdd_AcceptsAcyclicGraph(t *testing.T) {
	d := NewDAGRegistry()

	// stage-1 -> stage-2 -> stage-3, plus an independent job
	for _, tc := range []struct {
		name string
		deps []string
	}{
		{"stage-1", nil},
		{"stage-2", []string{"stage-1"}},
		{"stage-3", []string{"stage-2"}},
		{"background-1", nil},
	} {
		if err := d.Add(tc.name, tc.deps); err != nil {
			t.Fatalf("Add(%q, %v) returned unexpected error: %v", tc.name, tc.deps, err)
		}
	}

	got := d.List()
	sort.Strings(got)
	want := []string{"background-1", "stage-1", "stage-2", "stage-3"}
	if len(got) != len(want) {
		t.Fatalf("List() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List() = %v, want %v", got, want)
		}
	}
}

func TestAdd_AcceptsDiamond(t *testing.T) {
	d := NewDAGRegistry()
	mustAdd(t, d, "a", nil)
	mustAdd(t, d, "b", []string{"a"})
	mustAdd(t, d, "c", []string{"a"})
	// d depends on both b and c; two paths to a is not a cycle
	mustAdd(t, d, "d", []string{"b", "c"})
}

func TestAdd_RejectsCycles(t *testing.T) {
	tests := []struct {
		name  string
		setup []struct {
			job  string
			deps []string
		}
		job  string
		deps []string
	}{
		{
			name: "self dependency",
			job:  "a",
			deps: []string{"a"},
		},
		{
			name: "two-node cycle",
			setup: []struct {
				job  string
				deps []string
			}{{"a", []string{"b"}}},
			job:  "b",
			deps: []string{"a"},
		},
		{
			name: "three-node cycle",
			setup: []struct {
				job  string
				deps []string
			}{
				{"a", []string{"c"}},
				{"b", []string{"a"}},
			},
			job:  "c",
			deps: []string{"b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDAGRegistry()
			for _, s := range tt.setup {
				mustAdd(t, d, s.job, s.deps)
			}
			before := len(d.List())

			if err := d.Add(tt.job, tt.deps); err == nil {
				t.Fatalf("Add(%q, %v) = nil, want cycle error", tt.job, tt.deps)
			}

			// The rejected job must be rolled back out of the registry.
			if after := len(d.List()); after != before {
				t.Errorf("registry size after rejected Add = %d, want %d", after, before)
			}
			if _, ok := d.jobs[tt.job]; ok {
				t.Errorf("rejected job %q is still registered", tt.job)
			}
			// ...and out of every parent's Downstream list.
			for name, node := range d.jobs {
				for _, dn := range node.Downstream {
					if dn == tt.job {
						t.Errorf("job %q still listed as downstream of %q", tt.job, name)
					}
				}
			}
		})
	}
}

func TestAllUpstreamDone(t *testing.T) {
	d := NewDAGRegistry()
	mustAdd(t, d, "root", nil)
	mustAdd(t, d, "left", []string{"root"})
	mustAdd(t, d, "right", []string{"root"})
	mustAdd(t, d, "join", []string{"left", "right"})

	tests := []struct {
		name string
		job  string
		done map[string]bool
		want bool
	}{
		{"no dependencies is always ready", "root", map[string]bool{}, true},
		{"single dependency not done", "left", map[string]bool{}, false},
		{"single dependency done", "left", map[string]bool{"root": true}, true},
		{"only one of two dependencies done", "join", map[string]bool{"left": true}, false},
		{"all dependencies done", "join", map[string]bool{"left": true, "right": true}, true},
		{"nil done set", "left", nil, false},
		{"unknown job is never ready", "ghost", map[string]bool{"root": true}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := d.AllUpstreamDone(tt.job, tt.done); got != tt.want {
				t.Errorf("AllUpstreamDone(%q, %v) = %v, want %v", tt.job, tt.done, got, tt.want)
			}
		})
	}
}

func TestRemove(t *testing.T) {
	d := NewDAGRegistry()
	mustAdd(t, d, "a", nil)
	d.Remove("a")

	if len(d.List()) != 0 {
		t.Errorf("List() after Remove = %v, want empty", d.List())
	}
	if d.AllUpstreamDone("a", map[string]bool{}) {
		t.Error("AllUpstreamDone on a removed job = true, want false")
	}
}

func mustAdd(t *testing.T, d *DAGRegistry, name string, deps []string) {
	t.Helper()
	if err := d.Add(name, deps); err != nil {
		t.Fatalf("Add(%q, %v) returned unexpected error: %v", name, deps, err)
	}
}
