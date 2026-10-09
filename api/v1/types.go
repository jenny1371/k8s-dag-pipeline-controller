package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// JobState is the lifecycle state of a PipelineJob.
type JobState string

const (
	StateWaiting   JobState = "WAITING"
	StateReady     JobState = "READY"
	StateSubmitted JobState = "SUBMITTED"
	StateRunning   JobState = "RUNNING"
	StateKilling   JobState = "KILLING"
	StateTimedOut  JobState = "TIMED_OUT"
	StateDone      JobState = "DONE"
	StateFailed    JobState = "FAILED"
)

// PriorityClass decides whether a job may preempt others (realtime) or may be preempted (batch).
type PriorityClass string

const (
	PriorityRealtime PriorityClass = "realtime"
	PriorityBatch    PriorityClass = "batch"
)

// PipelineJobSpec is the desired state of a PipelineJob.
type PipelineJobSpec struct {
	// Dependencies lists the names (same namespace) of jobs that must be DONE before this one runs.
	Dependencies []string `json:"dependencies,omitempty"`

	// Priority is "realtime" or "batch".
	Priority PriorityClass `json:"priority"`

	// RequestCPU and RequestMemory are used both for admission control and as the
	// worker pod's resource requests.
	RequestCPU    string `json:"requestCPU"`
	RequestMemory string `json:"requestMemory"`

	// StorageMarker is the S3 object whose existence means the job finished,
	// e.g. s3://bucket/output/_SUCCESS.
	StorageMarker string `json:"storageMarker"`

	// TimeoutSeconds is the per-attempt timeout. Defaults to 300 when unset.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`

	// JobDurationSeconds is the simulated workload duration passed to the worker.
	JobDurationSeconds int64 `json:"jobDurationSeconds,omitempty"`

	// Stage selects which step of the example worker script to run.
	Stage string `json:"stage,omitempty"`
}

// PipelineJobStatus is the observed state, written only by the controller.
type PipelineJobStatus struct {
	State         JobState `json:"state,omitempty"`
	EvictionCount int      `json:"evictionCount,omitempty"`
	RetryCount    int      `json:"retryCount,omitempty"`
	// LastUpdated is when the job last changed state; timeouts are measured from it.
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`
	// Reason explains why the job is waiting, TIMED_OUT or FAILED.
	Reason string `json:"reason,omitempty"`
}

// PipelineJob is the Schema for the pipelinejobs API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type PipelineJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineJobSpec   `json:"spec,omitempty"`
	Status PipelineJobStatus `json:"status,omitempty"`
}

// PipelineJobList is a list of PipelineJob, as returned by the API server.
// +kubebuilder:object:root=true
type PipelineJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineJob `json:"items"`
}

func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(SchemeGroupVersion,
		&PipelineJob{},
		&PipelineJobList{},
	)
	metav1.AddToGroupVersion(s, SchemeGroupVersion)
	return nil
}

var SchemeGroupVersion = schema.GroupVersion{Group: "pipeline.io", Version: "v1"}
