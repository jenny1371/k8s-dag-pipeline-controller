package v1

import (
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/runtime/schema"
)
// Job states
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

// Job priority classes
type PriorityClass string

const (
    PriorityRealtime PriorityClass = "realtime"
    PriorityBatch    PriorityClass = "batch"
)

// yaml
type PipelineJobSpec struct {
    // job Dependencies job
    Dependencies []string `json:"dependencies,omitempty"`

    // realtime batch
    Priority PriorityClass `json:"priority"`

    // Resources
    RequestCPU    string `json:"requestCPU"`
    RequestMemory string `json:"requestMemory"`

    // S3/GCS CompletedPath， s3://bucket/output/_SUCCESS
    StorageMarker string `json:"storageMarker"`

    // Timeout， 300
    TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`

    // Job ，Calculate
    JobDurationSeconds int64 `json:"jobDurationSeconds,omitempty"`
    // Stage ， container
    Stage string `json:"stage,omitempty"`
}

// controller
type PipelineJobStatus struct {
    State         JobState `json:"state,omitempty"`
    EvictionCount int      `json:"evictionCount,omitempty"`
    RetryCount    int      `json:"retryCount,omitempty"`
    Preemptible   bool     `json:"preemptible,omitempty"`
    LastUpdated   string   `json:"lastUpdated,omitempty"`
}

// Internal controller logic
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type PipelineJob struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   PipelineJobSpec   `json:"spec,omitempty"`
    Status PipelineJobStatus `json:"status,omitempty"`
}

// List ，K8s
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