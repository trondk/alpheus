/*
Copyright The Alpheus Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// StreamRun is one run of a flow: a graph of steps that run at the same
// time and pass chunks to each other through object storage. A chunk is a
// file in any format; Alpheus moves it and never reads it.
//
// Create it to start the run, read its status to follow it, set
// spec.cancel to stop it, and delete it to clean up.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=sr,categories=alpheus
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Steps",type=string,JSONPath=`.status.stepsFinished`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`
// +kubebuilder:printcolumn:name="Error",type=string,JSONPath=`.status.message`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 40",message="name must be at most 40 characters, so pod names <run>-<step> stay within 63"
type StreamRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec StreamRunSpec `json:"spec"`

	// +optional
	Status StreamRunStatus `json:"status,omitzero"`
}

// StreamRunSpec describes the flow. Everything except cancel is immutable
// after creation.
//
// +kubebuilder:validation:XValidation:rule="self.steps == oldSelf.steps",message="steps is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.edges) == has(oldSelf.edges) && (!has(self.edges) || self.edges == oldSelf.edges)",message="edges is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.chunks) == has(oldSelf.chunks) && (!has(self.chunks) || self.chunks == oldSelf.chunks)",message="chunks is immutable"
// +kubebuilder:validation:XValidation:rule="self.storage.endpoint == oldSelf.storage.endpoint && self.storage.bucket == oldSelf.storage.bucket && self.storage.secretRef == oldSelf.storage.secretRef && has(self.storage.region) == has(oldSelf.storage.region) && (!has(self.storage.region) || self.storage.region == oldSelf.storage.region) && has(self.storage.resultsBucket) == has(oldSelf.storage.resultsBucket) && (!has(self.storage.resultsBucket) || self.storage.resultsBucket == oldSelf.storage.resultsBucket) && has(self.storage.resultsSecretRef) == has(oldSelf.storage.resultsSecretRef) && (!has(self.storage.resultsSecretRef) || self.storage.resultsSecretRef == oldSelf.storage.resultsSecretRef) && has(self.storage.timeout) == has(oldSelf.storage.timeout) && (!has(self.storage.timeout) || duration(self.storage.timeout) == duration(oldSelf.storage.timeout))",message="storage is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.signals) == has(oldSelf.signals) && (!has(self.signals) || self.signals == oldSelf.signals)",message="signals is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.ttlAfterFinished) == has(oldSelf.ttlAfterFinished) && (!has(self.ttlAfterFinished) || duration(self.ttlAfterFinished) == duration(oldSelf.ttlAfterFinished))",message="ttlAfterFinished is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.resultsTTL) == has(oldSelf.resultsTTL) && (!has(self.resultsTTL) || duration(self.resultsTTL) == duration(oldSelf.resultsTTL))",message="resultsTTL is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.cancel) || !oldSelf.cancel || (has(self.cancel) && self.cancel)",message="cancel cannot be undone"
// +kubebuilder:validation:XValidation:rule="!has(self.edges) || self.edges.all(e, self.steps.exists(s, s.name == e.from) && self.steps.exists(s, s.name == e.to))",message="every edge must go from and to an existing step"
type StreamRunSpec struct {
	// Steps are the containers of the flow. Names are unique within the run.
	//
	// +required
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	Steps []Step `json:"steps"`

	// Edges connect the output of one step to the input of another. The
	// graph must not have cycles; that is checked by the controller (and the
	// webhook, when it is installed), not by the schema.
	//
	// +optional
	// +listType=map
	// +listMapKey=from
	// +listMapKey=output
	// +listMapKey=to
	// +kubebuilder:validation:MaxItems=256
	Edges []Edge `json:"edges,omitempty"`

	// Chunks sets how output is cut into chunks and how far a producer may
	// run ahead of its consumers.
	//
	// +optional
	// +default={}
	Chunks ChunkPolicy `json:"chunks,omitzero"`

	// Storage is the S3-compatible store that holds the chunks.
	//
	// +required
	Storage Storage `json:"storage"`

	// Signals selects how agents tell each other that chunks are ready.
	// Correctness never depends on signals; agents fall back to polling.
	//
	// +optional
	// +default={}
	Signals Signals `json:"signals,omitzero"`

	// Cancel stops the run. It can be set to true once and not undone.
	//
	// +optional
	Cancel bool `json:"cancel,omitempty"`

	// TTLAfterFinished is how long the run's pods and chunks are kept after
	// it has finished. Not set means they are kept until the StreamRun is
	// deleted.
	//
	// +optional
	TTLAfterFinished *metav1.Duration `json:"ttlAfterFinished,omitempty"`

	// ResultsTTL is how long the run's results, error logs and summary are
	// kept after it has finished; "0s" means the controller never deletes
	// them (GO-78).
	//
	// +optional
	// +default="168h"
	ResultsTTL *metav1.Duration `json:"resultsTTL,omitempty"`
}

// Step is one container in the flow.
type Step struct {
	// Name of the step, unique within the run. It is part of pod names and
	// object keys in storage.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[a-z]([a-z0-9-]*[a-z0-9])?$`
	Name string `json:"name"`

	// Image of the component. It must have a digest
	// (registry/name@sha256:...), unless the installation allows tags.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`

	// Settings are passed to the component as they are. Their schema is the
	// component's business.
	//
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	Settings *runtime.RawExtension `json:"settings,omitempty"`

	// EnvFrom lists Secrets and ConfigMaps the component gets as
	// environment variables. The component gets no other credentials.
	//
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// Resources of the component container.
	//
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Replicas is how many copies of the step run. The copies share the
	// step's input: chunk n of the stream from producer replica p goes to
	// replica (n + p) mod replicas.
	//
	// +optional
	// +default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	Replicas *int32 `json:"replicas,omitempty"`

	// MaxRestarts is how many times the controller restarts a failed
	// replica of the step before the run fails.
	//
	// +optional
	// +default=3
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxRestarts *int32 `json:"maxRestarts,omitempty"`

	// FailurePolicy says what happens when a replica has used all its
	// restarts: stop ends the whole run, continue ends that replica's
	// streams and the run finishes as PartiallySucceeded (GO-97).
	//
	// +optional
	// +default="stop"
	FailurePolicy FailurePolicy `json:"failurePolicy,omitempty"`

	// Scratch is the size of the replica's scratch folder, where input and
	// output chunks are files (GO-106).
	//
	// +optional
	// +default="10Gi"
	Scratch *resource.Quantity `json:"scratch,omitempty"`

	// MaxOpen is how many input chunks the component may have open at once
	// (GO-110).
	//
	// +optional
	// +default=8
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	MaxOpen *int32 `json:"maxOpen,omitempty"`

	// Prefetch is how many chunks per incoming stream the agent fetches
	// ahead (GO-111).
	//
	// +optional
	// +default=2
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Prefetch *int32 `json:"prefetch,omitempty"`

	// InputOrder is the order of chunks from different streams: arrival,
	// as they become ready, or deterministic, one from each stream in turn
	// (GO-112).
	//
	// +optional
	// +default="arrival"
	InputOrder InputOrder `json:"inputOrder,omitempty"`
}

// FailurePolicy is what happens when a replica has used all its restarts.
//
// +kubebuilder:validation:Enum=stop;continue
type FailurePolicy string

const (
	FailureStop     FailurePolicy = "stop"
	FailureContinue FailurePolicy = "continue"
)

// Reason is a stable code for why a run or a replica did not succeed
// (GO-138). Messages may be reworded; reasons are part of the API.
//
// +kubebuilder:validation:Enum=InvalidSpec;Cancelled;ComponentFailed;AgentFailed;StorageRefused;StorageUnreachable;StorageFull;StorageError;ScratchFull;PodDeleted;PodFailed;FailedReplicas
type Reason string

const (
	// ReasonInvalidSpec: the graph is invalid or an image is not pinned.
	ReasonInvalidSpec Reason = "InvalidSpec"
	// ReasonCancelled: spec.cancel was set.
	ReasonCancelled Reason = "Cancelled"
	// ReasonComponentFailed: the component exited with an error.
	ReasonComponentFailed Reason = "ComponentFailed"
	// ReasonAgentFailed: the agent failed for another reason, or
	// restarted too often.
	ReasonAgentFailed Reason = "AgentFailed"
	// ReasonStorageRefused: the storage refused the keys (GO-137).
	ReasonStorageRefused Reason = "StorageRefused"
	// ReasonStorageUnreachable: the storage was down for longer than
	// spec.storage.timeout (GO-136).
	ReasonStorageUnreachable Reason = "StorageUnreachable"
	// ReasonStorageFull: the bucket's quota is reached.
	ReasonStorageFull Reason = "StorageFull"
	// ReasonStorageError: the storage answered with another error.
	ReasonStorageError Reason = "StorageError"
	// ReasonScratchFull: a chunk did not fit in the scratch folder
	// (GO-106).
	ReasonScratchFull Reason = "ScratchFull"
	// ReasonPodDeleted: the pod disappeared, such as by eviction, a lost
	// node or a deletion.
	ReasonPodDeleted Reason = "PodDeleted"
	// ReasonPodFailed: the pod failed without a container saying why.
	ReasonPodFailed Reason = "PodFailed"
	// ReasonFailedReplicas: the run finished, but replicas with
	// failurePolicy continue gave up (GO-97).
	ReasonFailedReplicas Reason = "FailedReplicas"
)

// Reasons returns all reasons, for checking a code from outside the API.
func Reasons() []Reason {
	return []Reason{ReasonInvalidSpec, ReasonCancelled, ReasonComponentFailed, ReasonAgentFailed,
		ReasonStorageRefused, ReasonStorageUnreachable, ReasonStorageFull, ReasonStorageError,
		ReasonScratchFull, ReasonPodDeleted, ReasonPodFailed, ReasonFailedReplicas}
}

// InputOrder is the order input chunks are given to the component in.
//
// +kubebuilder:validation:Enum=arrival;deterministic
type InputOrder string

const (
	InputArrival       InputOrder = "arrival"
	InputDeterministic InputOrder = "deterministic"
)

// Edge connects an output of one step to the input of another. A step's
// output can have several edges (fan-out), and a step can have several
// incoming edges (fan-in).
type Edge struct {
	// From is the name of the producing step.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=20
	From string `json:"from"`

	// Output is the producer's named output, as given to Create in the SDK.
	//
	// +optional
	// +default="default"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[a-z]([a-z0-9-]*[a-z0-9])?$`
	Output string `json:"output,omitempty"`

	// To is the name of the consuming step.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=20
	To string `json:"to"`

	// Port is the consumer's named input the edge ends in (GO-108).
	//
	// +optional
	// +default="default"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[a-z]([a-z0-9-]*[a-z0-9])?$`
	Port string `json:"port,omitempty"`
}

// ChunkPolicy sets chunk size and backpressure. A chunk is a file the
// component chose the size of; Alpheus does not cut it.
type ChunkPolicy struct {
	// MaxBytes is the largest chunk; closing a larger file is an error
	// (GO-105). Not set means no limit.
	//
	// +optional
	MaxBytes *resource.Quantity `json:"maxBytes,omitempty"`

	// MaxUnacked is how many chunks a consumer replica may have unacked
	// from a stream before the producer blocks (GO-35).
	//
	// +optional
	// +default=4
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	MaxUnacked int32 `json:"maxUnacked,omitempty"`
}

// Storage is an S3-compatible object store.
//
// +kubebuilder:validation:XValidation:rule="!has(self.resultsSecretRef) || has(self.resultsBucket)",message="resultsSecretRef needs resultsBucket"
type Storage struct {
	// Endpoint is the URL of the S3 API.
	//
	// +required
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^https?://`
	Endpoint string `json:"endpoint"`

	// Bucket holds the run's chunks under runs/<uid>/ and its results
	// under results/<run>/<uid>/, where <uid> is the StreamRun's UID.
	//
	// +required
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`
	Bucket string `json:"bucket"`

	// Region of the bucket. Most S3-compatible stores ignore it.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Region string `json:"region,omitempty"`

	// SecretRef names a Secret in the run's namespace with the keys
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. Only agents get it.
	// With ResultsBucket, it must be able to write both buckets.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	SecretRef string `json:"secretRef"`

	// ResultsBucket holds the results, results/<run>/<uid>/, instead of
	// Bucket, so they can be read with a key that can only read them
	// (GO-129).
	//
	// +optional
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`
	ResultsBucket string `json:"resultsBucket,omitempty"`

	// ResultsSecretRef names a Secret like SecretRef with a key that can
	// read ResultsBucket. status.results names it instead of SecretRef,
	// so readers of results do not get the agents' key.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ResultsSecretRef string `json:"resultsSecretRef,omitempty"`

	// Timeout is how long an agent keeps trying while the storage is down
	// or answers with a temporary error, such as 503 or a refused
	// connection. Its component waits meanwhile, as under backpressure,
	// and no restart is used. When the storage has not answered for this
	// long, the attempt fails with the reason "storage unreachable"
	// (GO-136). 0 gives up at once.
	//
	// +optional
	// +default="10m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s') && duration(self) <= duration('24h')",message="timeout must be between 0s and 24h"
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}

// SignalDriver names a signal driver.
//
// +kubebuilder:validation:Enum=builtin
type SignalDriver string

const (
	// SignalDriverBuiltin sends signals through the hub in the controller.
	SignalDriverBuiltin SignalDriver = "builtin"
)

// Signals selects the signal driver.
type Signals struct {
	// Driver is the signal driver. builtin is the only one.
	//
	// +optional
	// +default="builtin"
	Driver SignalDriver `json:"driver,omitempty"`
}

// RunPhase is the phase of a run.
//
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;PartiallySucceeded;Failed;Cancelled
type RunPhase string

const (
	RunPending   RunPhase = "Pending"
	RunRunning   RunPhase = "Running"
	RunSucceeded RunPhase = "Succeeded"
	// RunPartiallySucceeded means the run finished, but a replica with
	// failurePolicy continue gave up and some input chunks were not
	// processed (GO-97).
	RunPartiallySucceeded RunPhase = "PartiallySucceeded"
	RunFailed             RunPhase = "Failed"
	RunCancelled          RunPhase = "Cancelled"
)

// StepPhase is the phase of a step.
//
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
type StepPhase string

const (
	StepPending   StepPhase = "Pending"
	StepRunning   StepPhase = "Running"
	StepSucceeded StepPhase = "Succeeded"
	StepFailed    StepPhase = "Failed"
)

// Condition types of a StreamRun.
const (
	// ConditionReady is true when the run has finished successfully.
	ConditionReady = "Ready"
	// ConditionProgressing is true while chunks are moving.
	ConditionProgressing = "Progressing"
	// ConditionDegraded is true while a step is restarting or lagging.
	ConditionDegraded = "Degraded"
)

// StreamRunStatus is written by the controller only.
type StreamRunStatus struct {
	// ObservedGeneration is the generation the status was computed from.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase of the run.
	//
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// Message says why the run failed, in words a person can act on.
	//
	// +optional
	Message string `json:"message,omitempty"`

	// Reason is why the run did not succeed, as a code a program can rely
	// on; Message says the same in words (GO-138).
	//
	// +optional
	Reason Reason `json:"reason,omitempty"`

	// FailedStep and FailedReplica are the replica whose failure stopped
	// the run, when one did (GO-138).
	//
	// +optional
	FailedStep string `json:"failedStep,omitempty"`
	// +optional
	FailedReplica *int32 `json:"failedReplica,omitempty"`

	// StepsFinished is "<finished>/<total>", for kubectl get.
	//
	// +optional
	StepsFinished string `json:"stepsFinished,omitempty"`

	// StartedAt is when the first pod of the run started.
	//
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// FinishedAt is when the run reached Succeeded, Failed or Cancelled.
	//
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// Steps has one entry per step.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	Steps []StepStatus `json:"steps,omitempty"`

	// Edges has one entry per edge.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	Edges []EdgeStatus `json:"edges,omitempty"`

	// Results has one entry per result: an output that no edge goes from.
	// It holds what is needed to fetch the result.
	//
	// +optional
	// +listType=map
	// +listMapKey=step
	// +listMapKey=output
	Results []ResultStatus `json:"results,omitempty"`

	// Conditions are Ready, Progressing and Degraded.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// StepStatus is the state of one step.
type StepStatus struct {
	// Name of the step.
	//
	// +required
	Name string `json:"name"`

	// Phase of the step: Succeeded when all replicas have succeeded,
	// Failed when one replica has used all its restarts.
	//
	// +optional
	Phase StepPhase `json:"phase,omitempty"`

	// Replicas has one entry per replica.
	//
	// +optional
	// +listType=map
	// +listMapKey=index
	Replicas []ReplicaStatus `json:"replicas,omitempty"`

	// Counts are the component's committed counters, summed over its
	// replicas, per unit (GO-115). They are exact, also after restarts.
	//
	// +optional
	Counts map[string]int64 `json:"counts,omitempty"`

	// Errors and Warnings are how many entries the step's error log has,
	// and ErrorsDropped how many were not kept because of the limits
	// (GO-119, GO-120).
	//
	// +optional
	Errors int64 `json:"errors,omitempty"`
	// +optional
	Warnings int64 `json:"warnings,omitempty"`
	// +optional
	ErrorsDropped int64 `json:"errorsDropped,omitempty"`

	// ErrorLog is the prefix of the step's error log in the bucket:
	// results/<run>/<uid>/_errors/<step>/ (GO-118).
	//
	// +optional
	ErrorLog string `json:"errorLog,omitempty"`

	// Logs is where the logs of the step's failed attempts are kept:
	// s3://<bucket>/results/<run>/<uid>/_logs/<step>/ (GO-140).
	//
	// +optional
	Logs string `json:"logs,omitempty"`

	// Failures is where failed attempts are kept, with the input chunks
	// they had not finished, so each can be replayed on its own with
	// alpheus replay: s3://<bucket>/results/<run>/<uid>/_failures/<step>/
	// (GO-141).
	//
	// +optional
	Failures string `json:"failures,omitempty"`
}

// ReplicaStatus is the state of one replica of a step.
type ReplicaStatus struct {
	// Index of the replica, from 0.
	//
	// +required
	Index int32 `json:"index"`

	// Phase of the replica.
	//
	// +optional
	Phase StepPhase `json:"phase,omitempty"`

	// Attempt is the current attempt, starting at 1. A new attempt starts
	// only when the previous attempt's pod is gone. Agents write it to the
	// manifests they own as epoch.
	//
	// +optional
	Attempt int32 `json:"attempt,omitempty"`

	// Restarts is how many times the replica has been restarted.
	//
	// +optional
	Restarts int32 `json:"restarts,omitempty"`

	// Pod is the name of the current attempt's pod.
	//
	// +optional
	Pod string `json:"pod,omitempty"`

	// Message says why the replica failed, or while it runs, the
	// component's latest progress report (GO-122).
	//
	// +optional
	Message string `json:"message,omitempty"`

	// Reason is why the replica's last attempt failed, as a code a program
	// can rely on (GO-138). It is cleared when the replica succeeds.
	//
	// +optional
	Reason Reason `json:"reason,omitempty"`

	// FailedChunks is how many input chunks assigned to the replica were
	// never processed, because it gave up with failurePolicy continue
	// (GO-97).
	//
	// +optional
	FailedChunks int64 `json:"failedChunks,omitempty"`
}

// EdgeStatus is the progress of one edge.
type EdgeStatus struct {
	// Name of the edge, as used in storage: <from>-<to>, or
	// <from>.<output>-<to> for an output other than default.
	//
	// +required
	Name string `json:"name"`

	// Chunks is how many chunks the producer's replicas have committed.
	//
	// +optional
	Chunks int64 `json:"chunks,omitempty"`

	// Bytes is how many bytes the producer's replicas have committed.
	//
	// +optional
	Bytes int64 `json:"bytes,omitempty"`

	// Acked is how many chunks the consumer's replicas have acked. Chunks
	// minus Acked is the edge's lag.
	//
	// +optional
	Acked int64 `json:"acked,omitempty"`

	// EOS is true when every producer replica has committed its last chunk.
	//
	// +optional
	EOS bool `json:"eos,omitempty"`
}

// ResultStatus says where a result is and how to fetch it.
type ResultStatus struct {
	// Step that wrote the result.
	//
	// +required
	Step string `json:"step"`

	// Output of the step that the result comes from.
	//
	// +required
	Output string `json:"output"`

	// Endpoint is the URL of the S3 API, as in spec.storage.
	//
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Bucket that holds the result.
	//
	// +optional
	Bucket string `json:"bucket,omitempty"`

	// Region of the bucket, as in spec.storage; some S3 servers check it.
	//
	// +optional
	Region string `json:"region,omitempty"`

	// Prefix of the result's objects: results/<run>/<uid>/<step>/<output>/.
	// Each replica writes a manifest and its chunks in <prefix><replica>/.
	//
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// SecretRef names the Secret with keys for the bucket. For now it is
	// spec.storage.secretRef, which gives access to the whole bucket.
	//
	// +optional
	SecretRef string `json:"secretRef,omitempty"`

	// Chunks is how many chunks the result has.
	//
	// +optional
	Chunks int64 `json:"chunks,omitempty"`

	// Bytes is how many bytes the result has.
	//
	// +optional
	Bytes int64 `json:"bytes,omitempty"`

	// Counts are the producing step's committed counters for the result,
	// per unit (GO-79).
	//
	// +optional
	Counts map[string]int64 `json:"counts,omitempty"`
}

// StreamRunList is a list of StreamRuns.
//
// +kubebuilder:object:root=true
type StreamRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StreamRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StreamRun{}, &StreamRunList{})
}
