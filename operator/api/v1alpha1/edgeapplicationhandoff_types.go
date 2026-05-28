package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type EdgeApplicationHandoffSpec struct {
	// +kubebuilder:validation:MinLength=1
	EdgeApplicationName string `json:"edgeApplicationName"`

	// +kubebuilder:validation:MinLength=1
	TargetReplicaName string `json:"targetReplicaName"`

	// +optional
	SourceReplicaName string `json:"sourceReplicaName,omitempty"`

	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// If true, deleting this handoff object removes the target replica from EdgeApplication.spec.replicas.
	// +optional
	CleanupOnDelete bool `json:"cleanupOnDelete,omitempty"`
}

// HandoffTimestamps records microsecond-precision timestamps at each phase
// of the handoff lifecycle, enabling accurate sub-second benchmarking.
type HandoffTimestamps struct {
	// When the reconciler first observed this CR.
	// +optional
	ReconcileStart *metav1.MicroTime `json:"reconcileStart,omitempty"`
	// When the replica was upserted into EdgeApplication.spec.replicas.
	// +optional
	ReplicaApplied *metav1.MicroTime `json:"replicaApplied,omitempty"`
	// When the thaw HTTP request was sent to the queue-proxy (freeze only).
	// +optional
	ThawStarted *metav1.MicroTime `json:"thawStarted,omitempty"`
	// When the thaw HTTP response was received (freeze only).
	// +optional
	ThawCompleted *metav1.MicroTime `json:"thawCompleted,omitempty"`
	// When the operator first observed a Running target pod.
	// +optional
	PodRunning *metav1.MicroTime `json:"podRunning,omitempty"`
	// When the operator first observed the target pod with all containers Ready.
	// +optional
	PodReady *metav1.MicroTime `json:"podReady,omitempty"`
	// When the target KService became Ready.
	// +optional
	KServiceReady *metav1.MicroTime `json:"kserviceReady,omitempty"`
	// When the target Trigger became Ready (if applicable).
	// +optional
	TriggerReady *metav1.MicroTime `json:"triggerReady,omitempty"`
	// When both KService and Trigger are Ready (final handoff completion).
	// +optional
	Ready *metav1.MicroTime `json:"ready,omitempty"`
}

type EdgeApplicationHandoffStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	TargetKService string `json:"targetKService,omitempty"`
	// +optional
	TargetTrigger string `json:"targetTrigger,omitempty"`

	// Microsecond-precision timestamps for each handoff phase.
	// +optional
	Timestamps *HandoffTimestamps `json:"timestamps,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=edgeapplicationhandoffs,scope=Namespaced,shortName=meah,categories=mec
type EdgeApplicationHandoff struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EdgeApplicationHandoffSpec   `json:"spec,omitempty"`
	Status EdgeApplicationHandoffStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type EdgeApplicationHandoffList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EdgeApplicationHandoff `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EdgeApplicationHandoff{}, &EdgeApplicationHandoffList{})
}
