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

type EdgeApplicationHandoffStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	TargetKService string `json:"targetKService,omitempty"`
	// +optional
	TargetTrigger string `json:"targetTrigger,omitempty"`

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
