/*
Copyright 2025.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EdgeRuleRef represents a reference to another MEC resource (e.g. TrafficRule, DNSRule).
type EdgeRuleRef struct {
	Name string `json:"name"`
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// EnvVarSpec supports either a literal value OR a valueFrom.
type EnvVarSpec struct {
	Name string `json:"name"`
	// +optional
	Value string `json:"value,omitempty"`
	// +optional
	ValueFrom *corev1.EnvVarSource `json:"valueFrom,omitempty"`
}

// KnativeContainerSpec holds the container configuration for the Knative Service.
type KnativeContainerSpec struct {
	// +optional
	Name string `json:"name,omitempty"`
	Image string `json:"image"`
	// +optional
	Env []EnvVarSpec `json:"env,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
}

// KnativeServiceSpec is the vendor-specific "service" block in the CRD,
// used to realize the EdgeApplication as Knative Services (and optionally Triggers).
type KnativeServiceSpec struct {
	Container KnativeContainerSpec `json:"container"`

	// minScale sets autoscaling.knative.dev/minScale and applies to ALL derived services
	// (base, replicas, autoReplicas).
	// +optional
	MinScale *int32 `json:"minScale,omitempty"`

	// triggerFilters are CloudEvent attribute filters for the Trigger.
	// If omitted or empty, NO triggers are created.
	// +optional
	TriggerFilters map[string]string `json:"triggerFilters,omitempty"`

	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty"`
	// +optional
	DNSPolicy corev1.DNSPolicy `json:"dnsPolicy,omitempty"`

	// podSecurityContext applies to ALL containers (including queue-proxy). Use with care.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`

	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// +optional
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty"`
}

// KnativeServiceReplicaSpec describes an additional Knative Service instance derived
// from the base EdgeApplication.spec.service. Placement-only overrides.
type KnativeServiceReplicaSpec struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// AutoReplicaRule defines an auto-fanout rule: one KService per matching node.
// New semantics: ONLY matchNodes is allowed.
type AutoReplicaRule struct {
	// +kubebuilder:validation:MinProperties=1
	MatchNodes map[string]string `json:"matchNodes"`
}

// EdgeApplicationSpec defines the desired state of EdgeApplication
type EdgeApplicationSpec struct {
	DID         string `json:"dId"`
	Name        string `json:"name"`
	Provider    string `json:"provider"`
	SoftVersion string `json:"softVersion"`
	DVersion    string `json:"dVersion"`

	// +optional
	InfoName string `json:"infoName,omitempty"`
	// +optional
	Description string `json:"description,omitempty"`
	// +optional
	InstanceID string `json:"instanceId,omitempty"`

	// +optional
	UsedTrafficRules []EdgeRuleRef `json:"usedTrafficRules,omitempty"`
	// +optional
	UsedDNSRules []EdgeRuleRef `json:"usedDNSRules,omitempty"`
	// +optional
	RelatedMepServices []string `json:"relatedMepServices,omitempty"`
	// +optional
	RelatedMeaServices []string `json:"relatedMeaServices,omitempty"`

	// +optional
	Service *KnativeServiceSpec `json:"service,omitempty"`

	// +optional
	Replicas []KnativeServiceReplicaSpec `json:"replicas,omitempty"`

	// +optional
	AutoReplicas []AutoReplicaRule `json:"autoReplicas,omitempty"`
}

// EdgeApplicationStatus defines the observed state of EdgeApplication.
type EdgeApplicationStatus struct {
	// +optional
	InstantiationState string `json:"instantiationState,omitempty"`
	// +optional
	AppState string `json:"appState,omitempty"`
	// +optional
	OperationalState string `json:"operationalState,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=edgeapplications,scope=Namespaced,shortName=mea;meapp,categories=mec
type EdgeApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              EdgeApplicationSpec   `json:"spec"`
	Status            EdgeApplicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type EdgeApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EdgeApplication `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EdgeApplication{}, &EdgeApplicationList{})
}
