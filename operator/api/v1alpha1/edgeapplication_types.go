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
	// name of the referenced resource (required in CRD)
	Name string `json:"name"`

	// namespace of the referenced resource (optional in CRD)
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// NameValuePair represents a simple name/value pair for environment variables.
type NameValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// KnativeContainerSpec holds the container configuration for the Knative Service.
type KnativeContainerSpec struct {
	// image is the container image for the Knative Service implementing this EdgeApplication.
	Image string `json:"image"`

	// env is an optional list of environment variables for the container.
	// +optional
	Env []NameValuePair `json:"env,omitempty"`

	// resources are the resource requests and limits for this container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// KnativeServiceSpec represents the vendor-specific "service" block in the CRD,
// used to realize the EdgeApplication as a Knative Service and Trigger.
type KnativeServiceSpec struct {
	// container describes the container configuration for the Knative Service.
	Container KnativeContainerSpec `json:"container"`

	// triggerFilters are the CloudEvent attribute filters for the Knative Trigger
	// (maps to spec.filter.attributes).
	// +optional
	TriggerFilters map[string]string `json:"triggerFilters,omitempty"`

	// nodeSelector selects the nodes on which the Knative Service's pods may run.
	// This is a direct pass-through to pod.spec.nodeSelector.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// affinity configures pod/node affinity (typically NodeAffinity).
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// tolerations applied to the pods created for this Knative Service.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// KnativeServiceReplicaSpec describes an additional Knative Service instance derived
// from the base EdgeApplication.spec.service (e.g., pinned to a specific RSU node).
type KnativeServiceReplicaSpec struct {
	// name is the replica identifier. The resulting Knative Service will be:
	// <edgeapplication name>-<replica name>
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// nodeSelector selects the nodes on which this replica's pods may run.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// affinity configures pod/node affinity.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// tolerations applied to the pods created for this replica.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// minScale sets autoscaling.knative.dev/minScale for this replica.
	// +optional
	MinScale *int32 `json:"minScale,omitempty"`

	// createTrigger controls whether a trigger is created for this replica.
	// If nil, defaults to true.
	// +optional
	CreateTrigger *bool `json:"createTrigger,omitempty"`

	// triggerFilters overrides trigger filters for this replica.
	// If nil, base EdgeApplication.spec.service.triggerFilters is used.
	// +optional
	TriggerFilters map[string]string `json:"triggerFilters,omitempty"`
}

// EdgeApplicationSpec defines the desired state of EdgeApplication
// and corresponds to spec in the CRD.
type EdgeApplicationSpec struct {
	// dId is the identifier of the edge application descriptor (dId).
	DID string `json:"dId"`

	// name is the human readable name of the MEC application.
	Name string `json:"name"`

	// provider is the provider of the MEC application.
	Provider string `json:"provider"`

	// softVersion is the version of the MEC application software.
	SoftVersion string `json:"softVersion"`

	// dVersion is the version of the application descriptor.
	DVersion string `json:"dVersion"`

	// infoName is a human readable product name.
	// +optional
	InfoName string `json:"infoName,omitempty"`

	// description is a human readable description of the MEC application.
	// +optional
	Description string `json:"description,omitempty"`

	// instanceId is the application instance identifier.
	// +optional
	InstanceID string `json:"instanceId,omitempty"`

	// usedTrafficRules references TrafficRule resources used by this app (active rules only).
	// +optional
	UsedTrafficRules []EdgeRuleRef `json:"usedTrafficRules,omitempty"`

	// usedDNSRules references DNSRule resources used by this app (active rules only).
	// +optional
	UsedDNSRules []EdgeRuleRef `json:"usedDNSRules,omitempty"`

	// relatedMepServices are edge platform services related to this application.
	// +optional
	RelatedMepServices []string `json:"relatedMepServices,omitempty"`

	// relatedMeaServices are edge application services related to this application.
	// +optional
	RelatedMeaServices []string `json:"relatedMeaServices,omitempty"`

	// service contains vendor-specific Knative configuration used to realize this
	// MEC application as a Knative Service and Trigger (not defined in ETSI GS MEC 010-1).
	// +optional
	Service *KnativeServiceSpec `json:"service,omitempty"`

	// replicas optionally defines extra Knative services derived from spec.service
	// while keeping the base service running.
	// +optional
	Replicas []KnativeServiceReplicaSpec `json:"replicas,omitempty"`
}

// EdgeApplicationStatus defines the observed state of EdgeApplication.
type EdgeApplicationStatus struct {
	// instantiationState corresponds to the ETSI lifecycle state:
	// one of "NOT_INSTANTIATED" or "INSTANTIATED".
	// +optional
	InstantiationState string `json:"instantiationState,omitempty"`

	// appState corresponds to the ETSI application state:
	// one of "STARTED" or "STOPPED".
	// +optional
	AppState string `json:"appState,omitempty"`

	// operationalState is a read-only indicator of operational state:
	// one of "ACTIVE" or "INACTIVE".
	// +optional
	OperationalState string `json:"operationalState,omitempty"`

	// conditions represent the current state of the EdgeApplication resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=edgeapplications,scope=Namespaced,shortName=mea;meapp,categories=mec

// EdgeApplication is the Schema for the edgeapplications API
type EdgeApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of EdgeApplication
	// +required
	Spec EdgeApplicationSpec `json:"spec"`

	// status defines the observed state of EdgeApplication
	// +optional
	Status EdgeApplicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EdgeApplicationList contains a list of EdgeApplication
type EdgeApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EdgeApplication `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EdgeApplication{}, &EdgeApplicationList{})
}
