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

// NOTE: json tags are required. Any new fields you add must have json tags
// for the fields to be serialized, and you must run "make" to regenerate code
// after modifying this file.

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
	// Each condition has a unique type and reflects the status of a specific aspect
	// of the resource.
	//
	// Standard condition types may include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
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

// -----------------------------------------------------------------------------
// Manual deep-copy methods to satisfy generated code expectations
// -----------------------------------------------------------------------------

// DeepCopyInto performs a deep copy of EdgeApplicationSpec into out.
func (in *EdgeApplicationSpec) DeepCopyInto(out *EdgeApplicationSpec) {
	*out = *in

	// Deep copy slices
	if in.UsedTrafficRules != nil {
		out.UsedTrafficRules = make([]EdgeRuleRef, len(in.UsedTrafficRules))
		copy(out.UsedTrafficRules, in.UsedTrafficRules)
	}

	if in.UsedDNSRules != nil {
		out.UsedDNSRules = make([]EdgeRuleRef, len(in.UsedDNSRules))
		copy(out.UsedDNSRules, in.UsedDNSRules)
	}

	if in.RelatedMepServices != nil {
		out.RelatedMepServices = append([]string(nil), in.RelatedMepServices...)
	}

	if in.RelatedMeaServices != nil {
		out.RelatedMeaServices = append([]string(nil), in.RelatedMeaServices...)
	}

	// Deep copy Service (container + env + triggerFilters + scheduling)
	if in.Service != nil {
		out.Service = &KnativeServiceSpec{
			Container: KnativeContainerSpec{
				Image: in.Service.Container.Image,
				Env:   nil,
				// copy resources deeply
				Resources: *in.Service.Container.Resources.DeepCopy(),
			},
			TriggerFilters: nil,
			NodeSelector:   nil,
			Affinity:       nil,
			Tolerations:    nil,
		}

		// env
		if in.Service.Container.Env != nil {
			out.Service.Container.Env = make([]NameValuePair, len(in.Service.Container.Env))
			copy(out.Service.Container.Env, in.Service.Container.Env)
		}

		// triggerFilters
		if in.Service.TriggerFilters != nil {
			out.Service.TriggerFilters = make(map[string]string, len(in.Service.TriggerFilters))
			for k, v := range in.Service.TriggerFilters {
				out.Service.TriggerFilters[k] = v
			}
		}

		// nodeSelector
		if in.Service.NodeSelector != nil {
			out.Service.NodeSelector = make(map[string]string, len(in.Service.NodeSelector))
			for k, v := range in.Service.NodeSelector {
				out.Service.NodeSelector[k] = v
			}
		}

		// affinity
		if in.Service.Affinity != nil {
			out.Service.Affinity = in.Service.Affinity.DeepCopy()
		}

		// tolerations
		if in.Service.Tolerations != nil {
			out.Service.Tolerations = make([]corev1.Toleration, len(in.Service.Tolerations))
			copy(out.Service.Tolerations, in.Service.Tolerations)
		}
	}
}

// DeepCopy creates a new deep-copied EdgeApplicationSpec.
func (in *EdgeApplicationSpec) DeepCopy() *EdgeApplicationSpec {
	if in == nil {
		return nil
	}
	out := new(EdgeApplicationSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto performs a deep copy of EdgeApplicationStatus into out.
func (in *EdgeApplicationStatus) DeepCopyInto(out *EdgeApplicationStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
}

// DeepCopy creates a new deep-copied EdgeApplicationStatus.
func (in *EdgeApplicationStatus) DeepCopy() *EdgeApplicationStatus {
	if in == nil {
		return nil
	}
	out := new(EdgeApplicationStatus)
	in.DeepCopyInto(out)
	return out
}

func init() {
	SchemeBuilder.Register(&EdgeApplication{}, &EdgeApplicationList{})
}
