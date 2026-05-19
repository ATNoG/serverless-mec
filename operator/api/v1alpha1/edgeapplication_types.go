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

// EnvVarSpec supports either a literal value OR a valueFrom (Downward API, ConfigMapKeyRef, SecretKeyRef).
type EnvVarSpec struct {
	Name string `json:"name"`

	// +optional
	Value string `json:"value,omitempty"`

	// +optional
	ValueFrom *corev1.EnvVarSource `json:"valueFrom,omitempty"`
}

// KnativeContainerSpec holds the container configuration for the Knative Service.
type KnativeContainerSpec struct {
	// name is the container name (optional). If omitted, Knative will assign one.
	// +optional
	Name string `json:"name,omitempty"`

	// image is the container image for the Knative Service implementing this EdgeApplication.
	Image string `json:"image"`

	// command overrides the container entrypoint.
	// +optional
	Command []string `json:"command,omitempty"`

	// args overrides the container command arguments.
	// +optional
	Args []string `json:"args,omitempty"`

	// env is an optional list of environment variables for the container.
	// +optional
	Env []EnvVarSpec `json:"env,omitempty"`

	// resources are the resource requests and limits for this container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// securityContext is the container security context (capabilities, runAsUser, etc).
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// volumeMounts are mounts for volumes defined at the pod level.
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
}

// KnativeServiceSpec represents the vendor-specific "service" block in the CRD,
// used to realize the EdgeApplication as a Knative Service and (optionally) Trigger(s).
type KnativeServiceSpec struct {
	// container describes the container configuration for the Knative Service.
	Container KnativeContainerSpec `json:"container"`

	// minScale sets autoscaling.knative.dev/minScale.
	// Applies to base service, replicas, and zones services.
	// +optional
	MinScale *int32 `json:"minScale,omitempty"`

	// triggerFilters is a list of CloudEvent attribute filter maps.
	// Each entry creates one Knative Trigger pointing to the same Knative Service.
	//
	// Example:
	//   triggerFilters:
	//     - type: its.cam
	//     - type: its.denm
	//
	// If triggerFilters is empty/nil, the operator will NOT create any Trigger resources.
	// +optional
	TriggerFilters []map[string]string `json:"triggerFilters,omitempty"`

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

	// hostNetwork puts the pod into the host's network namespace (needed for sniffing host interfaces).
	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty"`

	// dnsPolicy allows setting ClusterFirstWithHostNet when hostNetwork=true.
	// +optional
	DNSPolicy corev1.DNSPolicy `json:"dnsPolicy,omitempty"`

	// podSecurityContext is the pod-level security context (runAsUser, fsGroup, etc).
	// NOTE: This applies to queue-proxy too. Prefer container.securityContext.runAsUser for root sniffers.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// volumes are pod volumes (e.g., emptyDir) that can be mounted by the container.
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`

	// serviceAccountName sets the pod's serviceAccountName (optional).
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// automountServiceAccountToken controls automounting SA token.
	// +optional
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty"`

	// freezeEnabled enables the knative-freezer-plugin for this service.
	// When true, the operator adds the qpoption.knative.dev/freezer-activate annotation
	// and injects the HOST_IP environment variable via the Downward API.
	// +optional
	FreezeEnabled *bool `json:"freezeEnabled,omitempty"`

	// freezeIdleTimeout is the number of seconds a container must be idle
	// before the freezer plugin checkpoints it. Propagated to the pod via
	// the qpoption.knative.dev/freezer-idle-timeout annotation, which the
	// plugin reads from the Downward API volume at /etc/podinfo/annotations.
	// Defaults to 30 if unset.
	// +optional
	// +kubebuilder:validation:Minimum=1
	FreezeIdleTimeout *int32 `json:"freezeIdleTimeout,omitempty"`
}

// KnativeServiceReplicaSpec describes an additional Knative Service instance derived
// from the base EdgeApplication.spec.service.
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
}

// Zone defines an auto-fanout rule: one KService per node matching matchNodes.
// Each "zone" represents a logical grouping of nodes (e.g. all RSUs along a
// road segment) where the EdgeApplication should run, with one instance per
// matching node.
type Zone struct {
	// matchNodes selects the nodes that should receive a service instance
	// for this zone.
	// Example:
	//   matchNodes:
	//     road-rsu: "true"
	// +kubebuilder:validation:MinProperties=1
	MatchNodes map[string]string `json:"matchNodes"`
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
	// MEC application as a Knative Service and (optionally) Trigger(s).
	// +optional
	Service *KnativeServiceSpec `json:"service,omitempty"`

	// replicas optionally defines extra Knative services derived from spec.service
	// while keeping the base service running (only when zones is NOT set).
	// +optional
	Replicas []KnativeServiceReplicaSpec `json:"replicas,omitempty"`

	// zones creates one KService per node that matches each zone's matchNodes.
	// If zones is set (non-empty), the controller runs in daemon mode
	// (no base service + no replicas).
	// +optional
	Zones []Zone `json:"zones,omitempty"`
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
