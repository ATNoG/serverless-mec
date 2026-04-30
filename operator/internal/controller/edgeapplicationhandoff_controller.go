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

package controller

import (
	"context"
	"fmt"
	"regexp"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/equality"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
	"knative.dev/pkg/apis"

	mecv1alpha1 "github.com/ATNoG/serverless-mec/operator/api/v1alpha1"
)

const (
	handoffFinalizer      = "mec.atnog.org/handoff-finalizer"
	defaultRSULabelKeyHO  = "mec.atnog.org/rsu"
)

// EdgeApplicationHandoffReconciler reconciles EdgeApplicationHandoff objects.
type EdgeApplicationHandoffReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ConfigNamespace string
}

// +kubebuilder:rbac:groups=mec.atnog.org,resources=edgeapplicationhandoffs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mec.atnog.org,resources=edgeapplicationhandoffs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mec.atnog.org,resources=edgeapplicationhandoffs/finalizers,verbs=update
// +kubebuilder:rbac:groups=mec.atnog.org,resources=edgeapplications,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=eventing.knative.dev,resources=triggers,verbs=get;list;watch

func (r *EdgeApplicationHandoffReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	var ho mecv1alpha1.EdgeApplicationHandoff
	if err := r.Get(ctx, req.NamespacedName, &ho); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion handling (+ optional cleanup)
	if !ho.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&ho, handoffFinalizer) {
			if ho.Spec.CleanupOnDelete {
				if err := r.cleanupTargetReplica(ctx, &ho); err != nil {
					logger.Error(err, "cleanupTargetReplica failed")
					return ctrl.Result{}, err
				}
			}
			controllerutil.RemoveFinalizer(&ho, handoffFinalizer)
			if err := r.Update(ctx, &ho); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer
	if !controllerutil.ContainsFinalizer(&ho, handoffFinalizer) {
		controllerutil.AddFinalizer(&ho, handoffFinalizer)
		if err := r.Update(ctx, &ho); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Validate fields
	if ho.Spec.EdgeApplicationName == "" || ho.Spec.TargetReplicaName == "" {
		r.setFailed(ctx, &ho, "SpecInvalid", "edgeApplicationName and targetReplicaName are required")
		return ctrl.Result{}, nil
	}
	if !isDNSLabelHO(ho.Spec.TargetReplicaName) {
		r.setFailed(ctx, &ho, "SpecInvalid", "targetReplicaName must be a DNS label (lowercase, digits, '-')")
		return ctrl.Result{}, nil
	}
	if ho.Spec.SourceReplicaName != "" && !isDNSLabelHO(ho.Spec.SourceReplicaName) {
		r.setFailed(ctx, &ho, "SpecInvalid", "sourceReplicaName must be a DNS label (lowercase, digits, '-')")
		return ctrl.Result{}, nil
	}

	// Fetch EdgeApplication (same namespace)
	var app mecv1alpha1.EdgeApplication
	if err := r.Get(ctx, types.NamespacedName{Name: ho.Spec.EdgeApplicationName, Namespace: ho.Namespace}, &app); err != nil {
		if apierrors.IsNotFound(err) {
			r.setPending(ctx, &ho, "EdgeApplicationNotFound", "waiting for referenced EdgeApplication to exist")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Must have service template to create replicas
	if app.Spec.Service == nil {
		r.setFailed(ctx, &ho, "EdgeApplicationInvalid", "EdgeApplication.spec.service is nil; nothing to replicate")
		return ctrl.Result{}, nil
	}

	// Build desired replica spec:
	desiredRep, err := r.buildTargetReplica(&app, &ho)
	if err != nil {
		r.setFailed(ctx, &ho, "BuildReplicaFailed", err.Error())
		return ctrl.Result{}, nil
	}

	// Upsert into EdgeApplication.spec.replicas
	orig := app.DeepCopy()
	changed := upsertReplica(&app.Spec.Replicas, desiredRep)
	if changed {
		if err := r.Patch(ctx, &app, client.MergeFrom(orig)); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Status references (expected names)
	svcName := fmt.Sprintf("%s-%s", app.Name, ho.Spec.TargetReplicaName)
	ho.Status.TargetKService = fmt.Sprintf("%s/%s", app.Namespace, svcName)

	// trigger exists only if app.spec.service.triggerFilters is non-empty
	trigNs := ""
	var triggerNames []string
	if app.Spec.Service != nil && len(app.Spec.Service.TriggerFilters) > 0 {
		trigNs = r.expectedTriggerNamespace(ctx)
		for i := range app.Spec.Service.TriggerFilters {
			triggerNames = append(triggerNames, fmt.Sprintf("%s-trigger-%d", svcName, i))
		}
	}
	if len(triggerNames) > 0 {
		ho.Status.TargetTrigger = fmt.Sprintf("%s/%s", trigNs, triggerNames[0])
	} else {
		ho.Status.TargetTrigger = ""
	}

	// Readiness checks (best-effort)
	svcReady := r.isKServiceReady(ctx, app.Namespace, svcName)

	trigReady := true
	for _, tn := range triggerNames {
		if !r.isTriggerReady(ctx, trigNs, tn) {
			trigReady = false
			break
		}
	}

	if svcReady && trigReady {
		r.setReady(ctx, &ho, "Ready", "target replica is ready")
		return ctrl.Result{}, nil
	}

	r.setApplied(ctx, &ho, "Applied", "target replica applied; waiting for readiness")
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

func (r *EdgeApplicationHandoffReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ConfigNamespace == "" {
		r.ConfigNamespace = defaultConfigNs
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mecv1alpha1.EdgeApplicationHandoff{}).
		Named("edgeapplicationhandoff").
		Complete(r)
}

// --- Core logic ---

func (r *EdgeApplicationHandoffReconciler) buildTargetReplica(
	app *mecv1alpha1.EdgeApplication,
	ho *mecv1alpha1.EdgeApplicationHandoff,
) (mecv1alpha1.KnativeServiceReplicaSpec, error) {
	// Start with a copy of source replica if provided and found
	var base *mecv1alpha1.KnativeServiceReplicaSpec
	if ho.Spec.SourceReplicaName != "" {
		for i := range app.Spec.Replicas {
			if app.Spec.Replicas[i].Name == ho.Spec.SourceReplicaName {
				tmp := app.Spec.Replicas[i] // copy
				base = &tmp
				break
			}
		}
	}

	var out mecv1alpha1.KnativeServiceReplicaSpec
	if base != nil {
		out = *base
	}

	// Always set target name
	out.Name = ho.Spec.TargetReplicaName

	// Node selector behavior:
	// - if user provides nodeSelector, use exactly that
	// - otherwise default to pinning by RSU label:
	//     mec.atnog.org/rsu = <targetReplicaName>
	if ho.Spec.NodeSelector != nil {
		out.NodeSelector = ho.Spec.NodeSelector
	} else {
		if out.NodeSelector == nil {
			out.NodeSelector = map[string]string{}
		}
		out.NodeSelector[defaultRSULabelKeyHO] = ho.Spec.TargetReplicaName
	}

	// Optional overrides
	if ho.Spec.Affinity != nil {
		out.Affinity = ho.Spec.Affinity
	}
	if len(ho.Spec.Tolerations) > 0 {
		out.Tolerations = ho.Spec.Tolerations
	}

	return out, nil
}

func upsertReplica(list *[]mecv1alpha1.KnativeServiceReplicaSpec, desired mecv1alpha1.KnativeServiceReplicaSpec) bool {
	if *list == nil {
		*list = []mecv1alpha1.KnativeServiceReplicaSpec{desired}
		return true
	}

	for i := range *list {
		if (*list)[i].Name == desired.Name {
			if equality.Semantic.DeepEqual((*list)[i], desired) {
				return false
			}
			(*list)[i] = desired
			return true
		}
	}

	*list = append(*list, desired)
	return true
}

// Cleanup target replica from EdgeApplication.spec.replicas when handoff is deleted (optional)
func (r *EdgeApplicationHandoffReconciler) cleanupTargetReplica(ctx context.Context, ho *mecv1alpha1.EdgeApplicationHandoff) error {
	var app mecv1alpha1.EdgeApplication
	if err := r.Get(ctx, types.NamespacedName{Name: ho.Spec.EdgeApplicationName, Namespace: ho.Namespace}, &app); err != nil {
		return err
	}

	orig := app.DeepCopy()
	out := make([]mecv1alpha1.KnativeServiceReplicaSpec, 0, len(app.Spec.Replicas))
	removed := false

	for _, rep := range app.Spec.Replicas {
		if rep.Name == ho.Spec.TargetReplicaName {
			removed = true
			continue
		}
		out = append(out, rep)
	}

	if !removed {
		return nil
	}

	app.Spec.Replicas = out
	return r.Patch(ctx, &app, client.MergeFrom(orig))
}

// Determine the namespace where triggers are created.
// Uses mec-operator-config (same as EdgeApplication controller).
func (r *EdgeApplicationHandoffReconciler) expectedTriggerNamespace(ctx context.Context) string {
	cfgNs := r.ConfigNamespace
	if cfgNs == "" {
		cfgNs = defaultConfigNs
	}

	var cfg corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Name: defaultConfigMapName, Namespace: cfgNs}, &cfg); err != nil {
		return ""
	}

	brokerNamespace := cfg.Data["default-broker-namespace"]
	brokerName := cfg.Data["default-broker-name"]
	if brokerNamespace == "" || brokerName == "" {
		return ""
	}

	return brokerNamespace
}

// --- Readiness checks ---

func (r *EdgeApplicationHandoffReconciler) isKServiceReady(ctx context.Context, ns, name string) bool {
	var svc servingv1.Service
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &svc); err != nil {
		return false
	}
	cond := svc.Status.GetCondition(apis.ConditionReady)
	return cond != nil && cond.IsTrue()
}

func (r *EdgeApplicationHandoffReconciler) isTriggerReady(ctx context.Context, ns, name string) bool {
	var trg eventingv1.Trigger
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &trg); err != nil {
		return false
	}
	cond := trg.Status.GetCondition(apis.ConditionReady)
	return cond != nil && cond.IsTrue()
}

// --- Status helpers ---

func (r *EdgeApplicationHandoffReconciler) setPending(ctx context.Context, ho *mecv1alpha1.EdgeApplicationHandoff, reason, msg string) {
	r.setPhaseCondition(ctx, ho, "Pending", "Progressing", reason, msg, metav1.ConditionTrue)
}
func (r *EdgeApplicationHandoffReconciler) setApplied(ctx context.Context, ho *mecv1alpha1.EdgeApplicationHandoff, reason, msg string) {
	r.setPhaseCondition(ctx, ho, "Applied", "Progressing", reason, msg, metav1.ConditionTrue)
}
func (r *EdgeApplicationHandoffReconciler) setReady(ctx context.Context, ho *mecv1alpha1.EdgeApplicationHandoff, reason, msg string) {
	r.setPhaseCondition(ctx, ho, "Ready", "Ready", reason, msg, metav1.ConditionTrue)
}
func (r *EdgeApplicationHandoffReconciler) setFailed(ctx context.Context, ho *mecv1alpha1.EdgeApplicationHandoff, reason, msg string) {
	r.setPhaseCondition(ctx, ho, "Failed", "Ready", reason, msg, metav1.ConditionFalse)
}

func (r *EdgeApplicationHandoffReconciler) setPhaseCondition(
	ctx context.Context,
	ho *mecv1alpha1.EdgeApplicationHandoff,
	phase string,
	condType string,
	reason string,
	msg string,
	status metav1.ConditionStatus,
) {
	hoCopy := ho.DeepCopy()
	hoCopy.Status.Phase = phase
	hoCopy.Status.ObservedGeneration = hoCopy.Generation

	meta.SetStatusCondition(&hoCopy.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: hoCopy.Generation,
		LastTransitionTime: metav1.Now(),
	})

	_ = r.Status().Patch(ctx, hoCopy, client.MergeFrom(ho))
	*ho = *hoCopy
}

// --- Utilities ---

var dnsLabelReHO = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

func isDNSLabelHO(s string) bool {
	if len(s) < 1 || len(s) > 63 {
		return false
	}
	return dnsLabelReHO.MatchString(s)
}
