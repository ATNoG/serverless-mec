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
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	mecv1alpha1 "github.com/ATNoG/serverless-mec/operator/api/v1alpha1"
)

// EdgeApplicationReconciler reconciles an EdgeApplication object
type EdgeApplicationReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ConfigNamespace string
}

// +kubebuilder:rbac:groups=mec.atnog.org,resources=edgeapplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mec.atnog.org,resources=edgeapplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mec.atnog.org,resources=edgeapplications/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=eventing.knative.dev,resources=triggers,verbs=get;list;watch;create;update;patch;delete

type desiredService struct {
	Name          string
	Namespace     string
	PodSpec       corev1.PodSpec
	MinScale      *int32
	CreateTrigger bool
	TriggerNs     string
	TriggerName   string
	TriggerBroker string
	TriggerFilter map[string]string
}

const (
	defaultPinByLabelKey = "kubernetes.io/hostname"
)

var (
	reNonDNS = regexp.MustCompile(`[^a-z0-9-]+`)
)

func (r *EdgeApplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	// 1) Fetch the EdgeApplication
	var app mecv1alpha1.EdgeApplication
	if err := r.Get(ctx, req.NamespacedName, &app); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// If no "service" spec, do nothing (and also don't manage any knative resources)
	if app.Spec.Service == nil {
		logger.V(1).Info("EdgeApplication has no service spec; skipping Knative reconciliation",
			"edgeApplication", app.Name)
		return ctrl.Result{}, nil
	}

	// 2) Read operator config (broker info) from ConfigMap
	cfgNs := r.ConfigNamespace
	if cfgNs == "" {
		cfgNs = defaultConfigNs
	}

	var cfg corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{
		Name:      defaultConfigMapName,
		Namespace: cfgNs,
	}, &cfg); err != nil {
		logger.Error(err, "failed to get operator config ConfigMap",
			"configMap", defaultConfigMapName, "namespace", cfgNs)
		return ctrl.Result{}, err
	}

	brokerName := cfg.Data["default-broker-name"]
	brokerNamespace := cfg.Data["default-broker-namespace"]
	if brokerName == "" || brokerNamespace == "" {
		logger.Error(nil, "operator config is missing default-broker-name or default-broker-namespace",
			"configMap", defaultConfigMapName, "namespace", cfgNs)
		return ctrl.Result{}, nil
	}

	// 3) Base Knative inputs (ONLY from spec.service.*)
	baseEnv := buildEnvVars(app.Spec.Service.Container.Env)
	basePodSpec := corev1.PodSpec{
		Containers: []corev1.Container{{
			Image:     app.Spec.Service.Container.Image,
			Env:       baseEnv,
			Resources: app.Spec.Service.Container.Resources,
		}},
		NodeSelector: nilIfEmptyMap(app.Spec.Service.NodeSelector),
		Tolerations:  nilIfEmptyTolerations(app.Spec.Service.Tolerations),
		Affinity:     app.Spec.Service.Affinity,
	}

	baseMinScale := app.Spec.Service.MinScale // may be nil
	baseFilters := app.Spec.Service.TriggerFilters

	desired := make(map[string]desiredService)

	// ---- IMPORTANT CHANGE: daemon mode when autoReplicas is set ----
	daemonMode := len(app.Spec.AutoReplicas) > 0
	if daemonMode {
		logger.Info("autoReplicas enabled: daemon mode (no base service, no spec.replicas)",
			"rules", len(app.Spec.AutoReplicas))
	}

	// Normal mode: create base service + explicit replicas
	if !daemonMode {
		// Base service
		baseSvcName := app.Name
		desired[baseSvcName] = desiredService{
			Name:          baseSvcName,
			Namespace:     app.Namespace,
			PodSpec:       basePodSpec,
			MinScale:      baseMinScale,
			CreateTrigger: true,
			TriggerNs:     brokerNamespace,
			TriggerName:   baseSvcName + "-trigger",
			TriggerBroker: brokerName,
			TriggerFilter: baseFilters,
		}

		// Explicit replica services (spec.replicas)
		for _, rep := range app.Spec.Replicas {
			svcName := fmt.Sprintf("%s-%s", app.Name, rep.Name)

			repPod := basePodSpec
			repPod.NodeSelector = nilIfEmptyMap(rep.NodeSelector)
			repPod.Tolerations = nilIfEmptyTolerations(rep.Tolerations)
			repPod.Affinity = rep.Affinity

			createTrig := true
			if rep.CreateTrigger != nil {
				createTrig = *rep.CreateTrigger
			}

			filters := baseFilters
			if rep.TriggerFilters != nil {
				filters = rep.TriggerFilters
			}

			minScale := baseMinScale
			if rep.MinScale != nil {
				minScale = rep.MinScale
			}

			desired[svcName] = desiredService{
				Name:          svcName,
				Namespace:     app.Namespace,
				PodSpec:       repPod,
				MinScale:      minScale,
				CreateTrigger: createTrig,
				TriggerNs:     brokerNamespace,
				TriggerName:   svcName + "-trigger",
				TriggerBroker: brokerName,
				TriggerFilter: filters,
			}
		}
	}

	// Daemon mode: only auto replicas (one service per matching node)
	if daemonMode {
		for _, ar := range app.Spec.AutoReplicas {
			var nodeList corev1.NodeList
			if err := r.List(ctx, &nodeList, client.MatchingLabels(ar.MatchNodes)); err != nil {
				logger.Error(err, "autoReplicas: failed to list nodes", "matchNodes", ar.MatchNodes)
				return ctrl.Result{}, err
			}

			logger.Info("autoReplicas: matched nodes",
				"matchNodes", ar.MatchNodes,
				"count", len(nodeList.Items),
			)

			pinKey := ar.PinByLabelKey
			if pinKey == "" {
				pinKey = defaultPinByLabelKey
			}

			// rule-level minScale / trigger settings
			ruleMinScale := baseMinScale
			if ar.MinScale != nil {
				ruleMinScale = ar.MinScale
			}

			createTrig := true
			if ar.CreateTrigger != nil {
				createTrig = *ar.CreateTrigger
			}

			filters := baseFilters
			if ar.TriggerFilters != nil {
				filters = ar.TriggerFilters
			}

			for _, n := range nodeList.Items {
				// pin value for scheduling
				pinVal := n.Labels[pinKey]
				if pinVal == "" {
					if pinKey == defaultPinByLabelKey {
						pinVal = n.Name
					} else {
						logger.Info("autoReplicas: skipping node (missing pinByLabelKey label)",
							"node", n.Name, "pinByLabelKey", pinKey)
						continue
					}
				}

				// Name MUST be servicename-nodename (node name)
				svcName := makeServiceName(app.Name, n.Name)

				// start from base, then pin to a single node by label key/value
				repPod := basePodSpec
				repPod.NodeSelector = mergeNodeSelector(basePodSpec.NodeSelector, map[string]string{pinKey: pinVal})

				desired[svcName] = desiredService{
					Name:          svcName,
					Namespace:     app.Namespace,
					PodSpec:       repPod,
					MinScale:      ruleMinScale,
					CreateTrigger: createTrig,
					TriggerNs:     brokerNamespace,
					TriggerName:   svcName + "-trigger",
					TriggerBroker: brokerName,
					TriggerFilter: filters,
				}
			}
		}
	}

	// 4) Reconcile KServices
	for _, d := range desired {
		if err := r.reconcileKService(ctx, &app, d); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 5) Cleanup orphan KServices
	if err := r.cleanupOrphanKServices(ctx, &app, desired); err != nil {
		return ctrl.Result{}, err
	}

	// 6) Reconcile Triggers
	for _, d := range desired {
		if !d.CreateTrigger {
			continue
		}
		if err := r.reconcileTrigger(ctx, &app, d); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 7) Cleanup orphan Triggers
	if err := r.cleanupOrphanTriggers(ctx, &app, desired, brokerNamespace); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *EdgeApplicationReconciler) reconcileKService(ctx context.Context, app *mecv1alpha1.EdgeApplication, d desiredService) error {
	logger := logf.FromContext(ctx)

	appLabel := map[string]string{
		"mec.atnog.org/app": app.Name,
	}

	templateMeta := metav1.ObjectMeta{
		Labels: appLabel,
	}
	if d.MinScale != nil {
		templateMeta.Annotations = map[string]string{
			"autoscaling.knative.dev/minScale": strconv.FormatInt(int64(*d.MinScale), 10),
		}
	}

	desiredSvc := &servingv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.Name,
			Namespace: d.Namespace,
			Labels:    appLabel,
		},
		Spec: servingv1.ServiceSpec{
			ConfigurationSpec: servingv1.ConfigurationSpec{
				Template: servingv1.RevisionTemplateSpec{
					ObjectMeta: templateMeta,
					Spec: servingv1.RevisionSpec{
						PodSpec: d.PodSpec,
					},
				},
			},
		},
	}

	var existing servingv1.Service
	err := r.Get(ctx, types.NamespacedName{Name: d.Name, Namespace: d.Namespace}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if err := ctrl.SetControllerReference(app, desiredSvc, r.Scheme); err != nil {
				return err
			}
			logger.Info("creating Knative Service", "name", d.Name, "namespace", d.Namespace)
			if err := r.Create(ctx, desiredSvc); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return nil
				}
				return err
			}
			return nil
		}
		return err
	}

	updated := existing.DeepCopy()

	// PodSpec update
	ps := &updated.Spec.ConfigurationSpec.Template.Spec.PodSpec
	if len(ps.Containers) == 0 {
		ps.Containers = []corev1.Container{{}}
	}

	ps.Containers[0].Image = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Image
	ps.Containers[0].Env = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Env
	ps.Containers[0].Resources = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Resources

	ps.NodeSelector = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.NodeSelector
	ps.Tolerations = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Tolerations
	ps.Affinity = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Affinity

	// Ensure template label exists (pods selection)
	if updated.Spec.ConfigurationSpec.Template.Labels == nil {
		updated.Spec.ConfigurationSpec.Template.Labels = map[string]string{}
	}
	updated.Spec.ConfigurationSpec.Template.Labels["mec.atnog.org/app"] = app.Name

	// MinScale management
	if d.MinScale != nil {
		if updated.Spec.ConfigurationSpec.Template.Annotations == nil {
			updated.Spec.ConfigurationSpec.Template.Annotations = map[string]string{}
		}
		updated.Spec.ConfigurationSpec.Template.Annotations["autoscaling.knative.dev/minScale"] =
			strconv.FormatInt(int64(*d.MinScale), 10)
	} else {
		if updated.Spec.ConfigurationSpec.Template.Annotations != nil {
			delete(updated.Spec.ConfigurationSpec.Template.Annotations, "autoscaling.knative.dev/minScale")
			if len(updated.Spec.ConfigurationSpec.Template.Annotations) == 0 {
				updated.Spec.ConfigurationSpec.Template.Annotations = nil
			}
		}
	}

	// Keep KService labels for cleanup/listing
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	updated.Labels["mec.atnog.org/app"] = app.Name

	if equality.Semantic.DeepEqual(existing.Spec, updated.Spec) &&
		equality.Semantic.DeepEqual(existing.Labels, updated.Labels) {
		return nil
	}

	logger.Info("updating Knative Service", "name", d.Name, "namespace", d.Namespace)
	return r.Update(ctx, updated)
}

func (r *EdgeApplicationReconciler) reconcileTrigger(ctx context.Context, app *mecv1alpha1.EdgeApplication, d desiredService) error {
	logger := logf.FromContext(ctx)

	labels := map[string]string{
		"mec.atnog.org/app": app.Name,
	}

	desiredTrig := &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.TriggerName,
			Namespace: d.TriggerNs,
			Labels:    labels,
		},
		Spec: eventingv1.TriggerSpec{
			Broker: d.TriggerBroker,
			Filter: &eventingv1.TriggerFilter{
				Attributes: d.TriggerFilter,
			},
			Subscriber: duckv1.Destination{
				Ref: &duckv1.KReference{
					APIVersion: "serving.knative.dev/v1",
					Kind:       "Service",
					Name:       d.Name,
					Namespace:  d.Namespace,
				},
			},
		},
	}

	var existing eventingv1.Trigger
	err := r.Get(ctx, types.NamespacedName{Name: d.TriggerName, Namespace: d.TriggerNs}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if d.TriggerNs == app.Namespace {
				if err := ctrl.SetControllerReference(app, desiredTrig, r.Scheme); err != nil {
					return err
				}
			}

			logger.Info("creating Trigger", "name", d.TriggerName, "namespace", d.TriggerNs, "service", d.Name)
			if err := r.Create(ctx, desiredTrig); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return nil
				}
				return err
			}
			return nil
		}
		return err
	}

	if equality.Semantic.DeepEqual(existing.Spec, desiredTrig.Spec) {
		return nil
	}

	existing.Spec = desiredTrig.Spec
	logger.Info("updating Trigger", "name", d.TriggerName, "namespace", d.TriggerNs, "service", d.Name)
	return r.Update(ctx, &existing)
}

func (r *EdgeApplicationReconciler) cleanupOrphanKServices(ctx context.Context, app *mecv1alpha1.EdgeApplication, desired map[string]desiredService) error {
	var svcList servingv1.ServiceList
	if err := r.List(ctx, &svcList,
		client.InNamespace(app.Namespace),
		client.MatchingLabels{"mec.atnog.org/app": app.Name},
	); err != nil {
		return err
	}

	for i := range svcList.Items {
		svc := &svcList.Items[i]
		if _, ok := desired[svc.Name]; ok {
			continue
		}
		if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *EdgeApplicationReconciler) cleanupOrphanTriggers(ctx context.Context, app *mecv1alpha1.EdgeApplication, desired map[string]desiredService, triggerNs string) error {
	desiredTriggerNames := map[string]struct{}{}
	for _, d := range desired {
		if d.CreateTrigger {
			desiredTriggerNames[d.TriggerName] = struct{}{}
		}
	}

	var trigList eventingv1.TriggerList
	if err := r.List(ctx, &trigList,
		client.InNamespace(triggerNs),
		client.MatchingLabels{"mec.atnog.org/app": app.Name},
	); err != nil {
		return err
	}

	for i := range trigList.Items {
		tr := &trigList.Items[i]
		if _, ok := desiredTriggerNames[tr.Name]; ok {
			continue
		}
		if err := r.Delete(ctx, tr); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func buildEnvVars(in []mecv1alpha1.NameValuePair) []corev1.EnvVar {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.EnvVar, 0, len(in))
	for _, e := range in {
		out = append(out, corev1.EnvVar{Name: e.Name, Value: e.Value})
	}
	return out
}

func nilIfEmptyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return m
}

func nilIfEmptyTolerations(t []corev1.Toleration) []corev1.Toleration {
	if len(t) == 0 {
		return nil
	}
	return t
}

func mergeNodeSelector(base, extra map[string]string) map[string]string {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func dnsLabelize(s string) string {
	s = strings.ToLower(s)
	s = reNonDNS.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if s == "" {
		s = "x"
	}
	return s
}

func shortHash(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:])[:8]
}

func makeServiceName(base, suffix string) string {
	base = dnsLabelize(base)
	suffix = dnsLabelize(suffix)

	maxSuffix := 63 - len(base) - 1
	if maxSuffix < 1 {
		h := shortHash(base)
		base = base[:min(len(base), 54)] + "-" + h
		maxSuffix = 63 - len(base) - 1
		if maxSuffix < 1 {
			return base
		}
	}

	if len(suffix) > maxSuffix {
		h := shortHash(suffix)
		cut := maxSuffix - (1 + len(h))
		if cut < 1 {
			suffix = h
		} else {
			suffix = suffix[:cut] + "-" + h
		}
	}
	return base + "-" + suffix
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (r *EdgeApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ConfigNamespace == "" {
		r.ConfigNamespace = defaultConfigNs
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mecv1alpha1.EdgeApplication{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Owns(&servingv1.Service{}).
		Owns(&eventingv1.Trigger{}).
		Named("edgeapplication").
		Complete(r)
}
