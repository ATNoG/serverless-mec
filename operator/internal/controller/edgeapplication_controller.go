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
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mecv1alpha1 "github.com/ATNoG/serverless-mec/operator/api/v1alpha1"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
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
	FreezeEnabled bool
	CreateTrigger bool

	TriggerNs      string
	TriggerBroker  string
	TriggerFilters []map[string]string
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

	// If no "service" spec, do nothing
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

	// 3) Base Knative inputs
	baseEnv := buildEnvVars(app.Spec.Service.Container.Env)

	basePodSpec := corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:            app.Spec.Service.Container.Name,
			Image:           app.Spec.Service.Container.Image,
			Command:         nilIfEmptyStrings(app.Spec.Service.Container.Command),
			Args:            nilIfEmptyStrings(app.Spec.Service.Container.Args),
			Env:             baseEnv,
			Resources:       app.Spec.Service.Container.Resources,
			SecurityContext: app.Spec.Service.Container.SecurityContext,
			VolumeMounts:    nilIfEmptyVolumeMounts(app.Spec.Service.Container.VolumeMounts),
		}},
		NodeSelector:                 nilIfEmptyMap(app.Spec.Service.NodeSelector),
		Tolerations:                  nilIfEmptyTolerations(app.Spec.Service.Tolerations),
		Affinity:                     app.Spec.Service.Affinity,
		HostNetwork:                  app.Spec.Service.HostNetwork,
		DNSPolicy:                    app.Spec.Service.DNSPolicy,
		SecurityContext:              app.Spec.Service.PodSecurityContext,
		Volumes:                      nilIfEmptyVolumes(app.Spec.Service.Volumes),
		ServiceAccountName:           app.Spec.Service.ServiceAccountName,
		AutomountServiceAccountToken: app.Spec.Service.AutomountServiceAccountToken,
	}

	if basePodSpec.HostNetwork && basePodSpec.DNSPolicy == "" {
		basePodSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}

	baseMinScale := app.Spec.Service.MinScale
	baseFreezeEnabled := app.Spec.Service.FreezeEnabled != nil && *app.Spec.Service.FreezeEnabled
	baseFilters := app.Spec.Service.TriggerFilters

	createTriggers := len(baseFilters) > 0

	// Background app readiness probe when no trigger exists.
	if !createTriggers {
		basePodSpec.Containers[0].ReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "exit 0"}},
			},
			PeriodSeconds:    5,
			TimeoutSeconds:   1,
			FailureThreshold: 1,
		}
	}

	desired := make(map[string]desiredService)

	daemonMode := len(app.Spec.AutoReplicas) > 0
	if daemonMode {
		logger.Info("autoReplicas enabled: daemon mode (no base service, no spec.replicas)",
			"rules", len(app.Spec.AutoReplicas))
	}

	// Normal mode: create base service + explicit replicas
	if !daemonMode {
		baseSvcName := app.Name
		desired[baseSvcName] = desiredService{
			Name:           baseSvcName,
			Namespace:      app.Namespace,
			PodSpec:        basePodSpec,
			MinScale:       baseMinScale,
			FreezeEnabled:  baseFreezeEnabled,
			CreateTrigger:  createTriggers,
			TriggerNs:      brokerNamespace,
			TriggerBroker:  brokerName,
			TriggerFilters: baseFilters,
		}

		for _, rep := range app.Spec.Replicas {
			svcName := fmt.Sprintf("%s-%s", app.Name, rep.Name)

			repPod := basePodSpec
			repPod.NodeSelector = nilIfEmptyMap(rep.NodeSelector)
			repPod.Tolerations = nilIfEmptyTolerations(rep.Tolerations)
			repPod.Affinity = rep.Affinity

			desired[svcName] = desiredService{
				Name:           svcName,
				Namespace:      app.Namespace,
				PodSpec:        repPod,
				MinScale:       baseMinScale,
				FreezeEnabled:  baseFreezeEnabled,
				CreateTrigger:  createTriggers,
				TriggerNs:      brokerNamespace,
				TriggerBroker:  brokerName,
				TriggerFilters: baseFilters,
			}
		}
	}

	// Daemon mode: one KService per matching node
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

			pinKey := defaultPinByLabelKey

			for _, n := range nodeList.Items {
				pinVal := n.Labels[pinKey]
				if pinVal == "" {
					pinVal = n.Name
				}

				svcName := makeServiceName(app.Name, n.Name)

				repPod := basePodSpec
				repPod.NodeSelector = mergeNodeSelector(basePodSpec.NodeSelector, map[string]string{pinKey: pinVal})

				desired[svcName] = desiredService{
					Name:           svcName,
					Namespace:      app.Namespace,
					PodSpec:        repPod,
					MinScale:       baseMinScale,
					FreezeEnabled:  baseFreezeEnabled,
					CreateTrigger:  createTriggers,
					TriggerNs:      brokerNamespace,
					TriggerBroker:  brokerName,
					TriggerFilters: baseFilters,
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
		if err := r.reconcileTriggers(ctx, &app, d); err != nil {
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
	templateAnnotations := map[string]string{}
	if d.MinScale != nil {
		templateAnnotations["autoscaling.knative.dev/minScale"] = strconv.FormatInt(int64(*d.MinScale), 10)
	}
	if d.FreezeEnabled {
		templateAnnotations["qpoption.knative.dev/freezer-activate"] = "enable"
	}
	if len(templateAnnotations) > 0 {
		templateMeta.Annotations = templateAnnotations
	}

	// Inject HOST_IP env var via Downward API when freeze is enabled
	if d.FreezeEnabled {
		hostIPEnv := corev1.EnvVar{
			Name: "HOST_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.hostIP",
				},
			},
		}
		d.PodSpec.Containers[0].Env = append(d.PodSpec.Containers[0].Env, hostIPEnv)
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

	ps := &updated.Spec.ConfigurationSpec.Template.Spec.PodSpec
	if len(ps.Containers) == 0 {
		ps.Containers = []corev1.Container{{}}
	}

	ps.Containers[0].Name = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Name
	ps.Containers[0].Image = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Image
	ps.Containers[0].Command = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Command
	ps.Containers[0].Args = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Args
	ps.Containers[0].Env = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Env
	ps.Containers[0].Resources = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].Resources
	ps.Containers[0].SecurityContext = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].SecurityContext
	ps.Containers[0].VolumeMounts = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].VolumeMounts
	ps.Containers[0].ReadinessProbe = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Containers[0].ReadinessProbe

	ps.NodeSelector = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.NodeSelector
	ps.Tolerations = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Tolerations
	ps.Affinity = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Affinity
	ps.HostNetwork = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.HostNetwork
	ps.DNSPolicy = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.DNSPolicy
	ps.SecurityContext = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.SecurityContext
	ps.Volumes = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.Volumes
	ps.ServiceAccountName = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.ServiceAccountName
	ps.AutomountServiceAccountToken = desiredSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec.AutomountServiceAccountToken

	if updated.Spec.ConfigurationSpec.Template.Labels == nil {
		updated.Spec.ConfigurationSpec.Template.Labels = map[string]string{}
	}
	updated.Spec.ConfigurationSpec.Template.Labels["mec.atnog.org/app"] = app.Name

	if updated.Spec.ConfigurationSpec.Template.Annotations == nil {
		updated.Spec.ConfigurationSpec.Template.Annotations = map[string]string{}
	}
	if d.MinScale != nil {
		updated.Spec.ConfigurationSpec.Template.Annotations["autoscaling.knative.dev/minScale"] =
			strconv.FormatInt(int64(*d.MinScale), 10)
	} else {
		delete(updated.Spec.ConfigurationSpec.Template.Annotations, "autoscaling.knative.dev/minScale")
	}
	if d.FreezeEnabled {
		updated.Spec.ConfigurationSpec.Template.Annotations["qpoption.knative.dev/freezer-activate"] = "enable"
	} else {
		delete(updated.Spec.ConfigurationSpec.Template.Annotations, "qpoption.knative.dev/freezer-activate")
	}
	if len(updated.Spec.ConfigurationSpec.Template.Annotations) == 0 {
		updated.Spec.ConfigurationSpec.Template.Annotations = nil
	}

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

func (r *EdgeApplicationReconciler) reconcileTriggers(ctx context.Context, app *mecv1alpha1.EdgeApplication, d desiredService) error {
	for i, filter := range d.TriggerFilters {
		triggerName := fmt.Sprintf("%s-trigger-%d", d.Name, i)
		if err := r.reconcileOneTrigger(ctx, app, d, triggerName, filter); err != nil {
			return err
		}
	}
	return nil
}

func (r *EdgeApplicationReconciler) reconcileOneTrigger(
	ctx context.Context,
	app *mecv1alpha1.EdgeApplication,
	d desiredService,
	triggerName string,
	filter map[string]string,
) error {
	logger := logf.FromContext(ctx)

	labels := map[string]string{
		"mec.atnog.org/app": app.Name,
	}

	desiredTrig := &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      triggerName,
			Namespace: d.TriggerNs,
			Labels:    labels,
		},
		Spec: eventingv1.TriggerSpec{
			Broker: d.TriggerBroker,
			Filter: &eventingv1.TriggerFilter{
				Attributes: filter,
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
	err := r.Get(ctx, types.NamespacedName{Name: triggerName, Namespace: d.TriggerNs}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if d.TriggerNs == app.Namespace {
				if err := ctrl.SetControllerReference(app, desiredTrig, r.Scheme); err != nil {
					return err
				}
			}

			logger.Info("creating Trigger", "name", triggerName, "namespace", d.TriggerNs, "service", d.Name)
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
	logger.Info("updating Trigger", "name", triggerName, "namespace", d.TriggerNs, "service", d.Name)
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
		if !d.CreateTrigger {
			continue
		}
		for i := range d.TriggerFilters {
			name := fmt.Sprintf("%s-trigger-%d", d.Name, i)
			desiredTriggerNames[name] = struct{}{}
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

func buildEnvVars(in []mecv1alpha1.EnvVarSpec) []corev1.EnvVar {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.EnvVar, 0, len(in))
	for _, e := range in {
		out = append(out, corev1.EnvVar{
			Name:      e.Name,
			Value:     e.Value,
			ValueFrom: e.ValueFrom,
		})
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

func nilIfEmptyVolumes(v []corev1.Volume) []corev1.Volume {
	if len(v) == 0 {
		return nil
	}
	return v
}

func nilIfEmptyVolumeMounts(v []corev1.VolumeMount) []corev1.VolumeMount {
	if len(v) == 0 {
		return nil
	}
	return v
}

func nilIfEmptyStrings(v []string) []string {
	if len(v) == 0 {
		return nil
	}
	return v
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

// When a Node is added/removed or its labels change, requeue all EdgeApplications
// that are in daemon mode (have spec.autoReplicas set).
func (r *EdgeApplicationReconciler) nodeToDaemonEdgeApps(ctx context.Context, obj client.Object) []reconcile.Request {
	if _, ok := obj.(*corev1.Node); !ok {
		return nil
	}

	var apps mecv1alpha1.EdgeApplicationList
	if err := r.List(ctx, &apps); err != nil {
		return nil
	}

	reqs := make([]reconcile.Request, 0, len(apps.Items))
	for i := range apps.Items {
		a := &apps.Items[i]
		if a.Spec.Service == nil {
			continue
		}
		if len(a.Spec.AutoReplicas) == 0 {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      a.Name,
				Namespace: a.Namespace,
			},
		})
	}
	return reqs
}

func (r *EdgeApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ConfigNamespace == "" {
		r.ConfigNamespace = defaultConfigNs
	}

	nodePred := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return true },
		DeleteFunc: func(e event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, ok1 := e.ObjectOld.(*corev1.Node)
			newNode, ok2 := e.ObjectNew.(*corev1.Node)
			if !ok1 || !ok2 {
				return true
			}
			return !equality.Semantic.DeepEqual(oldNode.Labels, newNode.Labels)
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mecv1alpha1.EdgeApplication{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Owns(&servingv1.Service{}).
		Owns(&eventingv1.Trigger{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.nodeToDaemonEdgeApps),
			builder.WithPredicates(nodePred),
		).
		Named("edgeapplication").
		Complete(r)
}
