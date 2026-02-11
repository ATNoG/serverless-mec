package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=eventing.knative.dev,resources=triggers,verbs=get;list;watch;create;update;patch;delete

func (r *EdgeApplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	// 1. Fetch the EdgeApplication
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

	// 2. Read operator config (broker info) from mec-operator-config
	cfgNs := r.ConfigNamespace
	if cfgNs == "" {
		cfgNs = "mec-system"
	}

	var cfg corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{
		Name:      "mec-operator-config",
		Namespace: cfgNs,
	}, &cfg); err != nil {
		logger.Error(err, "failed to get mec-operator-config ConfigMap")
		return ctrl.Result{}, err
	}

	brokerName := cfg.Data["default-broker-name"]
	brokerNamespace := cfg.Data["default-broker-namespace"]
	if brokerName == "" || brokerNamespace == "" {
		logger.Error(nil, "mec-operator-config is missing default-broker-name or default-broker-namespace")
		return ctrl.Result{}, nil
	}

	// 3. Desired Knative Service (same name & ns as EdgeApplication)
	svcName := app.Name
	svcNs := app.Namespace

	// Convert []NameValuePair -> []corev1.EnvVar
	env := make([]corev1.EnvVar, 0, len(app.Spec.Service.Container.Env))
	for _, e := range app.Spec.Service.Container.Env {
		env = append(env, corev1.EnvVar{
			Name:  e.Name,
			Value: e.Value,
		})
	}

	// Build PodSpec from spec.service.*
	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{{
			Image:     app.Spec.Service.Container.Image,
			Env:       env,
			Resources: app.Spec.Service.Container.Resources,
		}},
		NodeSelector: app.Spec.Service.NodeSelector,
		Tolerations:  app.Spec.Service.Tolerations,
		Affinity:     app.Spec.Service.Affinity,
	}

	desiredSvc := &servingv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: svcNs,
			Labels: map[string]string{
				"mec.atnog.org/app": app.Name,
			},
		},
		Spec: servingv1.ServiceSpec{
			ConfigurationSpec: servingv1.ConfigurationSpec{
				Template: servingv1.RevisionTemplateSpec{
					Spec: servingv1.RevisionSpec{
						PodSpec: podSpec,
					},
				},
			},
		},
	}

	// 3a. Create / update Knative Service, but PRESERVE existing template metadata
	var existingSvc servingv1.Service
	if err := r.Get(ctx, types.NamespacedName{
		Name: svcName, Namespace: svcNs,
	}, &existingSvc); err != nil {
		if apierrors.IsNotFound(err) {
			if err := ctrl.SetControllerReference(&app, desiredSvc, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, desiredSvc); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			return ctrl.Result{}, err
		}
	} else {
		updatedSvc := existingSvc.DeepCopy()
		ps := &updatedSvc.Spec.ConfigurationSpec.Template.Spec.PodSpec

		if len(ps.Containers) == 0 {
			ps.Containers = []corev1.Container{{}}
		}

		ps.Containers[0].Image = app.Spec.Service.Container.Image
		ps.Containers[0].Env = env
		ps.Containers[0].Resources = app.Spec.Service.Container.Resources

		ps.NodeSelector = app.Spec.Service.NodeSelector
		ps.Tolerations = app.Spec.Service.Tolerations
		ps.Affinity = app.Spec.Service.Affinity

		if err := r.Update(ctx, updatedSvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Trigger
	trigName := svcName + "-trigger"
	trigNs := brokerNamespace

	desiredTrig := &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      trigName,
			Namespace: trigNs,
			Labels: map[string]string{
				"mec.atnog.org/app": app.Name,
			},
		},
		Spec: eventingv1.TriggerSpec{
			Broker: brokerName,
			Filter: &eventingv1.TriggerFilter{
				Attributes: app.Spec.Service.TriggerFilters,
			},
			Subscriber: duckv1.Destination{
				Ref: &duckv1.KReference{
					APIVersion: "serving.knative.dev/v1",
					Kind:       "Service",
					Name:       svcName,
					Namespace:  svcNs,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(&app, desiredTrig, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingTrig eventingv1.Trigger
	if err := r.Get(ctx, types.NamespacedName{
		Name: trigName, Namespace: trigNs,
	}, &existingTrig); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, desiredTrig); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			return ctrl.Result{}, err
		}
	} else {
		existingTrig.Spec = desiredTrig.Spec
		if err := r.Update(ctx, &existingTrig); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *EdgeApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ConfigNamespace == "" {
		r.ConfigNamespace = "mec-system"
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mecv1alpha1.EdgeApplication{}).
		Owns(&servingv1.Service{}).
		Owns(&eventingv1.Trigger{}).
		Named("edgeapplication").
		Complete(r)
}
