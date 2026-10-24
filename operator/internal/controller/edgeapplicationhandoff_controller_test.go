package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mecv1alpha1 "github.com/ATNoG/serverless-mec/operator/api/v1alpha1"
)

func TestTerminateSource(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := mecv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	newApp := func() *mecv1alpha1.EdgeApplication {
		return &mecv1alpha1.EdgeApplication{
			ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default"},
			Spec: mecv1alpha1.EdgeApplicationSpec{
				Replicas: []mecv1alpha1.KnativeServiceReplicaSpec{{Name: "rsu-a"}, {Name: "rsu-b"}},
			},
		}
	}
	handoff := func(source, target string) *mecv1alpha1.EdgeApplicationHandoff {
		return &mecv1alpha1.EdgeApplicationHandoff{
			Spec: mecv1alpha1.EdgeApplicationHandoffSpec{
				EdgeApplicationName: "app",
				SourceReplicaName:   source,
				TargetReplicaName:   target,
			},
		}
	}

	cases := []struct {
		name         string
		ho           *mecv1alpha1.EdgeApplicationHandoff
		wantReplicas []string
		wantDisabled bool
	}{
		{"no source replica disables the base service", handoff("", "rsu-b"), []string{"rsu-a", "rsu-b"}, true},
		{"named source replica is removed", handoff("rsu-a", "rsu-b"), []string{"rsu-b"}, false},
		{"source equal to target is kept", handoff("rsu-b", "rsu-b"), []string{"rsu-a", "rsu-b"}, false},
		{"missing source replica is a no-op", handoff("rsu-c", "rsu-b"), []string{"rsu-a", "rsu-b"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			app := newApp()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(app).Build()
			r := &EdgeApplicationHandoffReconciler{Client: c, Scheme: scheme}

			// Twice, to check it is idempotent across Ready reconciles.
			for i := 0; i < 2; i++ {
				if err := r.terminateSource(ctx, app, tc.ho); err != nil {
					t.Fatal(err)
				}
			}

			var got mecv1alpha1.EdgeApplication
			if err := c.Get(ctx, types.NamespacedName{Name: "app", Namespace: "default"}, &got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.DisableBaseService != tc.wantDisabled {
				t.Errorf("disableBaseService = %v, want %v", got.Spec.DisableBaseService, tc.wantDisabled)
			}
			var names []string
			for _, rep := range got.Spec.Replicas {
				names = append(names, rep.Name)
			}
			if len(names) != len(tc.wantReplicas) {
				t.Fatalf("replicas = %v, want %v", names, tc.wantReplicas)
			}
			for i := range names {
				if names[i] != tc.wantReplicas[i] {
					t.Fatalf("replicas = %v, want %v", names, tc.wantReplicas)
				}
			}
		})
	}
}
