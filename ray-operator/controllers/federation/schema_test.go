package federation

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// Exercise the generated CRDs against an API server, including CEL transition
// rules that fake clients and controller-only validation cannot cover.
func TestFederationSchemaAdmission(t *testing.T) {
	environment := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases", "ray.io_federatedrayclusters.yaml"),
			filepath.Join("..", "..", "config", "crd", "bases", "ray.io_rayclusters.yaml"),
			filepath.Join("..", "..", "config", "crd", "bases", "ray.io_rayjobs.yaml"),
			filepath.Join("..", "..", "config", "crd", "bases", "ray.io_rayservices.yaml"),
		},
		ErrorIfCRDPathMissing: true,
	}
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	config, err := environment.Start()
	require.NoError(t, err)
	c, err := client.NewWithWatch(config, client.Options{Scheme: testScheme(t)})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "federation-schema"}}))
	t.Run("real-api-delete-ownership-race", func(t *testing.T) {
		identity := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}}
		if err := c.Get(ctx, client.ObjectKeyFromObject(identity), identity); apierrors.IsNotFound(err) {
			require.NoError(t, c.Create(ctx, identity))
		} else {
			require.NoError(t, err)
		}
		frc, child := testFederation(), testMember(t)
		child.Namespace, child.Name, child.UID, child.ResourceVersion = "federation-schema", "delete-race", "", ""
		require.NoError(t, c.Create(ctx, child))
		member := boundTestMember(frc)
		member.Namespace, member.RayClusterName, member.RayClusterUID, member.ClusterUID = child.Namespace, child.Name, child.UID, identity.UID
		remote := interceptor.NewClient(c, interceptor.Funcs{Delete: func(ctx context.Context, current client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			latest := &rayv1.RayCluster{}
			require.NoError(t, current.Get(ctx, client.ObjectKeyFromObject(object), latest))
			latest.Labels[OwnerLabel] = "new-owner"
			require.NoError(t, current.Update(ctx, latest))
			return current.Delete(ctx, object, options...)
		}})
		r := &FederatedReconciler{Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
		done, err := r.cleanupMember(ctx, frc, member)
		require.True(t, apierrors.IsConflict(err), "%v", err)
		require.False(t, done)
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(child), child))
		require.Equal(t, "new-owner", child.Labels[OwnerLabel])
		require.True(t, child.DeletionTimestamp.IsZero())
	})
	t.Run("primary-autoscaler-options", func(t *testing.T) {
		frc := autoscalingFederation()
		frc.ObjectMeta = metav1.ObjectMeta{Name: "autoscaler-api", Namespace: "federation-schema"}
		frc.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{Version: new(rayv1.AutoscalerVersionV2)}
		require.NoError(t, c.Create(ctx, frc))
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), frc))
		require.True(t, *frc.Spec.PrimaryCluster.EnableInTreeAutoscaling)
		require.Equal(t, rayv1.AutoscalerVersionV2, *frc.Spec.PrimaryCluster.AutoscalerOptions.Version)
		object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(frc)
		require.NoError(t, err)
		require.NoError(t, unstructured.SetNestedField(object, true, "spec", "enableInTreeAutoscaling"))
		legacy := &unstructured.Unstructured{Object: object}
		legacy.SetGroupVersionKind(rayv1.GroupVersion.WithKind("FederatedRayCluster"))
		require.ErrorContains(t, c.Update(ctx, legacy, &client.UpdateOptions{FieldValidation: "Strict"}), "unknown field")
	})
	for _, tt := range []struct {
		name   string
		mutate func(*rayv1.FederatedRayCluster)
		valid  bool
	}{
		{"manual", func(*rayv1.FederatedRayCluster) {}, true},
		{"managed", func(f *rayv1.FederatedRayCluster) { f.Spec.MemberClusters = testFederation().Spec.MemberClusters }, true},
		{"empty-ref", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].KubeconfigSecretRef = &corev1.LocalObjectReference{}
		}, false},
		{"manual-groups", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].WorkerGroups = testFederation().Spec.MemberClusters[0].WorkerGroups
		}, false},
		{"managed-no-groups", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].KubeconfigSecretRef = &corev1.LocalObjectReference{Name: "credential"}
		}, false},
		{"missing-primary-head", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.HeadGroupSpec = nil }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			frc := manualFederation()
			frc.ObjectMeta = metav1.ObjectMeta{Name: tt.name, Namespace: "federation-schema"}
			tt.mutate(frc)
			err := c.Create(ctx, frc)
			if !tt.valid {
				require.True(t, apierrors.IsInvalid(err), "expected validation failure, got %v", err)
				return
			}
			require.NoError(t, err)
			if tt.name == "manual" {
				frc.Spec.MemberClusters = testFederation().Spec.MemberClusters
			} else {
				frc.Spec.MemberClusters = manualFederation().Spec.MemberClusters
			}
			err = c.Update(ctx, frc)
			require.True(t, apierrors.IsInvalid(err), "management transition must fail: %v", err)
			require.ErrorContains(t, err, "management cannot change in place")
		})
	}
	for _, scope := range []string{"primary", "member"} {
		for _, manager := range []struct {
			name  string
			value string
			valid bool
		}{
			{"local", rayv1.WorkerGroupManagedByRayCluster, true},
			{"federation", rayv1.WorkerGroupManagedByFederatedRayCluster, false},
		} {
			t.Run("input-manager-"+scope+"-"+manager.name, func(t *testing.T) {
				frc := federationWithLocalWorkers()
				frc.ObjectMeta = metav1.ObjectMeta{Name: "input-" + scope + "-" + manager.name, Namespace: "federation-schema"}
				group := &frc.Spec.PrimaryCluster.WorkerGroups[0]
				if scope == "member" {
					group = &frc.Spec.MemberClusters[0].WorkerGroups[0]
				}
				group.ManagedBy = new(manager.value)
				err := c.Create(ctx, frc)
				if manager.valid {
					require.NoError(t, err)
				} else {
					require.True(t, apierrors.IsInvalid(err), "FRC assigns generated group managers: %v", err)
				}
			})
		}
	}
	mrc := manualMember(t)
	mrc.ObjectMeta = metav1.ObjectMeta{Name: "manual", Namespace: "federation-schema"}
	require.NoError(t, c.Create(ctx, mrc), "standalone member uses existing address parameters without a new mode field")
	require.Nil(t, mrc.Spec.HeadGroupSpec)
	mrc.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	require.NoError(t, c.Update(ctx, mrc), "workers-only groups allow explicit local management")
	for _, tt := range []struct {
		name   string
		mutate func(*rayv1.RayCluster)
	}{
		{"empty-head", func(c *rayv1.RayCluster) { c.Spec.HeadGroupSpec = &rayv1.HeadGroupSpec{} }},
		{"missing-workers", func(c *rayv1.RayCluster) { c.Spec.WorkerGroupSpecs = nil }},
		{"missing-address", func(c *rayv1.RayCluster) { delete(c.Spec.WorkerGroupSpecs[0].RayStartParams, "address") }},
		{"autoscale", func(c *rayv1.RayCluster) { c.Spec.EnableInTreeAutoscaling = new(true) }},
		{"nested-federation-manager", func(c *rayv1.RayCluster) {
			c.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
		}},
		{"head-options", func(c *rayv1.RayCluster) { c.Spec.HistoryServerOptions = &rayv1.HistoryServerOptions{} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cluster := manualMember(t)
			cluster.ObjectMeta = metav1.ObjectMeta{Name: tt.name, Namespace: "federation-schema"}
			tt.mutate(cluster)
			require.True(t, apierrors.IsInvalid(c.Create(ctx, cluster)), "invalid workers-only configuration must be rejected by the API")
		})
	}
	mrc.Spec.HeadGroupSpec = testFederation().Spec.PrimaryCluster.HeadGroupSpec.DeepCopy()
	require.ErrorContains(t, c.Update(ctx, mrc), "adding or removing headGroupSpec")
	full := manualMember(t)
	full.ObjectMeta = metav1.ObjectMeta{Name: "full-cluster", Namespace: "federation-schema"}
	full.Spec.HeadGroupSpec = testFederation().Spec.PrimaryCluster.HeadGroupSpec.DeepCopy()
	full.Spec.ManagedBy = new("ray.io/kuberay-operator")
	require.NoError(t, c.Create(ctx, full))
	for _, tt := range []struct {
		name      string
		managedBy any
		valid     bool
	}{
		{"default-manager", nil, true},
		{"federation-manager", rayv1.WorkerGroupManagedByFederatedRayCluster, true},
		{"local-manager", rayv1.WorkerGroupManagedByRayCluster, true},
		{"empty-manager", "", false},
		{"unknown-manager", "example.io/unknown-controller", false},
		{"operator-name-is-cluster-scoped", "ray.io/kuberay-operator", false},
		{"legacy-federation-manager", "ray.io/federation-controller", false},
		{"multikueue-manager", "kueue.x-k8s.io/multikueue", false},
		{"boolean-manager", true, false},
		{"object-manager", map[string]any{"mode": "External"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cluster := full.DeepCopy()
			cluster.TypeMeta = metav1.TypeMeta{APIVersion: "ray.io/v1", Kind: "RayCluster"}
			cluster.ObjectMeta = metav1.ObjectMeta{Name: tt.name, Namespace: full.Namespace}
			object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			require.NoError(t, err)
			groups, _, err := unstructured.NestedSlice(object, "spec", "workerGroupSpecs")
			require.NoError(t, err)
			if tt.managedBy != nil {
				groups[0].(map[string]any)["managedBy"] = tt.managedBy
			}
			require.NoError(t, unstructured.SetNestedSlice(object, groups, "spec", "workerGroupSpecs"))
			resource := &unstructured.Unstructured{Object: object}
			err = c.Create(ctx, resource)
			if tt.valid {
				require.NoError(t, err, "group manager admission requires no FRC reference or owner")
				for _, manager := range []string{rayv1.WorkerGroupManagedByFederatedRayCluster, rayv1.WorkerGroupManagedByRayCluster} {
					groups, _, err = unstructured.NestedSlice(resource.Object, "spec", "workerGroupSpecs")
					require.NoError(t, err)
					groups[0].(map[string]any)["managedBy"] = manager
					require.NoError(t, unstructured.SetNestedSlice(resource.Object, groups, "spec", "workerGroupSpecs"))
					require.NoError(t, c.Update(ctx, resource), "group management remains mutable")
				}
			} else {
				require.True(t, apierrors.IsInvalid(err), "invalid managedBy must be rejected: %v", err)
			}
		})
	}
	full.Spec.HeadGroupSpec = nil
	require.ErrorContains(t, c.Update(ctx, full), "adding or removing headGroupSpec")
	job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: "no-head", Namespace: "federation-schema"}, Spec: rayv1.RayJobSpec{Entrypoint: "echo test", RayClusterSpec: &full.Spec}}
	require.ErrorContains(t, c.Create(ctx, job), "requires a RayCluster with a head")
	service := &rayv1.RayService{ObjectMeta: metav1.ObjectMeta{Name: "no-head", Namespace: "federation-schema"}, Spec: rayv1.RayServiceSpec{RayClusterSpec: full.Spec}}
	require.ErrorContains(t, c.Create(ctx, service), "requires a RayCluster with a head")

	t.Run("persisted-identity-and-primary-revision", func(t *testing.T) {
		frc := federationWithLocalWorkers()
		frc.ObjectMeta = metav1.ObjectMeta{Name: "primary-revision", Namespace: "federation-schema"}
		require.NoError(t, c.Create(ctx, frc))
		frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
		require.NoError(t, c.Status().Update(ctx, frc))
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), frc))
		require.EqualValues(t, testMemberClusterUID, frc.Status.MemberClusterStatuses[0].ClusterUID)
		require.Equal(t, memberRayClusterName(frc.Name, "member-b"), frc.Status.MemberClusterStatuses[0].RayClusterName)
		r := &FederatedReconciler{Client: c, Reader: c, Scheme: c.Scheme()}
		primary, err := r.ensurePrimary(ctx, frc)
		require.NoError(t, err)
		version := primary.ResourceVersion
		for range 3 {
			require.NoError(t, r.ValidatePrimaryUpdate(ctx, frc))
			primary, err = r.ensurePrimary(ctx, frc)
			require.NoError(t, err)
			require.Equal(t, version, primary.ResourceVersion, "API defaults must not cause false upgrade rejection or repeated writes")
		}
	})
}
