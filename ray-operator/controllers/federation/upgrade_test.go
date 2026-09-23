package federation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func federationWithLocalWorkers() *rayv1.FederatedRayCluster {
	frc := testFederation()
	group := frc.Spec.MemberClusters[0].WorkerGroups[0].DeepCopy()
	group.GroupName = "local-workers"
	frc.Spec.PrimaryCluster.WorkerGroups = []rayv1.WorkerGroupSpec{*group}
	return frc
}

func TestUnsafePrimaryUpdatesStopBeforeTopologyChanges(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*rayv1.FederatedRayCluster)
	}{
		{"ray-version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "2.57.0" }},
		{"head-image", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.Containers[0].Image = "rayproject/ray:2.57.0"
		}},
		{"head-start-parameters", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = map[string]string{"num-cpus": "3"}
		}},
		{"local-worker-image", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.WorkerGroups[0].Template.Spec.Containers[0].Image = "rayproject/ray:2.57.0"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, scheme := context.Background(), testScheme(t)
			frc, mrc := federationWithLocalWorkers(), testMember(t)
			frc.Finalizers = []string{Finalizer}
			frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
			local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
			remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
			r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
			primary, err := r.ensurePrimary(ctx, frc)
			require.NoError(t, err)
			before := primary.DeepCopy()
			tt.mutate(frc)
			// A combined update must not clean up the old topology before discovering
			// that the primary cannot roll its Pods to the requested runtime.
			frc.Spec.MemberClusters = []rayv1.FederationMemberCluster{{Name: "replacement", Namespace: "workers"}}
			require.NoError(t, local.Update(ctx, frc))
			_, err = r.Reconcile(ctx, requestFor(frc))
			require.ErrorContains(t, err, "cannot change in place")
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			assert.True(t, meta.IsStatusConditionFalse(frc.Status.Conditions, "Ready"))
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(primary), primary))
			assert.Equal(t, before.Spec, primary.Spec)
			assert.Equal(t, before.ResourceVersion, primary.ResourceVersion)
			require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
			assert.EqualValues(t, "member-owner", mrc.UID)
			assert.True(t, mrc.DeletionTimestamp.IsZero())
			_, err = r.ensurePrimary(ctx, frc)
			require.ErrorContains(t, err, "cannot change in place")
		})
	}
}

func TestPrimaryUpgradeGuardAllowsScalingAndRepairsDrift(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), federationWithLocalWorkers()
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.NotEmpty(t, primary.Annotations[primaryRevisionAnnotation])
	primary.Spec.HeadGroupSpec.Template.Spec.Containers[0].Image = "drift-head"
	primary.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Image = "drift-worker"
	primary.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](3)
	primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{"selected-worker"}
	require.NoError(t, local.Update(ctx, primary))
	group := &frc.Spec.PrimaryCluster.WorkerGroups[0]
	group.ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	group.Replicas, group.MinReplicas, group.MaxReplicas, group.Suspend = ptr.To[int32](4), ptr.To[int32](1), ptr.To[int32](5), new(true)
	frc.Spec.MemberClusters[0].WorkerGroups[0].Template.Spec.Containers[0].Image = "member-template-change"
	require.NoError(t, r.ValidatePrimaryUpdate(ctx, frc))
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.Equal(t, frc.Spec.PrimaryCluster.HeadGroupSpec.Template, primary.Spec.HeadGroupSpec.Template)
	assert.Equal(t, group.Template, primary.Spec.WorkerGroupSpecs[0].Template)
	assert.EqualValues(t, 3, *primary.Spec.WorkerGroupSpecs[0].Replicas)
	assert.Equal(t, []string{"selected-worker"}, primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
	assert.True(t, *primary.Spec.WorkerGroupSpecs[0].Suspend)
	version := primary.ResourceVersion
	for range 2 {
		primary, err = r.ensurePrimary(ctx, frc)
		require.NoError(t, err)
		assert.Equal(t, version, primary.ResourceVersion)
	}
}

func TestLegacyPrimaryConfigurationCannotBeSilentlyReplaced(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), federationWithLocalWorkers()
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	delete(primary.Annotations, primaryRevisionAnnotation)
	require.NoError(t, local.Update(ctx, primary))
	changed := frc.DeepCopy()
	changed.Spec.PrimaryCluster.RayVersion = "2.57.0"
	require.ErrorContains(t, r.ValidatePrimaryUpdate(ctx, changed), "cannot change in place")
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.NotEmpty(t, primary.Annotations[primaryRevisionAnnotation])
}

func TestPrimaryManagedByDefaultsPreserveReplicaAuthority(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), federationWithLocalWorkers()
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	primary.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](5)
	primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{"selected-worker"}
	require.NoError(t, local.Update(ctx, primary))
	for _, manager := range []*string{new(rayv1.WorkerGroupManagedByRayCluster), nil} {
		frc.Spec.PrimaryCluster.WorkerGroups[0].ManagedBy = manager
		require.NoError(t, r.ValidatePrimaryUpdate(ctx, frc))
		primary, err = r.ensurePrimary(ctx, frc)
		require.NoError(t, err)
		assert.EqualValues(t, 5, *primary.Spec.WorkerGroupSpecs[0].Replicas)
		assert.Equal(t, []string{"selected-worker"}, primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
		assert.Nil(t, primary.Spec.ManagedBy, "group delegation must not skip reconciliation of the entire RayCluster")
	}
}
