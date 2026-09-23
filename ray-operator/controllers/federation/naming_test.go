package federation

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestMemberNamesRespectRayClusterLimits(t *testing.T) {
	assert.Equal(t, "test-ray-member-b", memberRayClusterName("test-ray", "member-b"))
	boundary := strings.Repeat("a", 40) + "-" + strings.Repeat("b", 12)
	assert.Equal(t, boundary, memberRayClusterName(strings.Repeat("a", 40), strings.Repeat("b", 12)))
	names := map[string]bool{}
	for _, federation := range []string{strings.Repeat("a", 53), strings.Repeat("a", 52) + "b"} {
		for _, member := range []string{strings.Repeat("b", 63), strings.Repeat("b", 62) + "c"} {
			name := memberRayClusterName(federation, member)
			require.NoError(t, utils.ValidateRayClusterMetadata(metav1.ObjectMeta{Name: name}))
			assert.Equal(t, name, memberRayClusterName(federation, member), "names survive controller restarts")
			assert.False(t, names[name], "truncation must retain member identity")
			names[name] = true
		}
	}
}

func TestMembersSharePrimaryClusterAndNamespace(t *testing.T) {
	for _, policy := range []string{"Delete", "Orphan"} {
		t.Run(policy, func(t *testing.T) {
			ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
			frc.Finalizers, frc.Spec.MemberCleanupPolicy = []string{Finalizer}, policy
			frc.Spec.MemberClusters[0].Namespace = frc.Namespace
			other := frc.Spec.MemberClusters[0].DeepCopy()
			other.Name, other.WorkerGroups[0].GroupName = "member-c", "other-workers"
			frc.Spec.MemberClusters = append(frc.Spec.MemberClusters, *other)
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc, &rayv1.RayCluster{}).WithObjects(frc, testMemberClusterIdentity()).Build()
			r := &FederatedReconciler{
				Client: c, Reader: c, Scheme: scheme,
				Members: &fixedMembers{clients: map[string]client.Client{"member-credential": c}},
			}
			for range 8 {
				_, err := r.Reconcile(ctx, requestFor(frc))
				require.NoError(t, err)
				primary := &rayv1.RayCluster{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(frc), primary); err == nil {
					primary.Status.ObservedGeneration = primary.Generation
					condition(&primary.Status.Conditions, primary.Generation, string(rayv1.HeadPodReady), metav1.ConditionTrue, "Ready", "")
					require.NoError(t, c.Status().Update(ctx, primary))
				}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), frc))
				markMemberReady(t, c, frc)
			}
			assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "Ready"))
			require.Len(t, frc.Status.MemberClusterStatuses, 2)
			clusters := &rayv1.RayClusterList{}
			require.NoError(t, c.List(ctx, clusters, client.InNamespace(frc.Namespace)))
			require.Len(t, clusters.Items, 3, "one primary and two independent members")
			for _, status := range frc.Status.MemberClusterStatuses {
				assert.EqualValues(t, testMemberClusterUID, status.ClusterUID)
				assert.Equal(t, "test-ray-"+status.Name, status.RayClusterName)
				require.Len(t, status.WorkerGroupStatuses, 1)
				assert.Equal(t, int32(2), status.WorkerGroupStatuses[0].ReadyReplicas)
			}
			first, second := &rayv1.RayCluster{}, &rayv1.RayCluster{}
			firstKey := client.ObjectKey{Namespace: frc.Namespace, Name: "test-ray-member-b"}
			secondKey := client.ObjectKey{Namespace: frc.Namespace, Name: "test-ray-member-c"}
			require.NoError(t, c.Get(ctx, firstKey, first))
			require.NoError(t, c.Get(ctx, secondKey, second))
			secondBefore := second.DeepCopy()
			primary := &rayv1.RayCluster{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), primary))
			for i := range primary.Spec.WorkerGroupSpecs {
				if primary.Spec.WorkerGroupSpecs[i].GroupName == frc.Spec.MemberClusters[0].WorkerGroups[0].GroupName {
					primary.Spec.WorkerGroupSpecs[i].Replicas = new(int32(3))
				}
			}
			require.NoError(t, c.Update(ctx, primary))
			for range 2 {
				_, err := r.Reconcile(ctx, requestFor(frc))
				require.NoError(t, err)
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), frc))
				markMemberReady(t, c, frc)
			}
			assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "Ready"))
			assert.Equal(t, int32(3), frc.Status.MemberClusterStatuses[0].WorkerGroupStatuses[0].ReadyReplicas)
			assert.Equal(t, int32(3), frc.Status.MemberClusterStatuses[0].WorkerGroupStatuses[0].DesiredReplicas)
			assert.EqualValues(t, 2, *frc.Spec.MemberClusters[0].WorkerGroups[0].Replicas, "FRC retains the initial value")
			assert.Equal(t, int32(2), frc.Status.MemberClusterStatuses[1].WorkerGroupStatuses[0].ReadyReplicas)
			frc.Spec.MemberClusters = frc.Spec.MemberClusters[1:]
			require.NoError(t, c.Update(ctx, frc))
			for range 4 {
				_, err := r.Reconcile(ctx, requestFor(frc))
				require.NoError(t, err)
			}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "Ready"))
			require.Len(t, frc.Status.MemberClusterStatuses, 1)
			assert.Equal(t, "member-c", frc.Status.MemberClusterStatuses[0].Name)
			require.NoError(t, c.Get(ctx, secondKey, second))
			assert.Equal(t, secondBefore.UID, second.UID)
			assert.Equal(t, secondBefore.Spec, second.Spec)
			assert.Equal(t, secondBefore.Labels, second.Labels)
			err := c.Get(ctx, firstKey, first)
			if policy == "Orphan" {
				require.NoError(t, err)
				assert.Empty(t, first.Labels[OwnerLabel])
				assert.Equal(t, "member-b", first.Labels[utils.FederationMemberLabel])
			} else {
				assert.True(t, apierrors.IsNotFound(err))
			}
		})
	}
}

func TestExistingMemberKeepsItsNameWhenAnotherMemberIsAdded(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Finalizers = []string{Finalizer}
	legacy := testMember(t)
	legacy.Name = frc.Name
	status := boundTestMember(frc)
	status.RayClusterName = ""
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{status}
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc, &rayv1.RayCluster{}).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacy, testMemberClusterIdentity()).Build()
	r := &FederatedReconciler{
		Client: local, Reader: local, Scheme: scheme,
		Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}},
	}
	_, err := r.Reconcile(ctx, requestFor(frc))
	require.NoError(t, err)
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	assert.Equal(t, legacy.Name, frc.Status.MemberClusterStatuses[0].RayClusterName)
	assert.True(t, apierrors.IsNotFound(local.Get(ctx, client.ObjectKeyFromObject(frc), &rayv1.RayCluster{})), "persist the name before writing children")
	other := frc.Spec.MemberClusters[0].DeepCopy()
	other.Name, other.WorkerGroups[0].GroupName = "member-c", "other-workers"
	frc.Spec.MemberClusters = append(frc.Spec.MemberClusters, *other)
	require.NoError(t, local.Update(ctx, frc))
	for range 3 {
		_, err = r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
	}
	clusters := &rayv1.RayClusterList{}
	require.NoError(t, remote.List(ctx, clusters))
	require.Len(t, clusters.Items, 2)
	actual := &rayv1.RayCluster{}
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(legacy), actual))
	assert.Equal(t, legacy.UID, actual.UID)
	require.NoError(t, remote.Get(ctx, client.ObjectKey{Namespace: legacy.Namespace, Name: "test-ray-member-c"}, actual))
	assert.Equal(t, "member-c", actual.Labels[utils.FederationMemberLabel])
	// Cleanup uses the persisted legacy name, without touching the newer member.
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	frc.Spec.MemberClusters = frc.Spec.MemberClusters[1:]
	require.NoError(t, local.Update(ctx, frc))
	for range 4 {
		_, err = r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
	}
	assert.True(t, apierrors.IsNotFound(remote.Get(ctx, client.ObjectKeyFromObject(legacy), &rayv1.RayCluster{})))
	require.NoError(t, remote.Get(ctx, client.ObjectKey{Namespace: legacy.Namespace, Name: "test-ray-member-c"}, &rayv1.RayCluster{}))
}

func TestLegacyMemberNameFinalization(t *testing.T) {
	for _, policy := range []string{"Delete", "Orphan", "OrphanRetry"} {
		t.Run(policy, func(t *testing.T) {
			ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
			frc.Finalizers, frc.Spec.MemberCleanupPolicy = []string{Finalizer}, policy
			legacy := testMember(t)
			legacy.Name = frc.Name
			if policy == "OrphanRetry" {
				frc.Spec.MemberCleanupPolicy = "Orphan"
				delete(legacy.Labels, OwnerLabel)
				legacy.Labels[orphanedByLabel] = string(frc.UID)
			}
			status := boundTestMember(frc)
			status.RayClusterName = ""
			frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{status}
			local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
			remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacy, testMemberClusterIdentity()).Build()
			r := &FederatedReconciler{
				Client: local, Reader: local, Scheme: scheme,
				Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}},
			}
			for range 5 {
				_, err := r.finalize(ctx, frc)
				require.NoError(t, err)
				require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			}
			assert.False(t, controllerutil.ContainsFinalizer(frc, Finalizer))
			actual := &rayv1.RayCluster{}
			err := remote.Get(ctx, client.ObjectKeyFromObject(legacy), actual)
			if policy == "Delete" {
				assert.True(t, apierrors.IsNotFound(err))
			} else {
				require.NoError(t, err)
				assert.Equal(t, legacy.UID, actual.UID)
				assert.Empty(t, actual.Labels[OwnerLabel])
			}
		})
	}
}

func TestLegacyNameBindingUsesRotatedCredential(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	status := boundTestMember(frc)
	status.RayClusterName = ""
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{status}
	frc.Spec.MemberClusters[0].KubeconfigSecretRef.Name = "replacement"
	legacy := testMember(t)
	legacy.Name = frc.Name
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacy, testMemberClusterIdentity()).Build()
	r := &FederatedReconciler{Members: &fixedMembers{clients: map[string]client.Client{"replacement": remote}}}
	ready, err := r.syncInventory(ctx, frc)
	require.NoError(t, err, "the previous credential may already be expired during upgrade")
	assert.False(t, ready)
	assert.Equal(t, frc.Name, frc.Status.MemberClusterStatuses[0].RayClusterName)
	assert.Equal(t, "replacement", frc.Status.MemberClusterStatuses[0].KubeconfigSecretRef.Name)
	assert.EqualValues(t, testMemberClusterUID, frc.Status.MemberClusterStatuses[0].ClusterUID)
}
