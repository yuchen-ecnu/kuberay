package federation

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestFullFederationAndMemberReconciliation(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Finalizers = []string{Finalizer}
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc, &rayv1.RayCluster{}).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&rayv1.RayCluster{}).WithObjects(testMemberClusterIdentity()).Build()
	members := &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}
	r := &FederatedReconciler{
		Client: local, Reader: local, Scheme: scheme, Members: members,
	}
	for range 6 {
		_, err := r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
		primary := &rayv1.RayCluster{}
		if err := local.Get(ctx, client.ObjectKeyFromObject(frc), primary); err == nil {
			primary.Status.ObservedGeneration = primary.Generation
			condition(&primary.Status.Conditions, primary.Generation, string(rayv1.HeadPodReady), metav1.ConditionTrue, "Ready", "")
			require.NoError(t, local.Status().Update(ctx, primary))
		}
		markMemberReady(t, remote, frc)
	}
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "HeadEndpointReady"))
	require.NotNil(t, frc.Status.HeadEndpoint)
	pods := &corev1.PodList{}
	require.NoError(t, remote.List(ctx, pods))
	assert.Len(t, pods.Items, 2)
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "Ready"))
	// API failure retains observed capacity and reports current worker readiness as Unknown.
	frc.Status.MemberClusterStatuses[0].WorkerGroupStatuses = []rayv1.FederationWorkerGroupStatus{{GroupName: "remote-workers", ObservedReplicas: 2}}
	require.NoError(t, local.Status().Update(ctx, frc))
	members.err = io.ErrUnexpectedEOF
	_, err := r.Reconcile(ctx, requestFor(frc))
	require.NoError(t, err)
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	status := frc.Status.MemberClusterStatuses[0]
	assert.Equal(t, int32(2), status.WorkerGroupStatuses[0].ObservedReplicas)
	assert.Equal(t, metav1.ConditionUnknown, meta.FindStatusCondition(status.Conditions, "WorkersReady").Status)
	assert.False(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "Ready"))
}

func TestFederationMembersConvergeIndependently(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	other := frc.Spec.MemberClusters[0].DeepCopy()
	other.Name, other.Namespace = "healthy", "healthy-workers"
	other.KubeconfigSecretRef.Name = "healthy-credential"
	other.WorkerGroups[0].GroupName = "healthy-workers"
	frc.Spec.MemberClusters = append(frc.Spec.MemberClusters, *other)
	frc.Finalizers = []string{Finalizer}
	for _, member := range frc.Spec.MemberClusters {
		frc.Status.MemberClusterStatuses = append(frc.Status.MemberClusterStatuses, rayv1.FederationMemberStatus{
			Name: member.Name, Namespace: member.Namespace, RayClusterName: memberRayClusterName(frc.Name, member.Name), ClusterUID: testMemberClusterUID, KubeconfigSecretRef: member.KubeconfigSecretRef.DeepCopy(),
			Conditions: []metav1.Condition{{Type: "MemberDataPlaneReady", Status: metav1.ConditionFalse, Reason: "NetworkPreflight"}},
		})
	}
	condition(&frc.Status.Conditions, frc.Generation, "MemberDataPlaneReady", metav1.ConditionFalse, "NetworkPreflight", "")
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity()).Build()
	// The first member is unreachable and the primary head is not ready. Neither
	// prevents desired-state propagation to the healthy member.
	r := &FederatedReconciler{
		Client: local, Reader: local, Scheme: scheme,
		Members: &fixedMembers{clients: map[string]client.Client{"healthy-credential": remote}},
	}
	_, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	for _, replicas := range []int32{2, 3} {
		primary := &rayv1.RayCluster{}
		require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), primary))
		for i := range primary.Spec.WorkerGroupSpecs {
			if primary.Spec.WorkerGroupSpecs[i].GroupName == other.WorkerGroups[0].GroupName {
				primary.Spec.WorkerGroupSpecs[i].Replicas = &replicas
			}
		}
		require.NoError(t, local.Update(ctx, primary))
		_, err := r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
		mrc := &rayv1.RayCluster{}
		require.NoError(t, remote.Get(ctx, client.ObjectKey{Namespace: other.Namespace, Name: memberRayClusterName(frc.Name, other.Name)}, mrc))
		assert.Equal(t, replicas, *mrc.Spec.WorkerGroupSpecs[0].Replicas)
		require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
		assert.EqualValues(t, 2, *frc.Spec.MemberClusters[1].WorkerGroups[0].Replicas, "FRC retains the initial value")
		assert.True(t, meta.IsStatusConditionFalse(frc.Status.Conditions, "Ready"))
		assert.True(t, meta.IsStatusConditionFalse(frc.Status.MemberClusterStatuses[0].Conditions, "MemberControlPlaneReachable"))
		assert.True(t, meta.IsStatusConditionTrue(frc.Status.MemberClusterStatuses[1].Conditions, "MemberControlPlaneReachable"))
		assert.Nil(t, meta.FindStatusCondition(frc.Status.Conditions, "MemberDataPlaneReady"))
		for _, status := range frc.Status.MemberClusterStatuses {
			assert.Nil(t, meta.FindStatusCondition(status.Conditions, "MemberDataPlaneReady"))
		}
	}
	for _, c := range []client.Client{local, remote} {
		pods, configs := &corev1.PodList{}, &corev1.ConfigMapList{}
		require.NoError(t, c.List(ctx, pods))
		require.NoError(t, c.List(ctx, configs))
		assert.Empty(t, pods.Items, "only local RayCluster controllers create Pods")
		assert.Empty(t, configs.Items)
	}
}

// Simulate only the member-controller contract; its Pod lifecycle is tested in package ray.
func markMemberReady(t *testing.T, remote client.Client, frc *rayv1.FederatedRayCluster) {
	t.Helper()
	ctx := context.Background()
	for _, member := range frc.Spec.MemberClusters {
		if member.KubeconfigSecretRef == nil {
			continue
		}
		name := memberRayClusterName(frc.Name, member.Name)
		for _, status := range frc.Status.MemberClusterStatuses {
			if status.Name == member.Name && status.RayClusterName != "" {
				name = status.RayClusterName
			}
		}
		mrc := &rayv1.RayCluster{}
		err := remote.Get(ctx, client.ObjectKey{Namespace: member.Namespace, Name: name}, mrc)
		if apierrors.IsNotFound(err) {
			continue
		}
		require.NoError(t, err)
		markRayClusterReady(t, remote, mrc)
	}
}

func markRayClusterReady(t *testing.T, remote client.Client, mrc *rayv1.RayCluster) {
	t.Helper()
	ctx := context.Background()
	if mrc.UID == "" {
		mrc.UID = types.UID(mrc.Name)
		require.NoError(t, remote.Update(ctx, mrc))
		return
	}
	for _, group := range mrc.Spec.WorkerGroupSpecs {
		pods := &corev1.PodList{}
		require.NoError(t, remote.List(ctx, pods, client.InNamespace(mrc.Namespace), client.MatchingLabels{utils.RayNodeGroupLabelKey: group.GroupName, utils.RayClusterLabelKey: mrc.Name}))
		for count := len(pods.Items); count < int(utils.GetWorkerGroupDesiredReplicas(group)); count++ {
			require.NoError(t, remote.Create(ctx, readyWorker(t, testScheme(t), mrc, group)))
		}
	}
	mrc.Status.ObservedGeneration = mrc.Generation
	mrc.Status.LastUpdateTime = new(metav1.Now())
	condition(&mrc.Status.Conditions, mrc.Generation, utils.WorkersReady, metav1.ConditionTrue, "Ready", "")
	require.NoError(t, remote.Status().Update(ctx, mrc))
}
