package federation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))
	require.NoError(t, rayv1.AddToScheme(s))
	return s
}

const testMemberClusterUID = "member-cluster-uid"

func testMemberClusterIdentity() *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: metav1.NamespaceSystem, UID: testMemberClusterUID}}
}

func testFederation() *rayv1.FederatedRayCluster {
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Image: "rayproject/ray:2.56.0"}}}}
	group := rayv1.WorkerGroupSpec{GroupName: "remote-workers", Template: *template.DeepCopy(), Replicas: ptr.To[int32](2), MinReplicas: ptr.To[int32](0), MaxReplicas: ptr.To[int32](10), NumOfHosts: 1}
	normalizeGroup(&group)
	frc := &rayv1.FederatedRayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ray", Namespace: "default", UID: "frc-owner", Generation: 1},
		Spec: rayv1.FederatedRayClusterSpec{
			PrimaryCluster: rayv1.FederationPrimaryCluster{HeadGroupSpec: &rayv1.HeadGroupSpec{Template: template}, RayVersion: "2.56.0"},
			MemberClusters: []rayv1.FederationMemberCluster{{Name: "member-b", Namespace: "workers", KubeconfigSecretRef: &corev1.LocalObjectReference{Name: "member-credential"}, WorkerGroups: []rayv1.WorkerGroupSpec{group}}},
			Networking:     rayv1.FederationNetworking{HeadEndpoint: rayv1.FederationHeadEndpoint{Mode: "UserProvided", Address: "head.private", GCSPort: 6379}},
		},
	}
	normalizeNetworking(&frc.Spec.Networking)
	return frc
}

func testMember(t *testing.T) *rayv1.RayCluster {
	t.Helper()
	frc := testFederation()
	group := *frc.Spec.MemberClusters[0].WorkerGroups[0].DeepCopy()
	group.ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
	primary := &rayv1.RayCluster{Spec: rayv1.RayClusterSpec{RayVersion: "2.56.0", WorkerGroupSpecs: []rayv1.WorkerGroupSpec{group}}}
	mrc := desiredMember(frc, primary, frc.Spec.MemberClusters[0])
	mrc.UID, mrc.Generation = "member-owner", 1
	return mrc
}

type fixedMembers struct {
	clients map[string]client.Client
	err     error
}

func (m *fixedMembers) Get(_ context.Context, _, name string) (client.Client, error) {
	if m.err != nil {
		return nil, m.err
	}
	if c, ok := m.clients[name]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("unknown credential")
}

func TestFederationValidation(t *testing.T) {
	require.NoError(t, ValidateFederation(testFederation()))
	local := federationWithLocalWorkers()
	local.Spec.PrimaryCluster.WorkerGroups[0].ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	local.Spec.MemberClusters[0].WorkerGroups[0].ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	require.NoError(t, ValidateFederation(local))
	tests := []struct {
		name    string
		mutate  func(*rayv1.FederatedRayCluster)
		message string
	}{
		{"duplicate groups", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.WorkerGroups = f.Spec.MemberClusters[0].WorkerGroups
		}, "unique"},
		{"duplicate members", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters = append(f.Spec.MemberClusters, f.Spec.MemberClusters[0])
		}, "duplicate member"},
		{"public endpoint", func(f *rayv1.FederatedRayCluster) { f.Spec.Networking.HeadEndpoint.Address = "8.8.8.8" }, "private"},
		{"injected endpoint", func(f *rayv1.FederatedRayCluster) {
			f.Spec.Networking.HeadEndpoint.Address = "head;touch /tmp/injected"
		}, "without a scheme"},
		{"replica bounds", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].WorkerGroups[0].MinReplicas = ptr.To[int32](20)
		}, "bounds"},
		{"nested managedBy", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].WorkerGroups[0].ManagedBy = new("")
		}, "managedBy"},
		{"input federation manager", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].WorkerGroups[0].ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
		}, "managedBy is assigned by the federation"},
		{"multi host", func(f *rayv1.FederatedRayCluster) { f.Spec.MemberClusters[0].WorkerGroups[0].NumOfHosts = 2 }, "single-host"},
		{"reserved group", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.WorkerGroups = f.Spec.MemberClusters[0].WorkerGroups
			f.Spec.PrimaryCluster.WorkerGroups[0].GroupName = utils.RayNodeHeadGroupLabelValue
		}, "reserved"},
		{"qualified deletion ID", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].WorkerGroups[0].ScaleStrategy.WorkersToDelete = []string{"member-b/pod-name"}
		}, "Pod names"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frc := testFederation()
			test.mutate(frc)
			require.ErrorContains(t, ValidateFederation(frc), test.message)
		})
	}
}

func TestPrimaryOwnershipDriftAndReplicaAuthority(t *testing.T) {
	ctx, scheme := context.Background(), testScheme(t)
	frc := testFederation()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: c, Reader: c, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.True(t, metav1.IsControlledBy(primary, frc))
	assert.True(t, primary.Spec.WorkerGroupSpecs[0].IsExternallyManaged())
	assert.False(t, *primary.Spec.EnableInTreeAutoscaling)
	assert.Zero(t, utils.CalculateDesiredReplicas(primary))
	assert.EqualValues(t, 2, *primary.Spec.WorkerGroupSpecs[0].Replicas, "new groups use FRC initial replicas")
	primary.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](5)
	primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{"selected-worker"}
	primary.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Image = "drift"
	require.NoError(t, c.Update(ctx, primary))
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.Equal(t, int32(5), *primary.Spec.WorkerGroupSpecs[0].Replicas)
	assert.Equal(t, []string{"selected-worker"}, primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
	assert.NotEqual(t, "drift", primary.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Image)
	version := primary.ResourceVersion
	for range 3 {
		primary, err = r.ensurePrimary(ctx, frc)
		require.NoError(t, err)
		assert.Equal(t, version, primary.ResourceVersion)
	}
	frc.Spec.MemberClusters[0].WorkerGroups[0].Replicas = new(int32(8))
	frc.Spec.MemberClusters[0].WorkerGroups[0].ScaleStrategy.WorkersToDelete = []string{"outdated-selection"}
	frc.Spec.MemberClusters[0].WorkerGroups[0].MaxReplicas = new(int32(9))
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.EqualValues(t, 5, *primary.Spec.WorkerGroupSpecs[0].Replicas, "PRC owns runtime targets")
	assert.Equal(t, []string{"selected-worker"}, primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
	assert.EqualValues(t, 9, *primary.Spec.WorkerGroupSpecs[0].MaxReplicas, "FRC still owns policy")
	assert.NotEqual(t, version, primary.ResourceVersion)
	member := desiredMember(frc, primary, frc.Spec.MemberClusters[0])
	assert.EqualValues(t, 5, *member.Spec.WorkerGroupSpecs[0].Replicas)
	assert.Equal(t, []string{"selected-worker"}, member.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
	primary.OwnerReferences = nil
	require.NoError(t, c.Update(ctx, primary))
	_, err = r.ensurePrimary(ctx, frc)
	require.ErrorContains(t, err, "another owner")
}

func TestFederationPreservesNativeNetworkingParameters(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), federationWithLocalWorkers()
	frc.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = map[string]string{"node-manager-port": "0"}
	frc.Spec.PrimaryCluster.WorkerGroups[0].RayStartParams = map[string]string{"min-worker-port": "10000", "max-worker-port": "20000"}
	group := &frc.Spec.MemberClusters[0].WorkerGroups[0]
	group.RayStartParams = map[string]string{"worker-port-list": "19090,20000"}
	group.Template.Spec.HostNetwork = true
	group.Template.Annotations = map[string]string{utils.RayOverwriteContainerCmdAnnotationKey: "true"}
	group.Template.Spec.Containers[0].Command = []string{"bash", "-c", "exec $KUBERAY_GEN_RAY_START_CMD"}
	require.NoError(t, ValidateFederation(frc))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: c, Reader: c, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.Equal(t, frc.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams, primary.Spec.HeadGroupSpec.RayStartParams)
	assert.Equal(t, frc.Spec.PrimaryCluster.WorkerGroups[0].RayStartParams, primary.Spec.WorkerGroupSpecs[0].RayStartParams)
	mrc := desiredMember(frc, primary, frc.Spec.MemberClusters[0])
	assert.Equal(t, map[string]string{"worker-port-list": "19090,20000", "address": "head.private:6379"}, mrc.Spec.WorkerGroupSpecs[0].RayStartParams)
	assert.Equal(t, group.Template, mrc.Spec.WorkerGroupSpecs[0].Template)
}

func TestMemberSyncIsIdempotentAndConflictSafe(t *testing.T) {
	ctx, scheme := context.Background(), testScheme(t)
	frc, desired := testFederation(), testMember(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&rayv1.RayCluster{}).Build()
	mrc, err := syncMember(ctx, c, frc, desired)
	require.NoError(t, err)
	version := mrc.ResourceVersion
	mrc, err = syncMember(ctx, c, frc, desired)
	require.NoError(t, err)
	assert.Equal(t, version, mrc.ResourceVersion)
	assert.Empty(t, mrc.OwnerReferences, "cross-cluster ownerReferences are invalid")
	assert.False(t, mrc.Spec.WorkerGroupSpecs[0].IsExternallyManaged())
	desired.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](3)
	failing := interceptor.NewClient(c, interceptor.Funcs{Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
		return apierrors.NewConflict(rayv1.Resource("rayclusters"), mrc.Name, errors.New("concurrent write"))
	}})
	_, err = syncMember(ctx, failing, frc, desired)
	require.True(t, apierrors.IsConflict(err))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
	assert.Equal(t, int32(2), *mrc.Spec.WorkerGroupSpecs[0].Replicas)
	desired.Labels[utils.FederationMemberLabel] = "another-member"
	_, err = syncMember(ctx, c, frc, desired)
	require.ErrorContains(t, err, "already bound")
	desired.Labels[utils.FederationMemberLabel] = mrc.Labels[utils.FederationMemberLabel]
	mrc.Labels[OwnerLabel] = "another-federation"
	require.NoError(t, c.Update(ctx, mrc))
	_, err = syncMember(ctx, c, frc, desired)
	require.ErrorContains(t, err, "different federation owner")
}

func TestMemberManagedByUsesFederationGroupNames(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	local := *frc.Spec.MemberClusters[0].WorkerGroups[0].DeepCopy()
	local.GroupName = "local-workers"
	frc.Spec.PrimaryCluster.WorkerGroups = []rayv1.WorkerGroupSpec{local}
	other := *frc.Spec.MemberClusters[0].DeepCopy()
	other.Name, other.Namespace = "member-c", "other-workers"
	other.WorkerGroups[0].GroupName = "other-group"
	frc.Spec.MemberClusters = append(frc.Spec.MemberClusters, other)
	require.NoError(t, ValidateFederation(frc))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: c, Reader: c, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	require.Len(t, primary.Spec.WorkerGroupSpecs, 3)
	require.Nil(t, primary.Spec.WorkerGroupSpecs[0].ManagedBy)
	for i := 1; i < 3; i++ {
		group := &primary.Spec.WorkerGroupSpecs[i]
		assert.Equal(t, new(rayv1.WorkerGroupManagedByFederatedRayCluster), group.ManagedBy)
		group.Replicas = new(int32(i + 3))
		group.ScaleStrategy.WorkersToDelete = []string{group.GroupName + "-selected"}
	}
	// An externally managed group absent from FRC topology must reach no member.
	unassigned := *primary.Spec.WorkerGroupSpecs[1].DeepCopy()
	unassigned.GroupName = "unassigned"
	primary.Spec.WorkerGroupSpecs = append(primary.Spec.WorkerGroupSpecs, unassigned)
	before := primary.DeepCopy()
	for i, member := range frc.Spec.MemberClusters {
		mrc := desiredMember(frc, primary, member)
		require.Len(t, mrc.Spec.WorkerGroupSpecs, 1)
		group := mrc.Spec.WorkerGroupSpecs[0]
		assert.Equal(t, member.WorkerGroups[0].GroupName, group.GroupName)
		assert.Equal(t, int32(i+4), *group.Replicas)
		assert.Equal(t, []string{group.GroupName + "-selected"}, group.ScaleStrategy.WorkersToDelete)
		assert.Nil(t, group.ManagedBy)
		assert.Equal(t, "head.private:6379", group.RayStartParams["address"])
		assert.Equal(t, member.Name, mrc.Labels[utils.FederationMemberLabel])
		assert.Equal(t, member.Namespace, mrc.Namespace)
	}
	assert.Equal(t, before, primary, "member rendering must not mutate primary managedBy or parameters")
	// A membership change is resolved from the FRC without encoding a destination
	// on the primary RayCluster. The group keeps its own replica intent.
	frc.Spec.MemberClusters[0].WorkerGroups, frc.Spec.MemberClusters[1].WorkerGroups = frc.Spec.MemberClusters[1].WorkerGroups, frc.Spec.MemberClusters[0].WorkerGroups
	moved := desiredMember(frc, primary, frc.Spec.MemberClusters[0])
	require.Len(t, moved.Spec.WorkerGroupSpecs, 1)
	assert.Equal(t, "other-group", moved.Spec.WorkerGroupSpecs[0].GroupName)
	assert.Equal(t, int32(5), *moved.Spec.WorkerGroupSpecs[0].Replicas)
}

func TestPrimaryRetainsRemovedLocalGroupUntilOwnedPodsAreDrained(t *testing.T) {
	ctx, scheme := context.Background(), testScheme(t)
	frc := testFederation()
	localGroup := *frc.Spec.MemberClusters[0].WorkerGroups[0].DeepCopy()
	localGroup.GroupName = "local-workers"
	frc.Spec.PrimaryCluster.WorkerGroups = []rayv1.WorkerGroupSpec{localGroup}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: c, Reader: c, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	primary.UID = "primary-owner"
	require.NoError(t, c.Update(ctx, primary))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "local-worker", Namespace: frc.Namespace,
		Labels: map[string]string{utils.RayClusterLabelKey: primary.Name, utils.RayNodeGroupLabelKey: localGroup.GroupName, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode)},
	}}
	require.NoError(t, ctrl.SetControllerReference(primary, pod, scheme))
	require.NoError(t, c.Create(ctx, pod))
	frc.Spec.PrimaryCluster.WorkerGroups = nil
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	require.Len(t, primary.Spec.WorkerGroupSpecs, 2)
	retired := primary.Spec.WorkerGroupSpecs[1]
	assert.Equal(t, localGroup.GroupName, retired.GroupName)
	assert.True(t, *retired.Suspend)
	assert.Zero(t, *retired.Replicas)
	assert.Zero(t, *retired.MaxReplicas)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}), "federation does not delete worker Pods")
	require.NoError(t, c.Delete(ctx, pod))
	foreign := pod.DeepCopy()
	foreign.Name, foreign.ResourceVersion = "foreign-worker", ""
	foreign.OwnerReferences[0].UID = "another-owner"
	require.NoError(t, c.Create(ctx, foreign))
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	require.Len(t, primary.Spec.WorkerGroupSpecs, 1)
	assert.True(t, primary.Spec.WorkerGroupSpecs[0].IsExternallyManaged())
}

func readyWorker(t *testing.T, scheme *runtime.Scheme, cluster *rayv1.RayCluster, group rayv1.WorkerGroupSpec) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "worker-" + rand.String(8), Namespace: cluster.Namespace,
		Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: "worker", utils.RayNodeGroupLabelKey: group.GroupName, utils.WorkerTemplateHashLabel: utils.WorkerTemplateHash(cluster, group)},
	}, Spec: *group.Template.Spec.DeepCopy()}
	pod.UID = types.UID(pod.Name)
	require.NoError(t, ctrl.SetControllerReference(cluster, pod, scheme))
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	return pod
}

func TestInventoryAndFinalization(t *testing.T) {
	for _, policy := range []string{"Delete", "Orphan"} {
		t.Run(policy, func(t *testing.T) {
			ctx, scheme := context.Background(), testScheme(t)
			frc, mrc := testFederation(), testMember(t)
			frc.Spec.MemberCleanupPolicy = policy
			controllerutil.AddFinalizer(frc, Finalizer)
			local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
			remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mrc, testMemberClusterIdentity()).Build()
			members := &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}
			r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: members}
			ready, err := r.syncInventory(ctx, frc)
			require.NoError(t, err)
			assert.False(t, ready, "first reconcile must persist the cleanup inventory")
			require.Len(t, frc.Status.MemberClusterStatuses, 1)
			require.NoError(t, local.Status().Update(ctx, frc))
			members.err = errors.New("member API offline")
			_, err = r.finalize(ctx, frc)
			require.NoError(t, err)
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			assert.True(t, controllerutil.ContainsFinalizer(frc, Finalizer))
			assert.Equal(t, "DeletionBlocked", meta.FindStatusCondition(frc.Status.Conditions, "Ready").Reason)
			members.err = nil
			for range 4 {
				_, err = r.finalize(ctx, frc)
				require.NoError(t, err)
				require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			}
			assert.False(t, controllerutil.ContainsFinalizer(frc, Finalizer))
			err = remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc)
			if policy == "Orphan" {
				require.NoError(t, err)
				assert.Empty(t, mrc.Labels[OwnerLabel])
			} else {
				assert.True(t, apierrors.IsNotFound(err))
			}
		})
	}
}

func TestMemberFreshnessDoesNotInventZeroCapacity(t *testing.T) {
	ctx, scheme, mrc := context.Background(), testScheme(t), testMember(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	mrc.Status.ObservedGeneration = mrc.Generation
	mrc.Status.LastUpdateTime = new(metav1.Now())
	require.True(t, freshMember(mrc))
	mrc.Generation++
	assert.False(t, freshMember(mrc))
	status := &rayv1.FederationMemberStatus{WorkerGroupStatuses: []rayv1.FederationWorkerGroupStatus{{ObservedReplicas: 7}}}
	require.NoError(t, observeMemberWorkers(ctx, c, mrc, status, 2))
	assert.Equal(t, int32(7), status.WorkerGroupStatuses[0].ObservedReplicas)
	assert.Equal(t, metav1.ConditionUnknown, meta.FindStatusCondition(status.Conditions, "WorkersReady").Status)
	mrc.Status.ObservedGeneration = mrc.Generation
	mrc.Status.LastUpdateTime = new(metav1.NewTime(time.Now().Add(-2 * staleAfter)))
	assert.False(t, freshMember(mrc))
}

func TestMemberCredentialBoundaryAndRotation(t *testing.T) {
	valid := "apiVersion: v1\nkind: Config\ncurrent-context: member\ncontexts:\n- name: member\n  context: {cluster: member, user: member}\nclusters:\n- name: member\n  cluster: {server: https://127.0.0.1:6443}\nusers:\n- name: member\n  user: {token: test-token}\n"
	_, err := memberRESTConfig([]byte(valid))
	require.NoError(t, err)
	for _, change := range []string{"exec: {command: touch}", "tokenFile: /etc/secret", "client-key: /etc/key", "auth-provider: {name: custom}", "as: administrator"} {
		_, err := memberRESTConfig([]byte(strings.Replace(valid, "token: test-token", change, 1)))
		require.Error(t, err)
	}
	_, err = memberRESTConfig([]byte(strings.Replace(valid, "https://127.0.0.1:6443", "http://127.0.0.1:6443", 1)))
	require.Error(t, err)
	ctx, scheme := context.Background(), testScheme(t)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: "approved", UID: "credential-uid"}, Data: map[string][]byte{"kubeconfig": []byte(valid)}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	provider := &MemberClients{Reader: c, Scheme: scheme}
	_, err = provider.Get(ctx, secret.Namespace, secret.Name)
	require.ErrorContains(t, err, "approval")
	secret.Labels = map[string]string{CredentialLabel: "true"}
	require.NoError(t, c.Update(ctx, secret))
	first, err := provider.Get(ctx, secret.Namespace, secret.Name)
	require.NoError(t, err)
	again, err := provider.Get(ctx, secret.Namespace, secret.Name)
	require.NoError(t, err)
	assert.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(again).Pointer())
	secret.Data["kubeconfig"] = []byte(strings.Replace(valid, "test-token", "rotated-token", 1))
	require.NoError(t, c.Update(ctx, secret))
	rotated, err := provider.Get(ctx, secret.Namespace, secret.Name)
	require.NoError(t, err)
	assert.NotEqual(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(rotated).Pointer())
	_, err = provider.Get(ctx, "unapproved", secret.Name)
	require.Error(t, err)
}

func requestFor(object client.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(object)}
}
