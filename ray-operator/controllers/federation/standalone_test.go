package federation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func manualFederation() *rayv1.FederatedRayCluster {
	frc := testFederation()
	frc.Spec.MemberClusters[0].KubeconfigSecretRef = nil
	frc.Spec.MemberClusters[0].WorkerGroups = nil
	return frc
}

func manualMember(t *testing.T) *rayv1.RayCluster {
	t.Helper()
	mrc := testMember(t)
	mrc.Labels = nil
	return mrc
}

// Any credential access for a manual member fails the test, including cleanup.
type forbiddenMemberClients struct{ t *testing.T }

func (f forbiddenMemberClients) Get(context.Context, string, string) (client.Client, error) {
	f.t.Fatal("manual member attempted to access credentials or the remote API")
	return nil, errors.New("unexpected remote API access")
}

func TestOptionalCredentialValidation(t *testing.T) {
	require.NoError(t, ValidateFederation(manualFederation()))
	tests := []struct {
		name    string
		mutate  func(*rayv1.FederatedRayCluster)
		message string
	}{
		{"empty credential", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].KubeconfigSecretRef = &corev1.LocalObjectReference{}
		}, "nonempty"},
		{"manual worker groups", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].WorkerGroups = testFederation().Spec.MemberClusters[0].WorkerGroups
		}, "on its member RayCluster"},
		{"managed groups required", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].KubeconfigSecretRef = &corev1.LocalObjectReference{Name: "credential"}
		}, "requires a worker group"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frc := manualFederation()
			tt.mutate(frc)
			require.ErrorContains(t, ValidateFederation(frc), tt.message)
		})
	}
}

func TestManualFederationConvergesWithoutRemoteAPI(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), manualFederation()
	frc.Finalizers = []string{Finalizer}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc, &rayv1.RayCluster{}).WithObjects(frc).Build()
	r := &FederatedReconciler{
		Client: c, Reader: c, Scheme: scheme, Members: forbiddenMemberClients{t},
	}
	for range 5 {
		_, err := r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
		primary := &rayv1.RayCluster{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(frc), primary); err == nil {
			assert.Empty(t, primary.Spec.WorkerGroupSpecs, "manual members have no virtual PRC worker groups")
			assert.False(t, *primary.Spec.EnableInTreeAutoscaling)
			primary.Status.ObservedGeneration = primary.Generation
			condition(&primary.Status.Conditions, primary.Generation, string(rayv1.HeadPodReady), metav1.ConditionTrue, "Ready", "")
			require.NoError(t, c.Status().Update(ctx, primary))
		}
	}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	require.NotNil(t, frc.Status.HeadEndpoint)
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "HeadEndpointReady"))
	for _, kind := range []string{"Ready", "WorkersReady", "MemberControlPlaneReachable"} {
		assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, kind), kind)
	}
	require.Len(t, frc.Status.MemberClusterStatuses, 1)
	assert.Empty(t, frc.Status.MemberClusterStatuses[0].WorkerGroupStatuses)
	assert.Nil(t, frc.Status.MemberClusterStatuses[0].LastUpdateTime)
	require.Len(t, frc.Status.MemberClusterStatuses[0].Conditions, 1)
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.MemberClusterStatuses[0].Conditions, "ManuallyManaged"))
	assert.Empty(t, r.secretRequests(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: frc.Namespace}}))
	for range 2 {
		_, err := r.finalize(ctx, frc)
		require.NoError(t, err)
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	}
	assert.False(t, controllerutil.ContainsFinalizer(frc, Finalizer))
}

func TestManualFederationRequiresPrimaryReadiness(t *testing.T) {
	for _, tt := range []struct {
		name               string
		headReady          bool
		observedGeneration int64
		readyWorkers       int32
		wantReady          bool
	}{
		{"head not ready", false, 1, 2, false},
		{"primary observation stale", true, 0, 2, false},
		{"local workers not ready", true, 1, 1, false},
		{"primary ready", true, 1, 2, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, scheme, frc := context.Background(), testScheme(t), federationWithLocalWorkers()
			frc.Spec.MemberClusters = manualFederation().Spec.MemberClusters
			frc.Finalizers = []string{Finalizer}
			local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc, &rayv1.RayCluster{}).WithObjects(frc).Build()
			r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: forbiddenMemberClients{t}}
			// Persist inventory, then create the primary before observing its status.
			for range 2 {
				_, err := r.Reconcile(ctx, requestFor(frc))
				require.NoError(t, err)
			}
			primary := &rayv1.RayCluster{}
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), primary))
			primary.Generation = 1
			require.NoError(t, local.Update(ctx, primary))
			primary.Status.ObservedGeneration = tt.observedGeneration
			primary.Status.ReadyWorkerReplicas = tt.readyWorkers
			setBooleanCondition(&primary.Status.Conditions, primary.Generation, string(rayv1.HeadPodReady), tt.headReady, "HeadObserved")
			require.NoError(t, local.Status().Update(ctx, primary))
			_, err := r.Reconcile(ctx, requestFor(frc))
			require.NoError(t, err)
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			assert.Equal(t, tt.wantReady, meta.IsStatusConditionTrue(frc.Status.Conditions, "Ready"))
			assert.Equal(t, tt.wantReady, meta.IsStatusConditionTrue(frc.Status.Conditions, "WorkersReady"))
			assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "MemberControlPlaneReachable"))
			require.Len(t, frc.Status.MemberClusterStatuses, 1)
			require.Len(t, frc.Status.MemberClusterStatuses[0].Conditions, 1)
			assert.True(t, meta.IsStatusConditionTrue(frc.Status.MemberClusterStatuses[0].Conditions, "ManuallyManaged"))
		})
	}
}

func TestMemberManagementCannotChangeInPlace(t *testing.T) {
	for _, managed := range []bool{false, true} {
		frc := manualFederation()
		if managed {
			frc = testFederation()
		}
		r := &FederatedReconciler{Members: forbiddenMemberClients{t}}
		frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{{
			Name: frc.Spec.MemberClusters[0].Name, Namespace: frc.Spec.MemberClusters[0].Namespace,
			KubeconfigSecretRef: frc.Spec.MemberClusters[0].KubeconfigSecretRef.DeepCopy(),
		}}
		if managed {
			frc.Spec.MemberClusters[0].KubeconfigSecretRef = nil
		} else {
			frc.Spec.MemberClusters[0].KubeconfigSecretRef = &corev1.LocalObjectReference{Name: "credential"}
		}
		_, err := r.syncInventory(context.Background(), frc)
		require.ErrorContains(t, err, "cannot change in place")
		assert.Equal(t, managed, frc.Status.MemberClusterStatuses[0].KubeconfigSecretRef != nil)
	}
}

func TestManagedMemberConvergesAlongsideManualMember(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	manual := manualFederation().Spec.MemberClusters[0]
	manual.Name = "manual-member"
	// Put the manual entry first to exercise observation/inventory indexing.
	frc.Spec.MemberClusters = append([]rayv1.FederationMemberCluster{manual}, frc.Spec.MemberClusters...)
	frc.Finalizers = []string{Finalizer}
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc, &rayv1.RayCluster{}).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&rayv1.RayCluster{}).WithObjects(testMemberClusterIdentity()).Build()
	members := &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}
	r := &FederatedReconciler{
		Client: local, Reader: local, Scheme: scheme, Members: members,
	}
	for range 9 {
		_, err := r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
		primary := &rayv1.RayCluster{}
		if err := local.Get(ctx, client.ObjectKeyFromObject(frc), primary); err == nil {
			require.Len(t, primary.Spec.WorkerGroupSpecs, 1)
			primary.Status.ObservedGeneration = primary.Generation
			condition(&primary.Status.Conditions, primary.Generation, string(rayv1.HeadPodReady), metav1.ConditionTrue, "Ready", "")
			require.NoError(t, local.Status().Update(ctx, primary))
		}
		markMemberReady(t, remote, frc)
	}
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "Ready"))
	require.Len(t, frc.Status.MemberClusterStatuses, 2)
	require.Len(t, frc.Status.MemberClusterStatuses[0].Conditions, 1)
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.MemberClusterStatuses[0].Conditions, "ManuallyManaged"))
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.MemberClusterStatuses[1].Conditions, "WorkersReady"))
	assert.Equal(t, int32(2), frc.Status.MemberClusterStatuses[1].WorkerGroupStatuses[0].ReadyReplicas)
	manualStatus := frc.Status.MemberClusterStatuses[0].DeepCopy()
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"managed API unavailable", errors.New("member API unavailable")},
		{"managed API recovered", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			members.err = tt.err
			_, err := r.Reconcile(ctx, requestFor(frc))
			require.NoError(t, err)
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			for _, kind := range []string{"Ready", "WorkersReady", "MemberControlPlaneReachable"} {
				assert.Equal(t, tt.err == nil, meta.IsStatusConditionTrue(frc.Status.Conditions, kind), kind)
			}
			assert.Equal(t, *manualStatus, frc.Status.MemberClusterStatuses[0])
		})
	}
	// A reachable managed API with stale worker observations still blocks readiness.
	mrc := &rayv1.RayCluster{}
	require.NoError(t, remote.Get(ctx, client.ObjectKey{Namespace: "workers", Name: memberRayClusterName(frc.Name, "member-b")}, mrc))
	mrc.Status.LastUpdateTime = new(metav1.NewTime(time.Now().Add(-2 * staleAfter)))
	require.NoError(t, remote.Status().Update(ctx, mrc))
	_, err := r.Reconcile(ctx, requestFor(frc))
	require.NoError(t, err)
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	assert.True(t, meta.IsStatusConditionFalse(frc.Status.Conditions, "Ready"))
	assert.True(t, meta.IsStatusConditionFalse(frc.Status.Conditions, "WorkersReady"))
	assert.True(t, meta.IsStatusConditionTrue(frc.Status.Conditions, "MemberControlPlaneReachable"))
	assert.Equal(t, *manualStatus, frc.Status.MemberClusterStatuses[0])
}

func TestOrphanedMemberContinuesWithoutFederation(t *testing.T) {
	ctx, scheme, frc, mrc := context.Background(), testScheme(t), testFederation(), testMember(t)
	frc.Spec.MemberCleanupPolicy = "Orphan"
	worker := readyWorker(t, scheme, mrc, mrc.Spec.WorkerGroupSpecs[0])
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mrc, worker, testMemberClusterIdentity()).Build()
	r := &FederatedReconciler{Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": c}}}
	destination := boundTestMember(frc)
	destination.RayClusterUID = mrc.UID
	done, err := r.cleanupMember(ctx, frc, destination)
	require.NoError(t, err)
	assert.True(t, done)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
	assert.Empty(t, mrc.Labels[OwnerLabel])
	assert.Equal(t, "member-b", mrc.Labels[utils.FederationMemberLabel], "orphaning preserves node identity")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(worker), &corev1.Pod{}))
	done, err = r.cleanupMember(ctx, frc, destination)
	require.NoError(t, err, "a retry after a lost local status update must recognize the completed release")
	assert.True(t, done)
	assert.Equal(t, string(frc.UID), mrc.Labels[orphanedByLabel])
	// A different federation cannot use another owner's release receipt.
	frc.UID = "different-federation"
	_, err = r.cleanupMember(ctx, frc, destination)
	require.ErrorContains(t, err, "owner does not match")
	assert.False(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(mrc), &rayv1.RayCluster{})))
}

func TestManualStatusRemovesHealthObservations(t *testing.T) {
	transition := metav1.NewTime(time.Unix(1, 0))
	status := rayv1.FederationMemberStatus{
		ObservedGeneration:  1,
		LastUpdateTime:      new(metav1.Now()),
		WorkerGroupStatuses: []rayv1.FederationWorkerGroupStatus{{ReadyReplicas: 9}},
		Conditions: []metav1.Condition{
			{Type: "WorkersReady", Status: metav1.ConditionTrue},
			{Type: "MemberControlPlaneReachable", Status: metav1.ConditionUnknown, Reason: "ManuallyManaged"},
			{Type: "ManuallyManaged", Status: metav1.ConditionTrue, LastTransitionTime: transition, ObservedGeneration: 1},
		},
	}
	markManualMember(&status, 2)
	assert.Empty(t, status.WorkerGroupStatuses)
	assert.Nil(t, status.LastUpdateTime)
	assert.Equal(t, int64(2), status.ObservedGeneration)
	require.Len(t, status.Conditions, 1)
	assert.True(t, meta.IsStatusConditionTrue(status.Conditions, "ManuallyManaged"))
	assert.Equal(t, int64(2), status.Conditions[0].ObservedGeneration)
	assert.Equal(t, "ManuallyManaged", status.Conditions[0].Reason)
	assert.Equal(t, transition, status.Conditions[0].LastTransitionTime)
	previous := status.DeepCopy()
	markManualMember(&status, 2)
	assert.Equal(t, *previous, status, "an unchanged management mode must not churn status")
}
