package federation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestMemberCreateResponseLossDoesNotDuplicateChild(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Finalizers = []string{Finalizer}
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
	creates := 0
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity()).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, object client.Object, options ...client.CreateOption) error {
			creates++
			object.SetUID("created-before-response-loss")
			if err := c.Create(ctx, object, options...); err != nil {
				return err
			}
			return errors.New("response lost after successful create")
		},
	}).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
	for range 4 {
		// Reconstruct the reconciler each time to rule out in-memory receipts.
		r = &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: r.Members}
		_, err := r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
	}
	require.Equal(t, 1, creates)
	children := &rayv1.RayClusterList{}
	require.NoError(t, remote.List(ctx, children))
	require.Len(t, children.Items, 1)
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	require.EqualValues(t, "created-before-response-loss", frc.Status.MemberClusterStatuses[0].RayClusterUID)
}

func TestMemberReleaseResponseLossIsRetryable(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Spec.MemberCleanupPolicy, frc.Finalizers = "Orphan", []string{Finalizer}
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
	patches := 0
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), testMember(t)).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
			patches++
			if err := c.Patch(ctx, object, patch, options...); err != nil {
				return err
			}
			return errors.New("response lost after releasing ownership")
		},
	}).Build()
	for range 4 {
		r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
		_, err := r.finalize(ctx, frc)
		require.NoError(t, err)
		require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	}
	require.Equal(t, 1, patches)
	require.False(t, controllerutil.ContainsFinalizer(frc, Finalizer))
	child := testMember(t)
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(child), child))
	require.Empty(t, child.Labels[OwnerLabel])
	require.Equal(t, string(frc.UID), child.Labels[orphanedByLabel])
}

func TestNewUnreachableMemberDoesNotBlockHealthyPropagation(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Finalizers = []string{Finalizer}
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
	mrc := testMember(t)
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc, &rayv1.RayCluster{}).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	primary.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](3)
	require.NoError(t, local.Update(ctx, primary))
	unknown := frc.Spec.MemberClusters[0].DeepCopy()
	unknown.Name, unknown.WorkerGroups[0].GroupName, unknown.KubeconfigSecretRef.Name = "offline-new", "offline-new-group", "offline-credential"
	frc.Spec.MemberClusters = append(frc.Spec.MemberClusters, *unknown)
	require.NoError(t, local.Update(ctx, frc))
	_, err = r.Reconcile(ctx, requestFor(frc))
	require.ErrorContains(t, err, "unknown credential")
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
	require.Equal(t, int32(3), *mrc.Spec.WorkerGroupSpecs[0].Replicas)
	// Reverting the new member immediately permits the pending healthy target.
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	frc.Spec.MemberClusters = frc.Spec.MemberClusters[:1]
	require.NoError(t, local.Update(ctx, frc))
	_, err = r.Reconcile(ctx, requestFor(frc))
	require.NoError(t, err)
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
	require.Equal(t, int32(3), *mrc.Spec.WorkerGroupSpecs[0].Replicas)
}

func TestFinalizerAcceptsReplacementCredential(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Finalizers = []string{Finalizer}
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
	frc.DeletionTimestamp = new(metav1.Now())
	frc.Spec.MemberClusters[0].KubeconfigSecretRef.Name = "replacement"
	mrc := testMember(t)
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"replacement": remote}}}
	_, err := r.Reconcile(ctx, requestFor(frc))
	require.NoError(t, err)
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	require.Equal(t, "replacement", frc.Status.MemberClusterStatuses[0].KubeconfigSecretRef.Name)
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc), "persist credentials before deleting")
	for range 4 {
		_, err = r.Reconcile(ctx, requestFor(frc))
		require.NoError(t, err)
	}
	require.True(t, apierrors.IsNotFound(local.Get(ctx, client.ObjectKeyFromObject(frc), frc)))
	require.True(t, apierrors.IsNotFound(remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc)))
}

func TestFinalizerCleansHealthyMembersWhileFirstIsOffline(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Finalizers = []string{Finalizer}
	first := boundTestMember(frc)
	second := *first.DeepCopy()
	second.Name = "healthy"
	second.RayClusterName = memberRayClusterName(frc.Name, "healthy")
	second.KubeconfigSecretRef.Name = "healthy-credential"
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{first, second}
	mrc := testMember(t)
	mrc.Name = second.RayClusterName
	mrc.Labels[utils.FederationMemberLabel] = "healthy"
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"healthy-credential": remote}}}
	for range 3 {
		_, err := r.finalize(ctx, frc)
		require.NoError(t, err)
		require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	}
	require.True(t, apierrors.IsNotFound(remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc)))
	require.Len(t, frc.Status.MemberClusterStatuses, 1)
	require.Equal(t, first.Name, frc.Status.MemberClusterStatuses[0].Name)
	require.True(t, controllerutil.ContainsFinalizer(frc, Finalizer))
}

func TestForeignNameCollisionDoesNotBlockUnusedFinalizer(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	frc.Finalizers = []string{Finalizer}
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
	mrc := testMember(t)
	mrc.Labels[OwnerLabel] = "different-frc-uid"
	local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
	_, err := r.Reconcile(ctx, requestFor(frc))
	require.NoError(t, err)
	require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	require.True(t, meta.IsStatusConditionFalse(frc.Status.Conditions, "Ready"))
	for range 3 {
		_, err = r.finalize(ctx, frc)
		require.NoError(t, err)
		require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
	}
	require.False(t, controllerutil.ContainsFinalizer(frc, Finalizer))
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
	require.Equal(t, "different-frc-uid", mrc.Labels[OwnerLabel])
}

func TestCleanupDeleteRejectsConcurrentOwnerChange(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	mrc := testMember(t)
	backing := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
	intercepted := false
	remote := interceptor.NewClient(backing, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, options ...client.DeleteOption) error {
		intercepted = true
		latest := &rayv1.RayCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), latest))
		latest.Labels[OwnerLabel] = "replacement-owner"
		require.NoError(t, c.Update(ctx, latest))
		return c.Delete(ctx, obj, options...)
	}})
	r := &FederatedReconciler{Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
	_, err := r.cleanupMember(ctx, frc, boundTestMember(frc))
	require.True(t, apierrors.IsConflict(err))
	require.True(t, intercepted)
	require.NoError(t, backing.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
	require.Equal(t, "replacement-owner", mrc.Labels[OwnerLabel])
}

func TestInvalidProjectionRejectedBeforePersistence(t *testing.T) {
	for _, autoscale := range []bool{false, true} {
		for _, invalidHead := range []bool{false, true} {
			frc := testFederation()
			frc.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(autoscale)
			if invalidHead {
				frc.Spec.PrimaryCluster.HeadGroupSpec.Resources = map[string]string{"CPU": "1"}
				frc.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = map[string]string{"num-cpus": "1"}
			} else {
				frc.Spec.MemberClusters[0].WorkerGroups[0].RayStartParams = map[string]string{"head": "true"}
			}
			require.Error(t, ValidateFederation(frc))
			local := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
			r := &FederatedReconciler{Client: local, Reader: local, Scheme: testScheme(t)}
			_, err := r.ensurePrimary(context.Background(), frc)
			require.Error(t, err)
			require.True(t, apierrors.IsNotFound(local.Get(context.Background(), client.ObjectKeyFromObject(frc), &rayv1.RayCluster{})))
		}
	}
}

func TestInvalidLegacyPrimaryCanBeRepairedOnlyBeforePodsExist(t *testing.T) {
	for _, hasPod := range []bool{false, true} {
		ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
		frc.Spec.PrimaryCluster.HeadGroupSpec.Resources = map[string]string{"CPU": "1"}
		frc.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = map[string]string{"num-cpus": "1"}
		primary := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: frc.Name, Namespace: frc.Namespace, UID: "primary-uid"}, Spec: desiredPrimarySpec(frc)}
		require.NoError(t, controllerutil.SetControllerReference(frc, primary, scheme))
		require.NoError(t, recordPrimaryConfiguration(primary, primary.Spec, false))
		objects := []client.Object{primary}
		if hasPod {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "head", Namespace: frc.Namespace}}
			require.NoError(t, controllerutil.SetControllerReference(primary, pod, scheme))
			objects = append(objects, pod)
		}
		local := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
		frc.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = nil
		require.NoError(t, ValidateFederation(frc))
		if hasPod {
			require.ErrorContains(t, r.ValidatePrimaryUpdate(ctx, frc), "cannot change in place")
			_, err := r.ensurePrimary(ctx, frc)
			require.ErrorContains(t, err, "cannot change in place")
		} else {
			require.NoError(t, r.ValidatePrimaryUpdate(ctx, frc))
			repaired, err := r.ensurePrimary(ctx, frc)
			require.NoError(t, err)
			require.Equal(t, primary.UID, repaired.UID)
			require.NoError(t, utils.ValidateRayClusterSpec(&repaired.Spec, nil))
		}
	}
}

func TestAutoscalerRevisionCanBeRecoveredWithoutRuntimeChange(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	revision := primary.Annotations[primaryRevisionAnnotation]
	delete(primary.Annotations, primaryRevisionAnnotation)
	require.NoError(t, local.Update(ctx, primary))
	require.NoError(t, r.ValidatePrimaryUpdate(ctx, frc))
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	require.Equal(t, revision, primary.Annotations[primaryRevisionAnnotation])
}

func TestMemberNameCollisionUsesStableFederationIdentity(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), testFederation()
	foreign := testMember(t)
	foreign.Labels[OwnerLabel] = "foreign-frc"
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), foreign).Build()
	r := &FederatedReconciler{Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
	ready, err := r.syncInventory(ctx, frc)
	require.NoError(t, err)
	require.False(t, ready)
	name := frc.Status.MemberClusterStatuses[0].RayClusterName
	require.NotEqual(t, foreign.Name, name)
	ready, err = r.syncInventory(ctx, frc)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, name, frc.Status.MemberClusterStatuses[0].RayClusterName)
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(foreign), foreign))
	require.Equal(t, "foreign-frc", foreign.Labels[OwnerLabel])
}
