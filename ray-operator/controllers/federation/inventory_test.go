package federation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func boundTestMember(frc *rayv1.FederatedRayCluster) rayv1.FederationMemberStatus {
	member := frc.Spec.MemberClusters[0]
	return rayv1.FederationMemberStatus{Name: member.Name, Namespace: member.Namespace, RayClusterName: memberRayClusterName(frc.Name, member.Name), ClusterUID: testMemberClusterUID, KubeconfigSecretRef: member.KubeconfigSecretRef.DeepCopy()}
}

func TestMemberCredentialReferenceRotationPreservesResources(t *testing.T) {
	ctx, scheme := context.Background(), testScheme(t)
	frc, mrc := testFederation(), testMember(t)
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
	worker := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: mrc.Namespace, UID: "worker-uid"}}
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc, worker).Build()
	// The original credential is no longer usable. Rotation must authenticate
	// with the replacement while checking the persisted destination identity.
	members := &fixedMembers{clients: map[string]client.Client{"replacement": remote}}
	frc.Spec.MemberClusters[0].KubeconfigSecretRef.Name = "replacement"
	r := &FederatedReconciler{Scheme: scheme, Members: members}
	ready, err := r.syncInventory(ctx, frc)
	require.NoError(t, err)
	assert.False(t, ready, "persist the new credential before remote writes")
	require.Len(t, frc.Status.MemberClusterStatuses, 1)
	assert.Equal(t, "replacement", frc.Status.MemberClusterStatuses[0].KubeconfigSecretRef.Name)
	assert.EqualValues(t, testMemberClusterUID, frc.Status.MemberClusterStatuses[0].ClusterUID)
	// A new reconciler must use only the durable inventory, not an in-memory binding.
	r = &FederatedReconciler{Scheme: scheme, Members: members}
	ready, err = r.syncInventory(ctx, frc)
	require.NoError(t, err)
	assert.True(t, ready)
	for _, object := range []client.Object{mrc, worker} {
		uid := object.GetUID()
		require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(object), object))
		assert.Equal(t, uid, object.GetUID())
		assert.True(t, object.GetDeletionTimestamp().IsZero())
	}
}

func TestInventoryPersistencePrecedesRemoteWrites(t *testing.T) {
	for _, rotation := range []bool{false, true} {
		name := "initial-binding"
		if rotation {
			name = "credential-rotation"
		}
		t.Run(name, func(t *testing.T) {
			ctx, scheme := context.Background(), testScheme(t)
			frc := testFederation()
			frc.Finalizers = []string{Finalizer}
			if rotation {
				frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
				frc.Spec.MemberClusters[0].KubeconfigSecretRef.Name = "replacement"
			}
			previous := frc.DeepCopy()
			local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
			remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity()).WithInterceptorFuncs(interceptor.Funcs{
				Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
					t.Fatal("remote create before inventory persistence")
					return nil
				},
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
					t.Fatal("remote patch before inventory persistence")
					return nil
				},
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
					t.Fatal("remote delete before inventory persistence")
					return nil
				},
			}).Build()
			conflicting := interceptor.NewClient(local, interceptor.Funcs{
				SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
					return apierrors.NewConflict(rayv1.Resource("federatedrayclusters"), frc.Name, errors.New("concurrent status write"))
				},
			})
			r := &FederatedReconciler{Client: conflicting, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{frc.Spec.MemberClusters[0].KubeconfigSecretRef.Name: remote}}}
			_, err := r.Reconcile(ctx, requestFor(frc))
			require.True(t, apierrors.IsConflict(err), "expected inventory conflict: %v", err)
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			assert.Equal(t, previous.Status, frc.Status)
			r.Client = local
			_, err = r.Reconcile(ctx, requestFor(frc))
			require.NoError(t, err)
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
			require.Len(t, frc.Status.MemberClusterStatuses, 1)
			assert.Equal(t, boundTestMember(frc), frc.Status.MemberClusterStatuses[0])
			assert.True(t, apierrors.IsNotFound(local.Get(ctx, client.ObjectKeyFromObject(frc), &rayv1.RayCluster{})))
		})
	}
}

func TestMemberCredentialRetargetCannotForgetOriginalCluster(t *testing.T) {
	for _, replacementSecret := range []bool{false, true} {
		for _, policy := range []string{"Delete", "Orphan"} {
			name := policy + "/same-secret"
			if replacementSecret {
				name = policy + "/replacement-secret"
			}
			t.Run(name, func(t *testing.T) {
				ctx, scheme := context.Background(), testScheme(t)
				frc, mrc := testFederation(), testMember(t)
				frc.Finalizers, frc.Spec.MemberCleanupPolicy = []string{Finalizer}, policy
				frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
				original := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
				otherIdentity := testMemberClusterIdentity()
				otherIdentity.UID = "other-cluster-uid"
				other := fake.NewClientBuilder().WithScheme(scheme).WithObjects(otherIdentity).Build()
				members := &fixedMembers{clients: map[string]client.Client{"member-credential": other}}
				if replacementSecret {
					frc.Spec.MemberClusters[0].KubeconfigSecretRef.Name = "replacement"
					members.clients["replacement"] = other
				}
				local := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(frc).WithObjects(frc).Build()
				r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: members}
				_, err := r.Reconcile(ctx, requestFor(frc))
				if replacementSecret {
					require.ErrorContains(t, err, "cluster identity changed")
				} else {
					require.NoError(t, err) // The per-member failure is recorded in status.
				}
				require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
				assert.EqualValues(t, testMemberClusterUID, frc.Status.MemberClusterStatuses[0].ClusterUID)
				assert.Equal(t, "member-credential", frc.Status.MemberClusterStatuses[0].KubeconfigSecretRef.Name)
				assert.True(t, meta.IsStatusConditionFalse(frc.Status.Conditions, "Ready"))
				assert.True(t, apierrors.IsNotFound(other.Get(ctx, client.ObjectKeyFromObject(mrc), &rayv1.RayCluster{})), "retargeting must not create a second member")
				// NotFound in the other cluster cannot release the original inventory.
				_, err = r.finalize(ctx, frc)
				require.NoError(t, err)
				require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
				assert.True(t, controllerutil.ContainsFinalizer(frc, Finalizer))
				require.Len(t, frc.Status.MemberClusterStatuses, 1)
				assert.Equal(t, "DeletionBlocked", meta.FindStatusCondition(frc.Status.Conditions, "Ready").Reason)
				require.NoError(t, original.Get(ctx, client.ObjectKeyFromObject(mrc), mrc))
				assert.Equal(t, string(frc.UID), mrc.Labels[OwnerLabel])
				// Restoring the original target permits cleanup, including retries.
				members.clients["member-credential"] = original
				for range 4 {
					_, err = r.finalize(ctx, frc)
					require.NoError(t, err)
					require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(frc), frc))
				}
				assert.False(t, controllerutil.ContainsFinalizer(frc, Finalizer))
			})
		}
	}
}

func TestMemberIdentityMustBeObservable(t *testing.T) {
	ctx, scheme := context.Background(), testScheme(t)
	frc := testFederation()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, metav1.NamespaceSystem, errors.New("missing identity permission"))
		},
	}).Build()
	r := &FederatedReconciler{Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
	_, err := r.syncInventory(ctx, frc)
	require.ErrorContains(t, err, "cannot verify member cluster identity")
	assert.Empty(t, frc.Status.MemberClusterStatuses)
	done, err := r.cleanupMember(ctx, frc, boundTestMember(frc))
	require.ErrorContains(t, err, "cannot verify member cluster identity")
	assert.False(t, done)
}

func TestFailedDuplicateDestinationCanBeRemoved(t *testing.T) {
	ctx, scheme := context.Background(), testScheme(t)
	frc, mrc := testFederation(), testMember(t)
	active := boundTestMember(frc)
	duplicate := *active.DeepCopy()
	duplicate.Name = "member-c"
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{active, duplicate}
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity(), mrc).Build()
	r := &FederatedReconciler{Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}

	ready, err := r.syncInventory(ctx, frc)
	require.NoError(t, err)
	assert.False(t, ready, "removing an inventory entry must be persisted before reconciliation continues")
	require.Equal(t, []rayv1.FederationMemberStatus{active}, frc.Status.MemberClusterStatuses)
	require.NoError(t, remote.Get(ctx, client.ObjectKeyFromObject(mrc), mrc), "the active member RayCluster must be preserved")

	ready, err = r.syncInventory(ctx, frc)
	require.NoError(t, err)
	assert.True(t, ready)

	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{duplicate}
	done, err := r.cleanupMember(ctx, frc, duplicate)
	require.ErrorContains(t, err, "different member binding")
	assert.False(t, done, "an untracked binding must remain protected")
}

func TestLegacyInventoryRequiresOriginalMemberEvidence(t *testing.T) {
	for _, state := range []string{"owned", "missing", "foreign"} {
		t.Run(state, func(t *testing.T) {
			ctx, scheme := context.Background(), testScheme(t)
			frc, mrc := testFederation(), testMember(t)
			status := boundTestMember(frc)
			status.ClusterUID = ""
			status.RayClusterName = ""
			mrc.Name = frc.Name
			frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{status}
			objects := []client.Object{testMemberClusterIdentity()}
			if state != "missing" {
				if state == "foreign" {
					mrc.Labels[OwnerLabel] = "another-federation"
				}
				objects = append(objects, mrc)
			}
			remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			r := &FederatedReconciler{Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
			ready, err := r.syncInventory(ctx, frc)
			assert.False(t, ready)
			if state == "owned" {
				require.NoError(t, err)
				assert.EqualValues(t, testMemberClusterUID, frc.Status.MemberClusterStatuses[0].ClusterUID)
				assert.Equal(t, frc.Name, frc.Status.MemberClusterStatuses[0].RayClusterName)
			} else {
				require.ErrorContains(t, err, "cannot bind legacy member")
				assert.Empty(t, frc.Status.MemberClusterStatuses[0].ClusterUID)
			}
		})
	}
}

func TestCredentialWatchIncludesPersistedCleanupReferences(t *testing.T) {
	ctx, scheme := context.Background(), testScheme(t)
	frc := testFederation()
	frc.Status.MemberClusterStatuses = []rayv1.FederationMemberStatus{boundTestMember(frc)}
	frc.Spec.MemberClusters[0].KubeconfigSecretRef.Name = "replacement"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(frc).Build()
	r := &FederatedReconciler{Client: c}
	for _, name := range []string{"member-credential", "replacement"} {
		requests := r.secretRequests(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: frc.Namespace}})
		require.Len(t, requests, 1)
		assert.Equal(t, client.ObjectKeyFromObject(frc), requests[0].NamespacedName)
	}
	assert.Empty(t, r.secretRequests(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: frc.Namespace}}))
}
