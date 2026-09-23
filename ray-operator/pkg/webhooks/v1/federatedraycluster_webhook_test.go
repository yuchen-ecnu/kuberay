package v1

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func webhookFederation() *rayv1.FederatedRayCluster {
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Image: "rayproject/ray:2.56.0"}}}}
	return &rayv1.FederatedRayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-validation", Namespace: "default"},
		Spec: rayv1.FederatedRayClusterSpec{
			PrimaryCluster: rayv1.FederationPrimaryCluster{
				RayVersion: "2.56.0", HeadGroupSpec: &rayv1.HeadGroupSpec{Template: template},
				WorkerGroups: []rayv1.WorkerGroupSpec{{GroupName: "local", Template: *template.DeepCopy(), Replicas: ptr.To[int32](1), MinReplicas: ptr.To[int32](0), MaxReplicas: ptr.To[int32](10)}},
			},
			MemberClusters: []rayv1.FederationMemberCluster{{Name: "manual", Namespace: "workers"}},
			Networking:     rayv1.FederationNetworking{HeadEndpoint: rayv1.FederationHeadEndpoint{Mode: "UserProvided", Address: "head.private", GCSPort: 6379}},
		},
	}
}

func TestFederatedRayClusterUpdateValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*rayv1.FederatedRayCluster)
		valid  bool
	}{
		{"unchanged", func(*rayv1.FederatedRayCluster) {}, true},
		{"version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "2.57.0" }, false},
		{"head-image", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.Containers[0].Image = "rayproject/ray:2.57.0"
		}, false},
		{"local-worker-image", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.WorkerGroups[0].Template.Spec.Containers[0].Image = "rayproject/ray:2.57.0"
		}, false},
		{"scale", func(f *rayv1.FederatedRayCluster) {
			group := &f.Spec.PrimaryCluster.WorkerGroups[0]
			group.Replicas, group.MinReplicas, group.MaxReplicas, group.Suspend = ptr.To[int32](3), ptr.To[int32](1), ptr.To[int32](5), new(true)
			group.ScaleStrategy.WorkersToDelete = []string{"worker"}
		}, true},
		{"remove-group", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.WorkerGroups = nil }, true},
		{"add-group", func(f *rayv1.FederatedRayCluster) {
			group := f.Spec.PrimaryCluster.WorkerGroups[0].DeepCopy()
			group.GroupName = "another"
			f.Spec.PrimaryCluster.WorkerGroups = append(f.Spec.PrimaryCluster.WorkerGroups, *group)
		}, true},
		{"endpoint", func(f *rayv1.FederatedRayCluster) { f.Spec.Networking.HeadEndpoint.Address = "new-head.private" }, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			old := webhookFederation()
			current := old.DeepCopy()
			tt.mutate(current)
			_, err := (&FederatedRayClusterWebhook{}).ValidateUpdate(context.Background(), old, current)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.True(t, apierrors.IsInvalid(err), "expected invalid update: %v", err)
				require.ErrorContains(t, err, "cannot change in place")
			}
		})
	}
}

func TestFederatedRayClusterCreateValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*rayv1.FederatedRayCluster)
		valid  bool
		match  string
	}{
		{"valid", func(*rayv1.FederatedRayCluster) {}, true, ""},
		{"managedBy", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.WorkerGroups[0].ManagedBy = ptr.To(rayv1.WorkerGroupManagedByFederatedRayCluster)
		}, false, "managedBy is assigned by the federation"},
		{"host-network", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.WorkerGroups[0].Template.Spec.HostNetwork = true
		}, true, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			frc := webhookFederation()
			tt.mutate(frc)
			_, err := (&FederatedRayClusterWebhook{}).ValidateCreate(context.Background(), frc)
			if tt.valid {
				require.NoError(t, err)
				return
			}
			require.True(t, apierrors.IsInvalid(err), "expected invalid create: %v", err)
			require.ErrorContains(t, err, tt.match)
		})
	}
}

func TestFederationAdmissionUsesAcceptedRuntimeForLegacyRecovery(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	accepted := webhookFederation()
	accepted.UID = "federation-uid"
	accepted.Spec.PrimaryCluster.WorkerGroups[0].NumOfHosts = 1
	accepted.Spec.PrimaryCluster.WorkerGroups[0].Priority = new(int32(0))
	primary := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: accepted.Name, Namespace: accepted.Namespace, UID: "primary-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(accepted, rayv1.GroupVersion.WithKind("FederatedRayCluster"))},
		},
		Spec: rayv1.RayClusterSpec{RayVersion: accepted.Spec.PrimaryCluster.RayVersion, HeadGroupSpec: accepted.Spec.PrimaryCluster.HeadGroupSpec.DeepCopy(), WorkerGroupSpecs: accepted.Spec.PrimaryCluster.DeepCopy().WorkerGroups},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(primary).Build()
	webhook := &FederatedRayClusterWebhook{Reader: reader}
	// An earlier invalid member spec must not grant permission to upgrade the
	// already accepted head runtime while fixing that member.
	invalid := accepted.DeepCopy()
	invalid.Spec.Networking.HeadEndpoint.Address = "invalid;address"
	changed := accepted.DeepCopy()
	changed.Spec.PrimaryCluster.RayVersion = "2.57.0"
	_, err := webhook.ValidateUpdate(context.Background(), invalid, changed)
	require.ErrorContains(t, err, "cannot change in place")
	// A valid but unapplied old spec may always be reverted to the actual
	// accepted configuration, even when old-vs-new immutable hashes differ.
	_, err = webhook.ValidateUpdate(context.Background(), changed, accepted)
	require.NoError(t, err)
}

var _ = Describe("FederatedRayCluster validating webhook", func() {
	It("rejects runtime changes while allowing scaling through the API server", func() {
		hostNetwork := webhookFederation()
		hostNetwork.Name = "host-network"
		hostNetwork.Spec.PrimaryCluster.WorkerGroups[0].Template.Spec.HostNetwork = true
		Expect(k8sClient.Create(ctx, hostNetwork)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, hostNetwork)).To(Succeed()) })

		frc := webhookFederation()
		Expect(k8sClient.Create(ctx, frc)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, frc)).To(Succeed()) })
		primary := &rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: frc.Name, Namespace: frc.Namespace,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(frc, rayv1.GroupVersion.WithKind("FederatedRayCluster"))},
			},
			Spec: rayv1.RayClusterSpec{RayVersion: frc.Spec.PrimaryCluster.RayVersion, HeadGroupSpec: frc.Spec.PrimaryCluster.HeadGroupSpec.DeepCopy(), WorkerGroupSpecs: frc.Spec.PrimaryCluster.DeepCopy().WorkerGroups},
		}
		Expect(k8sClient.Create(ctx, primary)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, primary)).To(Succeed()) })
		changed := frc.DeepCopy()
		changed.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.Containers[0].Image = "rayproject/ray:2.57.0"
		Expect(k8sClient.Update(ctx, changed)).To(MatchError(ContainSubstring("cannot change in place")))
		frc.Spec.PrimaryCluster.WorkerGroups[0].Replicas = ptr.To[int32](2)
		Expect(k8sClient.Update(ctx, frc)).To(Succeed())
	})
})
