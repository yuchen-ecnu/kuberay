package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestFederationAutoscalerResources(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
	frc.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.ServiceAccountName = "custom-head"
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
	require.NoError(t, r.ensureAutoscalerResources(ctx, frc))
	key := client.ObjectKey{Namespace: frc.Namespace, Name: autoscalerConfigMapName(frc)}
	cm, role, binding := &corev1.ConfigMap{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}
	for _, object := range []client.Object{cm, role, binding} {
		require.NoError(t, local.Get(ctx, key, object))
		assert.True(t, metav1.IsControlledBy(object, frc))
	}
	assert.NotEmpty(t, cm.Data["federation_autoscaler.py"])
	var snapshot autoscalerSnapshot
	require.NoError(t, json.Unmarshal([]byte(cm.Data[autoscalerSnapshotKey]), &snapshot))
	assert.NotEmpty(t, snapshot.Error, "new autoscalers must wait for a complete observation")
	assert.Equal(t, []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{key.Name}, Verbs: []string{"get"}}}, role.Rules)
	assert.Equal(t, rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: key.Name}, binding.RoleRef)
	assert.Equal(t, []rbacv1.Subject{{Kind: "ServiceAccount", Name: "custom-head", Namespace: frc.Namespace}}, binding.Subjects)

	// Reconciliation updates adapter code without losing the currently published view.
	cm.Data[autoscalerSnapshotKey] = `{"schemaVersion":1,"error":"saved observation"}`
	require.NoError(t, local.Update(ctx, cm))
	version := cm.ResourceVersion
	require.NoError(t, r.ensureAutoscalerResources(ctx, frc))
	require.NoError(t, local.Get(ctx, key, cm))
	assert.Equal(t, version, cm.ResourceVersion)
	assert.Contains(t, cm.Data[autoscalerSnapshotKey], "saved observation")

	frc.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(false)
	frc.Name = "manual-federation"
	require.NoError(t, r.ensureAutoscalerResources(ctx, frc))
	list := &corev1.ConfigMapList{}
	require.NoError(t, local.List(ctx, list))
	assert.Len(t, list.Items, 1, "disabled autoscaling must not create runtime resources")
}

func TestFederationAutoscalerResourceOwnershipConflict(t *testing.T) {
	for _, object := range []client.Object{&corev1.ConfigMap{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}} {
		t.Run(fmt.Sprintf("%T", object), func(t *testing.T) {
			ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
			object.SetName(autoscalerConfigMapName(frc))
			object.SetNamespace(frc.Namespace)
			object.SetLabels(map[string]string{"owner": "somebody-else"})
			local := fake.NewClientBuilder().WithScheme(scheme).WithObjects(object).Build()
			r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(object), object))
			before := object.DeepCopyObject()
			require.ErrorContains(t, r.ensureAutoscalerResources(ctx, frc), "another owner")
			require.NoError(t, local.Get(ctx, client.ObjectKeyFromObject(object), object))
			assert.Equal(t, before, object, "never adopt or mutate a conflicting runtime resource")
		})
	}
}

func TestFederationAutoscalerPrimaryAndMemberResponsibilities(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
	frc.Spec.MemberClusters[0].WorkerGroups[0].IdleTimeoutSeconds = ptr.To[int32](15)
	frc.Spec.MemberClusters[0].WorkerGroups[0].Priority = ptr.To[int32](5)
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
	before, err := json.Marshal(frc)
	require.NoError(t, err)
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.True(t, utils.IsFederationAutoscalingConfigured(&primary.Spec))
	require.NoError(t, utils.ValidateRayClusterSpec(&primary.Spec, nil))
	assert.Equal(t, []string{"python"}, primary.Spec.AutoscalerOptions.Command)
	assert.Equal(t, []string{utils.FederationAutoscalerPath}, primary.Spec.AutoscalerOptions.Args)
	require.Len(t, primary.Spec.HeadGroupSpec.Template.Spec.Volumes, 1)
	assert.Equal(t, autoscalerConfigMapName(frc), primary.Spec.HeadGroupSpec.Template.Spec.Volumes[0].ConfigMap.Name)
	after, err := json.Marshal(frc)
	require.NoError(t, err)
	assert.JSONEq(t, string(before), string(after), "adapter installation must not mutate FRC templates")

	// A subsequent FRC reconcile must preserve the autoscaler's newer scale intent.
	for i := range primary.Spec.WorkerGroupSpecs {
		primary.Spec.WorkerGroupSpecs[i].Replicas = ptr.To[int32](4)
		primary.Spec.WorkerGroupSpecs[i].ScaleStrategy.WorkersToDelete = []string{"idle-worker"}
	}
	require.NoError(t, local.Update(ctx, primary))
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	for _, group := range primary.Spec.WorkerGroupSpecs {
		assert.EqualValues(t, 4, *group.Replicas)
		assert.Equal(t, []string{"idle-worker"}, group.ScaleStrategy.WorkersToDelete)
	}
	mrc := desiredMember(frc, primary, frc.Spec.MemberClusters[0])
	assert.False(t, utils.IsAutoscalingEnabled(&mrc.Spec))
	assert.Nil(t, mrc.Spec.AutoscalerOptions)
	assert.Nil(t, mrc.Spec.HeadGroupSpec)
	require.Len(t, mrc.Spec.WorkerGroupSpecs, 1)
	group := mrc.Spec.WorkerGroupSpecs[0]
	assert.Nil(t, group.ManagedBy)
	assert.Nil(t, group.IdleTimeoutSeconds)
	assert.Zero(t, ptr.Deref(group.Priority, 0))
	assert.EqualValues(t, 4, *group.Replicas)
	assert.Equal(t, []string{"idle-worker"}, group.ScaleStrategy.WorkersToDelete)
	assert.Equal(t, "head.private:6379", group.RayStartParams["address"])
}

func TestFederationAutoscalerMemberBoundsWaitForAutoscalerDecision(t *testing.T) {
	for _, tt := range []struct {
		name string
		min  int32
		max  int32
	}{
		{"lower maximum below current replicas", 0, 1},
		{"raise minimum above current replicas", 4, 10},
		{"reduce bounds to zero", 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, f := context.Background(), newAutoscalerFixture(t)
			group := &f.frc.Spec.MemberClusters[0].WorkerGroups[0]
			group.MinReplicas, group.MaxReplicas = new(tt.min), new(tt.max)
			primary, err := f.r.ensurePrimary(ctx, f.frc)
			require.NoError(t, err)
			var policy *rayv1.WorkerGroupSpec
			for i := range primary.Spec.WorkerGroupSpecs {
				if primary.Spec.WorkerGroupSpecs[i].GroupName == group.GroupName {
					policy = &primary.Spec.WorkerGroupSpecs[i]
				}
			}
			require.NotNil(t, policy)
			assert.Equal(t, tt.min, *policy.MinReplicas)
			assert.Equal(t, tt.max, *policy.MaxReplicas)
			assert.EqualValues(t, 2, *policy.Replicas, "policy edits are not autoscaler scale decisions")
			desired := desiredMember(f.frc, primary, f.frc.Spec.MemberClusters[0])
			mrc, err := syncMember(ctx, f.remote, f.frc, desired)
			require.NoError(t, err)
			assert.EqualValues(t, 2, utils.GetWorkerGroupDesiredReplicas(mrc.Spec.WorkerGroupSpecs[0]), "member must not clamp or randomly delete workers before global draining")
			assert.Empty(t, mrc.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
			require.NoError(t, f.r.publishAutoscalerSnapshot(ctx, f.frc, nil))
			assert.Empty(t, f.snapshot(t).Error, "new scheduling policy must remain observable before a scale decision")

			if tt.max == 1 {
				// After Ray chooses and drains a specific worker, propagate exactly
				// that choice rather than allowing the member to pick another Pod.
				policy.Replicas = ptr.To[int32](1)
				policy.ScaleStrategy.WorkersToDelete = []string{"worker-member-b"}
				require.NoError(t, f.local.Update(ctx, primary))
				desired = desiredMember(f.frc, primary, f.frc.Spec.MemberClusters[0])
				mrc, err = syncMember(ctx, f.remote, f.frc, desired)
				require.NoError(t, err)
				assert.EqualValues(t, 1, utils.GetWorkerGroupDesiredReplicas(mrc.Spec.WorkerGroupSpecs[0]))
				assert.Equal(t, []string{"worker-member-b"}, mrc.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
			}
		})
	}
}

func TestFederationAutoscalerMemberAnnotationTracksTopology(t *testing.T) {
	ctx, f := context.Background(), newAutoscalerFixture(t)
	const annotation = utils.FederationAutoscalerMembersAnnotation
	checkMapping := func(primary *rayv1.RayCluster, expected map[string]string) {
		t.Helper()
		mapping := map[string]string{}
		require.NoError(t, json.Unmarshal([]byte(primary.Annotations[annotation]), &mapping))
		assert.Equal(t, expected, mapping)
		for _, group := range primary.Spec.WorkerGroupSpecs {
			_, mapped := mapping[group.GroupName]
			assert.Equal(t, group.IsExternallyManaged(), mapped, "only delegated groups need a member identity")
		}
	}
	checkMapping(f.primary, map[string]string{"remote-workers": "member-b", "remote-workers-c": "member-c"})
	f.primary.Annotations["example.com/user-note"] = "preserve-me"
	// Repair a modified projection on the next reconciliation.
	f.primary.Annotations[annotation] = `{"local-workers":"wrong-member"}`
	require.NoError(t, f.local.Update(ctx, f.primary))
	extra := f.frc.Spec.MemberClusters[0].WorkerGroups[0].DeepCopy()
	extra.GroupName = "extra-remote-workers"
	f.frc.Spec.MemberClusters[0].WorkerGroups = append(f.frc.Spec.MemberClusters[0].WorkerGroups, *extra)
	primary, err := f.r.ensurePrimary(ctx, f.frc)
	require.NoError(t, err)
	checkMapping(primary, map[string]string{"remote-workers": "member-b", "extra-remote-workers": "member-b", "remote-workers-c": "member-c"})
	assert.Equal(t, "preserve-me", primary.Annotations["example.com/user-note"])

	// Removing a member removes every old group mapping, while an added member
	// gets its own identity even when it uses the same Kubernetes credential.
	f.frc.Spec.MemberClusters = f.frc.Spec.MemberClusters[1:]
	added := f.frc.Spec.MemberClusters[0].DeepCopy()
	added.Name, added.WorkerGroups[0].GroupName = "member-d", "remote-workers-d"
	f.frc.Spec.MemberClusters = append(f.frc.Spec.MemberClusters, *added)
	primary, err = f.r.ensurePrimary(ctx, f.frc)
	require.NoError(t, err)
	checkMapping(primary, map[string]string{"remote-workers-c": "member-c", "remote-workers-d": "member-d"})
	assert.Equal(t, "preserve-me", primary.Annotations["example.com/user-note"])
}

type autoscalerFixture struct {
	frc     *rayv1.FederatedRayCluster
	primary *rayv1.RayCluster
	members []*rayv1.RayCluster
	local   client.WithWatch
	remote  client.WithWatch
	r       *FederatedReconciler
	key     client.ObjectKey
}

func newAutoscalerFixture(t *testing.T) *autoscalerFixture {
	t.Helper()
	ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
	second := frc.Spec.MemberClusters[0].DeepCopy()
	second.Name = "member-c"
	second.WorkerGroups[0].GroupName = "remote-workers-c"
	frc.Spec.MemberClusters = append(frc.Spec.MemberClusters, *second)
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	remote := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity()).Build()
	r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
	require.NoError(t, r.ensureAutoscalerResources(ctx, frc))
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	primary.UID, primary.Generation = "primary-uid", 7
	require.NoError(t, local.Update(ctx, primary))
	fixture := &autoscalerFixture{
		frc: frc, primary: primary, local: local, remote: remote, r: r,
		key: client.ObjectKey{Namespace: frc.Namespace, Name: autoscalerConfigMapName(frc)},
	}
	for i, member := range frc.Spec.MemberClusters {
		mrc := desiredMember(frc, primary, member)
		mrc.UID = types.UID(fmt.Sprintf("member-%d-uid", i))
		require.NoError(t, remote.Create(ctx, mrc))
		fixture.members = append(fixture.members, mrc)
		frc.Status.MemberClusterStatuses = append(frc.Status.MemberClusterStatuses, rayv1.FederationMemberStatus{
			Name: member.Name, Namespace: member.Namespace, RayClusterName: mrc.Name,
			ClusterUID: testMemberClusterUID, KubeconfigSecretRef: member.KubeconfigSecretRef.DeepCopy(),
		})
		pod := autoscalerTestPod(mrc, "worker-"+member.Name, "worker", member.WorkerGroups[0].GroupName)
		require.NoError(t, remote.Create(ctx, pod))
	}
	for _, pod := range []*corev1.Pod{
		autoscalerTestPod(primary, "primary-head", "head", utils.RayNodeHeadGroupLabelValue),
		autoscalerTestPod(primary, "primary-worker", "worker", frc.Spec.PrimaryCluster.WorkerGroups[0].GroupName),
	} {
		require.NoError(t, local.Create(ctx, pod))
	}
	return fixture
}

func autoscalerTestPod(cluster *rayv1.RayCluster, name, kind, group string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: cluster.Namespace, UID: types.UID(name + "-uid"),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, rayv1.GroupVersion.WithKind("RayCluster"))},
			Labels:          map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: kind, utils.RayNodeGroupLabelKey: group},
			Annotations:     map[string]string{"private-note": "sensitive-annotation"},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Image: "ray:private", Env: []corev1.EnvVar{{Name: "SECRET", Value: "sensitive-env"}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1", ContainerStatuses: []corev1.ContainerStatus{{Name: "ray", Image: "ray:private", ContainerID: "private-container", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
	}
}

func (f *autoscalerFixture) snapshot(t *testing.T) autoscalerSnapshot {
	t.Helper()
	cm := &corev1.ConfigMap{}
	require.NoError(t, f.local.Get(context.Background(), f.key, cm))
	var snapshot autoscalerSnapshot
	require.NoError(t, json.Unmarshal([]byte(cm.Data[autoscalerSnapshotKey]), &snapshot))
	return snapshot
}

func TestFederationAutoscalerSnapshotIncludesOnlyOwnedRuntimePods(t *testing.T) {
	ctx, f := context.Background(), newAutoscalerFixture(t)
	for _, test := range []struct {
		name    string
		cluster *rayv1.RayCluster
		client  client.Client
		kind    string
		group   string
		mutate  func(*corev1.Pod)
	}{
		{"probe", f.primary, f.local, "head", utils.RayNodeHeadGroupLabelValue, func(p *corev1.Pod) { p.Labels["ray.io/federation-probe"] = "true" }},
		{"old-owner", f.members[0], f.remote, "worker", "remote-workers", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "previous-raycluster" }},
		{"unowned", f.members[0], f.remote, "worker", "remote-workers", func(p *corev1.Pod) { p.OwnerReferences = nil }},
		{"undeclared", f.members[0], f.remote, "worker", "other-group", func(*corev1.Pod) {}},
		{"member-head", f.members[0], f.remote, "head", utils.RayNodeHeadGroupLabelValue, func(*corev1.Pod) {}},
		{"delegated-local-pod", f.primary, f.local, "worker", "remote-workers", func(*corev1.Pod) {}},
	} {
		pod := autoscalerTestPod(test.cluster, test.name, test.kind, test.group)
		test.mutate(pod)
		require.NoError(t, test.client.Create(ctx, pod))
	}
	// Pending and terminating workers remain observable to the native provider;
	// only the provider decides whether they are active, pending, or deleting.
	pending := autoscalerTestPod(f.members[0], "pending-worker", "worker", "remote-workers")
	pending.Status.Phase = corev1.PodPending
	pending.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
	require.NoError(t, f.remote.Create(ctx, pending))
	terminating := autoscalerTestPod(f.members[0], "terminating-worker", "worker", "remote-workers")
	terminating.Finalizers = []string{"test/hold"}
	require.NoError(t, f.remote.Create(ctx, terminating))
	require.NoError(t, f.remote.Delete(ctx, terminating))
	require.NoError(t, f.r.publishAutoscalerSnapshot(ctx, f.frc, nil))
	snapshot := f.snapshot(t)
	assert.Empty(t, snapshot.Error)
	assert.Equal(t, string(f.primary.UID), snapshot.PrimaryUID)
	assert.Equal(t, f.primary.Generation, snapshot.PrimaryGeneration)
	assert.False(t, snapshot.ObservedAt.IsZero())
	names := []string{}
	for _, pod := range snapshot.Pods {
		names = append(names, pod.Name)
		assert.Empty(t, pod.Spec.Containers)
		assert.Empty(t, pod.Annotations)
		assert.Empty(t, pod.OwnerReferences)
		member := map[string]string{"remote-workers": "member-b", "remote-workers-c": "member-c"}[pod.Labels[utils.RayNodeGroupLabelKey]]
		assert.Equal(t, member, pod.Labels[utils.FederationMemberLabel])
		require.Len(t, pod.Status.ContainerStatuses, 1)
		assert.Empty(t, pod.Status.ContainerStatuses[0].Image)
		assert.Empty(t, pod.Status.ContainerStatuses[0].ContainerID)
		if pod.Name == "pending-worker" {
			assert.Equal(t, corev1.PodPending, pod.Status.Phase)
			assert.Equal(t, "ContainerCreating", pod.Status.ContainerStatuses[0].State.Waiting.Reason)
		} else {
			assert.NotNil(t, pod.Status.ContainerStatuses[0].State.Running)
		}
		if pod.Name == "terminating-worker" {
			assert.NotNil(t, pod.DeletionTimestamp)
		}
	}
	assert.ElementsMatch(t, []string{"primary-head", "primary-worker", "worker-member-b", "worker-member-c", "pending-worker", "terminating-worker"}, names)
	assert.True(t, meta.IsStatusConditionTrue(f.frc.Status.Conditions, "AutoscalerObservationReady"))
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "sensitive-")
	assert.NotContains(t, string(encoded), "ray:private")
}

func TestFederationAutoscalerSnapshotAllowsPodNamesAcrossMembers(t *testing.T) {
	ctx, f := context.Background(), newAutoscalerFixture(t)
	// Two members may legally have identically named Pods in different namespaces
	// on the same API. Their Ray instance identities include the member name.
	mrc := f.members[1]
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-member-c", Namespace: mrc.Namespace}}
	require.NoError(t, f.remote.Delete(ctx, oldPod))
	require.NoError(t, f.remote.Delete(ctx, mrc))
	mrc.Namespace, mrc.ResourceVersion, mrc.UID = "other-workers", "", "member-c-new-uid"
	f.frc.Spec.MemberClusters[1].Namespace = mrc.Namespace
	f.frc.Status.MemberClusterStatuses[1].Namespace = mrc.Namespace
	require.NoError(t, f.remote.Create(ctx, mrc))
	for _, member := range f.members {
		pod := autoscalerTestPod(member, "primary-worker", "worker", member.Spec.WorkerGroupSpecs[0].GroupName)
		pod.UID = types.UID(string(member.UID) + "-" + pod.Name)
		pod.Labels[utils.FederationMemberLabel] = "untrusted-pod-label"
		require.NoError(t, f.remote.Create(ctx, pod))
	}
	require.NoError(t, f.r.publishAutoscalerSnapshot(ctx, f.frc, nil))
	snapshot := f.snapshot(t)
	require.Empty(t, snapshot.Error)
	require.Len(t, snapshot.Pods, 5)
	identities := []string{}
	for _, pod := range snapshot.Pods {
		if pod.Name == "primary-worker" {
			identities = append(identities, pod.Labels[utils.FederationMemberLabel])
		}
		assert.NotContains(t, pod.Name, "/", "snapshot metadata retains the actual Kubernetes Pod name")
		assert.NotEqual(t, "untrusted-pod-label", pod.Labels[utils.FederationMemberLabel], "member identity comes from the bound RayCluster")
	}
	assert.ElementsMatch(t, []string{"", "member-b", "member-c"}, identities)
	assert.True(t, meta.IsStatusConditionTrue(f.frc.Status.Conditions, "AutoscalerObservationReady"))
}

func TestFederationAutoscalerSnapshotFailsClosed(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*testing.T, *autoscalerFixture) error
		message string
	}{
		{"reconcile error", func(*testing.T, *autoscalerFixture) error { return errors.New("replica propagation failed") }, "replica propagation failed"},
		{"incomplete inventory", func(_ *testing.T, f *autoscalerFixture) error {
			f.frc.Status.MemberClusterStatuses = f.frc.Status.MemberClusterStatuses[:1]
			return nil
		}, "destination inventory"},
		{"topology transition", func(_ *testing.T, f *autoscalerFixture) error {
			condition(&f.frc.Status.Conditions, f.frc.Generation, "Ready", metav1.ConditionFalse, "ReconcilingTopology", "cleanup pending")
			return nil
		}, "destination inventory"},
		{"destination mismatch", func(_ *testing.T, f *autoscalerFixture) error {
			f.frc.Status.MemberClusterStatuses[1].Name = "previous-member"
			return nil
		}, "member member-c destination inventory"},
		{"API unavailable", func(_ *testing.T, f *autoscalerFixture) error {
			f.r.Members.(*fixedMembers).err = errors.New("connection unavailable")
			return nil
		}, "connection unavailable"},
		{"cluster identity changed", func(t *testing.T, f *autoscalerFixture) error {
			identity := testMemberClusterIdentity()
			require.NoError(t, f.remote.Get(context.Background(), client.ObjectKeyFromObject(identity), identity))
			identity.UID = "other-cluster"
			require.NoError(t, f.remote.Update(context.Background(), identity))
			return nil
		}, "identity"},
		{"member owner changed", func(t *testing.T, f *autoscalerFixture) error {
			mrc := f.members[1]
			mrc.Labels[OwnerLabel] = "different-frc"
			require.NoError(t, f.remote.Update(context.Background(), mrc))
			return nil
		}, "bound destination"},
		{"member deleted", func(t *testing.T, f *autoscalerFixture) error {
			require.NoError(t, f.remote.Delete(context.Background(), f.members[1]))
			return nil
		}, "not found"},
		{"replicas not propagated", func(t *testing.T, f *autoscalerFixture) error {
			mrc := f.members[1]
			mrc.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](3)
			require.NoError(t, f.remote.Update(context.Background(), mrc))
			return nil
		}, "replica intent"},
		{"deletion intent not propagated", func(t *testing.T, f *autoscalerFixture) error {
			mrc := f.members[1]
			mrc.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{"idle-worker"}
			require.NoError(t, f.remote.Update(context.Background(), mrc))
			return nil
		}, "replica intent"},
		{"Pod listing failure", func(_ *testing.T, f *autoscalerFixture) error {
			f.r.Members.(*fixedMembers).clients["member-credential"] = interceptor.NewClient(f.remote, interceptor.Funcs{
				List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
					return errors.New("pod listing unavailable")
				},
			})
			return nil
		}, "pod listing unavailable"},
		{"concurrent primary scale", func(_ *testing.T, f *autoscalerFixture) error {
			reads := 0
			f.r.Reader = interceptor.NewClient(f.local, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, object, opts...); err != nil {
						return err
					}
					if primary, ok := object.(*rayv1.RayCluster); ok {
						reads++
						if reads == 2 {
							primary.Generation++
						}
					}
					return nil
				},
			})
			return nil
		}, "primary changed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, f := context.Background(), newAutoscalerFixture(t)
			require.NoError(t, f.r.publishAutoscalerSnapshot(ctx, f.frc, nil))
			require.Len(t, f.snapshot(t).Pods, 4)
			reconcileErr := tt.mutate(t, f)
			require.NoError(t, f.r.publishAutoscalerSnapshot(ctx, f.frc, reconcileErr))
			snapshot := f.snapshot(t)
			require.ErrorContains(t, errors.New(snapshot.Error), tt.message)
			assert.Empty(t, snapshot.Pods, "an unavailable member must not appear as zero live workers in a partial inventory")
			assert.True(t, meta.IsStatusConditionFalse(f.frc.Status.Conditions, "AutoscalerObservationReady"))
		})
	}
}

func TestFederationAutoscalerRuntimeUpgradeAndPolicyChanges(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
			frc.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(enabled)
			local := fake.NewClientBuilder().WithScheme(scheme).Build()
			r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme}
			_, err := r.ensurePrimary(ctx, frc)
			require.NoError(t, err)
			frc.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(!enabled)
			require.ErrorContains(t, r.ValidatePrimaryUpdate(ctx, frc), "cannot change in place")
		})
	}
	ctx, f := context.Background(), newAutoscalerFixture(t)
	f.frc.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{IdleTimeoutSeconds: ptr.To[int32](10), UpscalingMode: ptr.To(rayv1.UpscalingMode("Conservative"))}
	f.frc.Spec.PrimaryCluster.WorkerGroups[0].Priority = ptr.To[int32](3)
	f.frc.Spec.PrimaryCluster.WorkerGroups[0].IdleTimeoutSeconds = ptr.To[int32](5)
	f.frc.Spec.PrimaryCluster.WorkerGroups[0].MaxReplicas = ptr.To[int32](15)
	require.NoError(t, f.r.ValidatePrimaryUpdate(ctx, f.frc))
	primary, err := f.r.ensurePrimary(ctx, f.frc)
	require.NoError(t, err)
	assert.EqualValues(t, 10, *primary.Spec.AutoscalerOptions.IdleTimeoutSeconds)
	assert.EqualValues(t, 3, *primary.Spec.WorkerGroupSpecs[0].Priority)
	f.frc.Spec.PrimaryCluster.AutoscalerOptions.Env = []corev1.EnvVar{{Name: "AUTOSCALER_UPDATE_INTERVAL_S", Value: "2"}}
	require.ErrorContains(t, f.r.ValidatePrimaryUpdate(ctx, f.frc), "cannot change in place")
}

func TestFederationAutoscalerDisabledRevisionCompatibility(t *testing.T) {
	// Before autoscaling support, existing FRCs stored the SHA of this exact head
	// tuple. Adding optional autoscaler fields must not demand a runtime restart.
	frc := federationWithLocalWorkers()
	data, err := json.Marshal([]any{frc.Spec.PrimaryCluster.RayVersion, frc.Spec.PrimaryCluster.HeadGroupSpec})
	require.NoError(t, err)
	digest := sha256.Sum256(data)
	legacy := hex.EncodeToString(digest[:])[:32]
	assert.Equal(t, legacy, FederationPrimaryConfiguration(frc.Spec).Head)
	frc.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(false)
	assert.Equal(t, legacy, FederationPrimaryConfiguration(frc.Spec).Head)
}

func TestFederationAutoscalerRemovedLocalGroupDrainsWithoutSuspend(t *testing.T) {
	ctx, f := context.Background(), newAutoscalerFixture(t)
	f.frc.Spec.PrimaryCluster.WorkerGroups = nil
	primary, err := f.r.ensurePrimary(ctx, f.frc)
	require.NoError(t, err)
	require.NoError(t, utils.ValidateRayClusterSpec(&primary.Spec, nil))
	for _, group := range primary.Spec.WorkerGroupSpecs {
		if group.GroupName == "local-workers" {
			assert.False(t, ptr.Deref(group.Suspend, false))
			assert.Zero(t, *group.Replicas)
			assert.Zero(t, *group.MinReplicas)
			assert.Zero(t, *group.MaxReplicas)
			return
		}
	}
	t.Fatal("removed group must remain until its owned workers are drained")
}

func TestFederationAutoscalerSnapshotConfigMapOwnership(t *testing.T) {
	ctx, f := context.Background(), newAutoscalerFixture(t)
	cm := &corev1.ConfigMap{}
	require.NoError(t, f.local.Get(ctx, f.key, cm))
	require.NoError(t, controllerutil.RemoveControllerReference(f.frc, cm, f.r.Scheme))
	require.NoError(t, f.local.Update(ctx, cm))
	require.NoError(t, f.local.Get(ctx, f.key, cm))
	before := cm.DeepCopy()
	require.ErrorContains(t, f.r.publishAutoscalerSnapshot(ctx, f.frc, nil), "different owner")
	require.NoError(t, f.local.Get(ctx, f.key, cm))
	assert.Equal(t, before, cm)
}
