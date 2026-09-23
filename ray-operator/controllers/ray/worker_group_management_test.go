package ray

import (
	"context"
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/expectations"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestFederationManagedGroupDoesNotManageLocalPods(t *testing.T) {
	setupTest(t)
	features.SetFeatureGateDuringTest(t, features.RayFederation, false)
	cluster := testRayCluster.DeepCopy()
	cluster.Spec.EnableInTreeAutoscaling = new(false)
	cluster.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](100)
	cluster.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{headNodeName}
	require.NoError(t, utils.ValidateRayClusterSpec(&cluster.Spec, nil), "external managedBy requires no FRC or federation feature gate")
	// Include only the head. An external deletion request must not delete it.
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(testPods[0]).Build()
	r := &RayClusterReconciler{Client: c, Scheme: scheme, Recorder: &events.FakeRecorder{}, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(c)}
	require.NoError(t, r.reconcilePods(context.Background(), cluster))
	pods := &corev1.PodList{}
	require.NoError(t, c.List(context.Background(), pods, client.InNamespace(cluster.Namespace)))
	require.Len(t, pods.Items, 1)
	assert.Equal(t, headNodeName, pods.Items[0].Name)
	assert.Zero(t, utils.CalculateDesiredReplicas(cluster))
	assert.Zero(t, utils.CalculateMinReplicas(cluster))
	assert.Zero(t, utils.CalculateMaxReplicas(cluster))
}

func TestWorkerGroupManagedByMigrationDrainsOnlyOwnedLocalWorkers(t *testing.T) {
	setupTest(t)
	ctx := context.Background()
	cluster := testRayCluster.DeepCopy()
	cluster.UID = "primary-owner"
	testPods[0].(*corev1.Pod).OwnerReferences[0].UID = cluster.UID
	cluster.Spec.EnableInTreeAutoscaling = new(false)
	cluster.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	worker := testPods[1].(*corev1.Pod).DeepCopy()
	worker.Labels[utils.RayNodeTypeLabelKey] = string(rayv1.WorkerNode)
	worker.OwnerReferences = nil
	require.NoError(t, ctrl.SetControllerReference(cluster, worker, scheme))
	foreign := worker.DeepCopy()
	foreign.Name = "foreign-worker"
	foreign.OwnerReferences[0].UID = "foreign-owner"
	cluster.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{foreign.Name, headNodeName}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(testPods[0], worker, foreign).Build()
	r := &RayClusterReconciler{Client: c, Scheme: scheme, Recorder: &events.FakeRecorder{}, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(c)}
	for range 2 {
		require.NoError(t, r.reconcilePods(ctx, cluster))
	}
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(worker), &corev1.Pod{})))
	for _, name := range []string{headNodeName, foreign.Name} {
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, &corev1.Pod{}))
	}
	assert.True(t, metav1.IsControlledBy(worker, cluster))
	// Returning responsibility to KubeRay creates local workers without adopting
	// or deleting Pods controlled by the external provider.
	cluster.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](1)
	cluster.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = nil
	for range 2 {
		require.NoError(t, r.reconcilePods(ctx, cluster))
	}
	pods := &corev1.PodList{}
	require.NoError(t, c.List(ctx, pods, common.RayClusterGroupPodsAssociationOptions(cluster, worker.Labels[utils.RayNodeGroupLabelKey]).ToListOptions()...))
	owned := 0
	for _, pod := range pods.Items {
		if metav1.IsControlledBy(&pod, cluster) {
			owned++
			assert.NotEqual(t, worker.Name, pod.Name)
		}
	}
	assert.Equal(t, 1, owned)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), &corev1.Pod{}))
}

func TestWorkerGroupManagedByLocalAccountingAndUpgradeHash(t *testing.T) {
	setupTest(t)
	cluster := testRayCluster.DeepCopy()
	before := cluster.DeepCopy()
	desired, minimum, maximum := utils.CalculateDesiredReplicas(cluster), utils.CalculateMinReplicas(cluster), utils.CalculateMaxReplicas(cluster)
	resources, minResources := utils.CalculateDesiredResources(cluster), utils.CalculateMinResources(cluster)
	beforeHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)
	remote := *cluster.Spec.WorkerGroupSpecs[0].DeepCopy()
	remote.GroupName, remote.Replicas = "remote", ptr.To[int32](500)
	remote.ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
	cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, remote)
	assert.Equal(t, desired, utils.CalculateDesiredReplicas(cluster))
	assert.Equal(t, minimum, utils.CalculateMinReplicas(cluster))
	assert.Equal(t, maximum, utils.CalculateMaxReplicas(cluster))
	assert.Equal(t, resources, utils.CalculateDesiredResources(cluster))
	assert.Equal(t, minResources, utils.CalculateMinResources(cluster))
	afterHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)
	assert.Equal(t, beforeHash, afterHash, "remote templates must not trigger local upgrades")
	assert.Equal(t, before.Spec.WorkerGroupSpecs[0], cluster.Spec.WorkerGroupSpecs[0])
	cluster.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	explicitLocalHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)
	assert.Equal(t, beforeHash, explicitLocalHash, "explicit local management must not trigger upgrades")
}

func TestWorkerGroupManagedByValidation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		managedBy *string
		autoscale bool
		message   string
	}{
		{name: "local"},
		{name: "federation", managedBy: new(rayv1.WorkerGroupManagedByFederatedRayCluster)},
		{name: "empty", managedBy: new(""), message: "invalid managedBy"},
		{name: "unknown", managedBy: new("example.io/unknown-controller"), message: "invalid managedBy"},
		{name: "operator-name-is-cluster-scoped", managedBy: new("ray.io/kuberay-operator"), message: "invalid managedBy"},
		{name: "legacy-federation-name", managedBy: new("ray.io/federation-controller"), message: "invalid managedBy"},
		{name: "multikueue-is-cluster-scoped", managedBy: new("kueue.x-k8s.io/multikueue"), message: "invalid managedBy"},
		{name: "explicit-local", managedBy: new(rayv1.WorkerGroupManagedByRayCluster)},
		{name: "local-autoscaling", managedBy: new(rayv1.WorkerGroupManagedByRayCluster), autoscale: true},
		{name: "federation-autoscaling", managedBy: new(rayv1.WorkerGroupManagedByFederatedRayCluster), autoscale: true, message: "enableInTreeAutoscaling is not supported"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setupTest(t)
			features.SetFeatureGateDuringTest(t, features.RayFederation, false)
			cluster := testRayCluster.DeepCopy()
			cluster.Spec.EnableInTreeAutoscaling = new(tt.autoscale)
			cluster.Spec.WorkerGroupSpecs[0].ManagedBy = tt.managedBy
			err := utils.ValidateRayClusterSpec(&cluster.Spec, nil)
			if tt.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.message)
			}
		})
	}
}

func TestFederationManagedGroupPreservesProviderPodsInTheSameCluster(t *testing.T) {
	setupTest(t)
	features.SetFeatureGateDuringTest(t, features.RayFederation, false)
	ctx := context.Background()
	cluster := testRayCluster.DeepCopy()
	cluster.UID = "primary-owner"
	testPods[0].(*corev1.Pod).OwnerReferences[0].UID = cluster.UID
	cluster.Spec.EnableInTreeAutoscaling = new(false)
	cluster.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	previous := testPods[1].(*corev1.Pod).DeepCopy()
	previous.Labels[utils.RayNodeTypeLabelKey] = string(rayv1.WorkerNode)
	previous.OwnerReferences = nil
	previous.Status.Phase = corev1.PodRunning
	previous.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	require.NoError(t, ctrl.SetControllerReference(cluster, previous, scheme))
	external := previous.DeepCopy()
	external.Name = "external-provider-worker"
	external.OwnerReferences[0].UID = "external-provider"
	head := testPods[0].(*corev1.Pod).DeepCopy()
	// Classification must consider node type even if group labels coincide.
	head.Labels[utils.RayNodeGroupLabelKey] = cluster.Spec.WorkerGroupSpecs[0].GroupName
	head.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	service := testServices[0].(*corev1.Service).DeepCopy()
	service.Spec.ClusterIP = "10.96.0.10"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(head, previous, external, service).Build()
	r := &RayClusterReconciler{Client: c, Scheme: scheme, Recorder: &events.FakeRecorder{}}
	observed, err := r.calculateStatus(ctx, cluster, nil)
	require.NoError(t, err)
	assert.Zero(t, observed.Status.ReadyWorkerReplicas)
	assert.Zero(t, observed.Status.AvailableWorkerReplicas)
	assert.Zero(t, observed.Status.DesiredWorkerReplicas)
	assert.Equal(t, rayv1.Ready, observed.Status.State)

	// Suspension, upgrade, and cleanup use this deletion path. It must retain
	// externally owned Pods and remove this controller's old local capacity.
	pods, err := r.deleteAllPods(ctx, cluster, common.RayClusterAllPodsAssociationOptions(cluster))
	require.NoError(t, err)
	assert.Len(t, pods.Items, 2)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(external), &corev1.Pod{}))
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(previous), &corev1.Pod{})))
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(head), &corev1.Pod{})))
	cluster.Spec.Suspend = new(true)
	observed, err = r.calculateStatus(ctx, cluster, nil)
	require.NoError(t, err)
	assert.Equal(t, rayv1.Suspended, observed.Status.State, "external Pods must not block local suspension")
}

func TestWorkerGroupManagedByChangesPreserveProviderPods(t *testing.T) {
	for _, change := range []string{"removed", "local", "renamed"} {
		t.Run(change, func(t *testing.T) {
			setupTest(t)
			ctx := context.Background()
			cluster := testRayCluster.DeepCopy()
			cluster.UID = "primary-owner"
			cluster.Spec.EnableInTreeAutoscaling = new(false)
			cluster.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
			head := testPods[0].(*corev1.Pod).DeepCopy()
			worker := testPods[1].(*corev1.Pod).DeepCopy()
			worker.Labels[utils.RayNodeTypeLabelKey] = string(rayv1.WorkerNode)
			worker.Status.Phase = corev1.PodRunning
			worker.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			ownTestPods(cluster, head, worker)
			provider := worker.DeepCopy()
			provider.Name, provider.OwnerReferences[0].UID = "provider-worker", "provider-owner"
			switch change {
			case "removed":
				cluster.Spec.WorkerGroupSpecs = nil
			case "local":
				cluster.Spec.WorkerGroupSpecs[0].ManagedBy = nil
			case "renamed":
				cluster.Spec.WorkerGroupSpecs[0].GroupName = "new-group"
			}
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, rayv1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(head, worker, provider, testServices[0].(*corev1.Service)).Build()
			r := &RayClusterReconciler{Client: c, Scheme: scheme, Recorder: &events.FakeRecorder{}}
			observed, err := r.calculateStatus(ctx, cluster, nil)
			require.NoError(t, err)
			assert.EqualValues(t, 1, observed.Status.ReadyWorkerReplicas)
			assert.EqualValues(t, 1, observed.Status.AvailableWorkerReplicas)
			pods, err := r.deleteAllPods(ctx, cluster, common.RayClusterAllPodsAssociationOptions(cluster))
			require.NoError(t, err)
			assert.Len(t, pods.Items, 2)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(provider), &corev1.Pod{}))
			assert.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(head), &corev1.Pod{})))
			assert.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(worker), &corev1.Pod{})))
			cluster.Spec.Suspend = new(true)
			observed, err = r.calculateStatus(ctx, cluster, nil)
			require.NoError(t, err)
			assert.Equal(t, rayv1.Suspended, observed.Status.State)
		})
	}
}

func TestLocalScalingAndTargetedDeletionRespectPodOwnership(t *testing.T) {
	setupTest(t)
	ctx := context.Background()
	cluster := testRayCluster.DeepCopy()
	cluster.UID = "primary-owner"
	cluster.Spec.EnableInTreeAutoscaling = new(false)
	group := &cluster.Spec.WorkerGroupSpecs[0]
	group.Replicas, group.MinReplicas = ptr.To[int32](0), ptr.To[int32](0)
	head := testPods[0].(*corev1.Pod).DeepCopy()
	worker := testPods[1].(*corev1.Pod).DeepCopy()
	worker.Labels[utils.RayNodeTypeLabelKey] = string(rayv1.WorkerNode)
	ownTestPods(cluster, head, worker)
	foreign := worker.DeepCopy()
	foreign.Name, foreign.OwnerReferences[0].UID = "provider-worker", "provider-owner"
	unowned := worker.DeepCopy()
	unowned.Name, unowned.OwnerReferences = "unowned-worker", nil
	otherGroup := worker.DeepCopy()
	otherGroup.Name, otherGroup.Labels[utils.RayNodeGroupLabelKey] = "another-group-worker", "another-group"
	group.ScaleStrategy.WorkersToDelete = []string{foreign.Name, unowned.Name, head.Name, otherGroup.Name}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(head, worker, foreign, unowned, otherGroup).Build()
	r := &RayClusterReconciler{Client: c, Scheme: scheme, Recorder: &events.FakeRecorder{}, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(c)}
	require.NoError(t, r.reconcilePods(ctx, cluster))
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(worker), &corev1.Pod{})))
	for _, pod := range []*corev1.Pod{head, foreign, unowned, otherGroup} {
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}))
	}
	group.Replicas = ptr.To[int32](1)
	for range 2 {
		require.NoError(t, r.reconcilePods(ctx, cluster))
	}
	pods := &corev1.PodList{}
	require.NoError(t, c.List(ctx, pods, common.RayClusterGroupPodsAssociationOptions(cluster, group.GroupName).ToListOptions()...))
	assert.Len(t, ownedPods(cluster, slices.Clone(pods.Items)), 1, "foreign Pods must not satisfy local replicas")
	assert.Len(t, pods.Items, 3)
}

var _ = Describe("RayCluster Pod ownership", func() {
	It("preserves a provider Pod replacing an owned Pod between list and delete", func(ctx SpecContext) {
		cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "deletion-race", Namespace: "default", UID: "primary-owner"}}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "deletion-race-worker", Namespace: cluster.Namespace, Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "rayproject/ray:2.56.0"}}},
		}
		ownTestPods(cluster, pod)
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed()) })
		c, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		intercepted := interceptor.NewClient(c, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
			Expect(c.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
			replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, Labels: pod.Labels}, Spec: pod.Spec}
			provider := cluster.DeepCopy()
			provider.UID = "provider-owner"
			ownTestPods(provider, replacement)
			Expect(c.Create(ctx, replacement)).To(Succeed())
			return c.Delete(ctx, object, opts...)
		}})
		r := &RayClusterReconciler{Client: intercepted}
		_, err = r.deleteAllPods(ctx, cluster, common.RayClusterAllPodsAssociationOptions(cluster))
		Expect(apierrors.IsConflict(err)).To(BeTrue(), "UID precondition must reject the stale deletion: %v", err)
		remaining := &corev1.Pod{}
		Expect(c.Get(ctx, client.ObjectKeyFromObject(pod), remaining)).To(Succeed())
		Expect(remaining.UID).NotTo(Equal(pod.UID))
		Expect(metav1.GetControllerOf(remaining).UID).To(Equal(types.UID("provider-owner")))
	})
})
