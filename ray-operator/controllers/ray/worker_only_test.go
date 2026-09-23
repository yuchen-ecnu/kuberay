package ray

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	configapi "github.com/ray-project/kuberay/ray-operator/apis/config/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/expectations"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func workerOnlyFixture(t *testing.T) (*RayClusterReconciler, *rayv1.RayCluster) {
	t.Helper()
	features.SetFeatureGateDuringTest(t, features.RayFederation, true)
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "member", Namespace: "workers", UID: "member-uid", Generation: 1},
		Spec: rayv1.RayClusterSpec{RayVersion: "2.56.0", EnableInTreeAutoscaling: new(false), WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
			GroupName: "cpu", Replicas: ptr.To[int32](2), MinReplicas: ptr.To[int32](0), MaxReplicas: ptr.To[int32](10), NumOfHosts: 1,
			RayStartParams: map[string]string{"address": "head.private:6379"},
			Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-worker", Image: "rayproject/ray:2.56.0"}}}},
		}}},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	scheduler, err := batchscheduler.NewSchedulerManager(context.Background(), configapi.Configuration{}, nil, c)
	require.NoError(t, err)
	return &RayClusterReconciler{
		Client: c, APIReader: c, Scheme: scheme, Recorder: &events.FakeRecorder{}, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(c),
		options: RayClusterReconcilerOptions{BatchSchedulerManager: scheduler},
	}, cluster
}

func reconcileWorkerOnly(t *testing.T, r *RayClusterReconciler, cluster *rayv1.RayCluster, steps int) []corev1.Pod {
	t.Helper()
	for range steps {
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		require.NoError(t, err)
	}
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(cluster), cluster))
	pods := &corev1.PodList{}
	require.NoError(t, r.List(context.Background(), pods, client.InNamespace(cluster.Namespace)))
	return pods.Items
}

func updateWorkerOnly(t *testing.T, r *RayClusterReconciler, cluster *rayv1.RayCluster) {
	t.Helper()
	cluster.Generation++
	require.NoError(t, r.Update(context.Background(), cluster))
}

func TestWorkerOnlyCommandAndLifecycle(t *testing.T) {
	r, cluster := workerOnlyFixture(t)
	group := &cluster.Spec.WorkerGroupSpecs[0]
	group.Template.Annotations = map[string]string{utils.RayOverwriteContainerCmdAnnotationKey: "true"}
	container := &group.Template.Spec.Containers[0]
	container.Command = []string{"/bin/bash", "-c", "--"}
	container.Args = []string{`ulimit -n 65536; exec /bin/bash -c "$KUBERAY_GEN_RAY_START_CMD"`}
	container.Env = []corev1.EnvVar{{Name: "POD_IP", Value: "incorrect"}, {Name: utils.KUBERAY_GEN_RAY_START_CMD, Value: "incorrect"}}
	expectedCommand, expectedArgs := slices.Clone(container.Command), slices.Clone(container.Args)
	updateWorkerOnly(t, r, cluster)
	pods := reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 2)
	for i := range pods {
		pod := &pods[i]
		assert.True(t, metav1.IsControlledBy(pod, cluster))
		assert.Equal(t, "worker", pod.Labels[utils.RayNodeTypeLabelKey])
		assert.Equal(t, expectedCommand, pod.Spec.Containers[0].Command)
		assert.Equal(t, expectedArgs, pod.Spec.Containers[0].Args)
		envs := pod.Spec.Containers[0].Env
		ipIndex := slices.IndexFunc(envs, func(e corev1.EnvVar) bool { return e.Name == "POD_IP" })
		commandIndex := slices.IndexFunc(envs, func(e corev1.EnvVar) bool { return e.Name == utils.KUBERAY_GEN_RAY_START_CMD })
		require.Greater(t, commandIndex, ipIndex)
		require.NotNil(t, envs[ipIndex].ValueFrom)
		assert.Equal(t, "status.podIP", envs[ipIndex].ValueFrom.FieldRef.FieldPath)
		assert.Contains(t, envs[commandIndex].Value, "--address=head.private:6379")
		assert.Contains(t, envs[commandIndex].Value, "--node-ip-address=$(POD_IP)")
		assert.NotContains(t, envs[commandIndex].Value, "--head")
		assert.Contains(t, pod.Spec.InitContainers[0].Args[0], "head.private:6379")
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		require.NoError(t, r.Status().Update(context.Background(), pod))
	}
	services := &corev1.ServiceList{}
	require.NoError(t, r.List(context.Background(), services))
	assert.Empty(t, services.Items)
	pods = reconcileWorkerOnly(t, r, cluster, 1)
	assert.True(t, meta.IsStatusConditionTrue(cluster.Status.Conditions, utils.WorkersReady))
	assert.Nil(t, meta.FindStatusCondition(cluster.Status.Conditions, string(rayv1.HeadPodReady)))
	assert.Empty(t, cluster.Status.Head)
	assert.Equal(t, int32(2), cluster.Status.ReadyWorkerReplicas)
	// Preemption is repaired locally, with no federation permit.
	require.NoError(t, r.Delete(context.Background(), &pods[0]))
	pods = reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 2)
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](1)
	updateWorkerOnly(t, r, cluster)
	pods = reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 1)
	oldName := pods[0].Name
	cluster.Spec.WorkerGroupSpecs[0].RayStartParams["address"] = "replacement.private:6380"
	updateWorkerOnly(t, r, cluster)
	pods = reconcileWorkerOnly(t, r, cluster, 3)
	require.Len(t, pods, 1)
	assert.NotEqual(t, oldName, pods[0].Name)
	assert.False(t, meta.IsStatusConditionTrue(cluster.Status.Conditions, utils.WorkersReady))
	assert.Contains(t, pods[0].Spec.InitContainers[0].Args[0], "replacement.private:6380")
	cluster.Spec.Suspend = new(true)
	updateWorkerOnly(t, r, cluster)
	assert.Empty(t, reconcileWorkerOnly(t, r, cluster, 2))
	assert.True(t, meta.IsStatusConditionTrue(cluster.Status.Conditions, string(rayv1.RayClusterSuspended)))
}

// No federation controller or coordination API is present in this fixture.
func TestFederationMemberScalesAndRepairsLocally(t *testing.T) {
	r, cluster := workerOnlyFixture(t)
	cluster.Labels = map[string]string{utils.FederationOwnerLabel: "frc-uid", utils.FederationMemberLabel: "member-b"}
	updateWorkerOnly(t, r, cluster)
	pods := reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 2)
	cloudID, ok := utils.EnvVarByName(utils.RAY_CLOUD_INSTANCE_ID, pods[0].Spec.Containers[0].Env)
	require.True(t, ok)
	assert.Equal(t, "member-b/$(KUBERAY_WORKER_POD_NAME)", cloudID.Value)
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](3)
	updateWorkerOnly(t, r, cluster)
	pods = reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 3, "spec changes take effect without federation authorization")
	// A local controller restart and worker preemption do not require a central heartbeat.
	r.rayClusterScaleExpectation = expectations.NewRayClusterScaleExpectation(r.Client)
	require.NoError(t, r.Delete(context.Background(), &pods[0]))
	pods = reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 3)
	// Failed Pods are replaced too, not retained by a missing provisioning permit.
	failed := pods[0].DeepCopy()
	failed.Status.Phase = corev1.PodFailed
	require.NoError(t, r.Status().Update(context.Background(), failed))
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	require.ErrorContains(t, err, "unhealthy worker")
	pods = reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 3)
	assert.False(t, slices.ContainsFunc(pods, func(pod corev1.Pod) bool { return pod.Name == failed.Name }))
	cluster.Spec.Suspend = new(true)
	updateWorkerOnly(t, r, cluster)
	assert.Empty(t, reconcileWorkerOnly(t, r, cluster, 2))
}

func TestWorkerOnlyDropsObsoleteProvisioningCondition(t *testing.T) {
	r, cluster := workerOnlyFixture(t)
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "WorkerProvisioningAllowed", Status: metav1.ConditionFalse, Reason: "ProvisioningPermitObserved"})
	require.NoError(t, r.Status().Update(context.Background(), cluster))
	require.Len(t, reconcileWorkerOnly(t, r, cluster, 2), 2)
	assert.Nil(t, meta.FindStatusCondition(cluster.Status.Conditions, "WorkerProvisioningAllowed"))
}

func TestWorkerOnlyDeletionIsolation(t *testing.T) {
	r, cluster := workerOnlyFixture(t)
	pods := reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 2)
	foreign := pods[0].DeepCopy()
	foreign.Name, foreign.ResourceVersion, foreign.UID = "foreign-worker", "", "foreign-worker"
	foreign.OwnerReferences[0].UID = "someone-else"
	require.NoError(t, r.Create(context.Background(), foreign))
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](1)
	cluster.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{pods[0].Name, foreign.Name}
	updateWorkerOnly(t, r, cluster)
	reconcileWorkerOnly(t, r, cluster, 3)
	assert.True(t, apierrors.IsNotFound(r.Get(context.Background(), client.ObjectKeyFromObject(&pods[0]), &corev1.Pod{})))
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(&pods[1]), &corev1.Pod{}))
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(foreign), &corev1.Pod{}))
	assert.Equal(t, int32(1), cluster.Status.DesiredWorkerReplicas)
}

func TestWorkerOnlyDownscaleAndGroupRemoval(t *testing.T) {
	r, cluster := workerOnlyFixture(t)
	second := cluster.Spec.WorkerGroupSpecs[0].DeepCopy()
	second.GroupName, second.Replicas = "second", ptr.To[int32](1)
	cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, *second)
	updateWorkerOnly(t, r, cluster)
	pods := reconcileWorkerOnly(t, r, cluster, 2)
	require.Len(t, pods, 3)
	var healthy string
	for i := range pods {
		if pods[i].Labels[utils.RayNodeGroupLabelKey] == "cpu" {
			healthy = pods[i].Name
			pods[i].Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			require.NoError(t, r.Status().Update(context.Background(), &pods[i]))
			break
		}
	}
	// Federation-owned members downscale and remove groups locally, preserving the ready worker.
	cluster.Labels = map[string]string{utils.FederationOwnerLabel: "frc-uid"}
	cluster.Spec.WorkerGroupSpecs = cluster.Spec.WorkerGroupSpecs[:1]
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](1)
	updateWorkerOnly(t, r, cluster)
	pods = reconcileWorkerOnly(t, r, cluster, 3)
	require.Len(t, pods, 1)
	assert.Equal(t, healthy, pods[0].Name)
	cluster.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{healthy}
	updateWorkerOnly(t, r, cluster)
	pods = reconcileWorkerOnly(t, r, cluster, 3)
	require.Len(t, pods, 1)
	assert.NotEqual(t, healthy, pods[0].Name, "targeted deletion replaces the worker to preserve desired capacity")
}

func TestWorkerOnlyInvalidConfigurationCreatesNoPods(t *testing.T) {
	for _, mode := range []string{"inconsistent-address", "batch-scheduler"} {
		t.Run(mode, func(t *testing.T) {
			r, cluster := workerOnlyFixture(t)
			if mode == "inconsistent-address" {
				other := cluster.Spec.WorkerGroupSpecs[0].DeepCopy()
				other.GroupName, other.RayStartParams["address"] = "other", "different-head:6379"
				cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, *other)
			} else {
				scheduler, err := batchscheduler.NewSchedulerManager(context.Background(), configapi.Configuration{BatchScheduler: "yunikorn"}, nil, r.Client)
				require.NoError(t, err)
				r.options.BatchSchedulerManager = scheduler
			}
			updateWorkerOnly(t, r, cluster)
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			if mode == "batch-scheduler" {
				require.ErrorContains(t, err, "batch schedulers")
			} else {
				require.NoError(t, err, "invalid specs emit a validation event without retries")
			}
			pods := &corev1.PodList{}
			require.NoError(t, r.List(context.Background(), pods))
			assert.Empty(t, pods.Items)
		})
	}
}

func TestWorkerOnlyRayJobSelection(t *testing.T) {
	r, cluster := workerOnlyFixture(t)
	job := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: cluster.Namespace},
		Spec:       rayv1.RayJobSpec{ClusterSelector: map[string]string{utils.RayClusterLabelKey: cluster.Name}},
		Status:     rayv1.RayJobStatus{RayClusterName: cluster.Name},
	}
	jobs := &RayJobReconciler{Client: r.Client, Scheme: r.Scheme, Recorder: r.Recorder}
	selected, err := jobs.getOrCreateRayClusterInstance(context.Background(), job)
	require.ErrorContains(t, err, "requires a RayCluster with a head")
	assert.Nil(t, selected)
}
