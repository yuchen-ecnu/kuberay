package federation

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func freshMember(cluster *rayv1.RayCluster) bool {
	return cluster.Status.ObservedGeneration == cluster.Generation && cluster.Status.LastUpdateTime != nil && time.Since(cluster.Status.LastUpdateTime.Time) < staleAfter
}

func observeMemberWorkers(ctx context.Context, remote client.Client, cluster *rayv1.RayCluster, status *rayv1.FederationMemberStatus, generation int64) error {
	if !freshMember(cluster) {
		condition(&status.Conditions, generation, "WorkersReady", metav1.ConditionUnknown, "ObservationStale", "Waiting for the RayCluster controller's current generation and heartbeat")
		return nil
	}
	pods := &corev1.PodList{}
	if err := remote.List(ctx, pods, common.RayClusterWorkerPodsAssociationOptions(cluster).ToListOptions()...); err != nil {
		return err
	}
	groups := make([]rayv1.FederationWorkerGroupStatus, 0, len(cluster.Spec.WorkerGroupSpecs))
	ready := meta.IsStatusConditionTrue(cluster.Status.Conditions, utils.WorkersReady)
	for _, group := range cluster.Spec.WorkerGroupSpecs {
		observation := rayv1.FederationWorkerGroupStatus{GroupName: group.GroupName, DesiredReplicas: utils.GetWorkerGroupDesiredReplicas(group), ObservedGeneration: cluster.Generation}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if !metav1.IsControlledBy(pod, cluster) || !pod.DeletionTimestamp.IsZero() || pod.Labels[utils.RayNodeGroupLabelKey] != group.GroupName {
				continue
			}
			observation.ObservedReplicas++
			if utils.IsCurrentWorker(cluster, group, pod) && utils.IsRunningAndReady(pod) {
				observation.ReadyReplicas++
			}
			if pod.Status.Phase == corev1.PodPending {
				observation.PendingReplicas++
			}
			if pod.Status.Phase == corev1.PodFailed {
				observation.FailedReplicas++
			}
		}
		ready = ready && observation.ReadyReplicas == observation.DesiredReplicas
		groups = append(groups, observation)
	}
	status.WorkerGroupStatuses, status.ObservedGeneration = groups, generation
	status.LastUpdateTime = cluster.Status.LastUpdateTime.DeepCopy()
	setBooleanCondition(&status.Conditions, generation, "WorkersReady", ready, "MemberWorkersObserved")
	return nil
}
