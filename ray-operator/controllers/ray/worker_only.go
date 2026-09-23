package ray

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// reconcileWorkerOnlyCluster uses the same worker reconciler as a full cluster.
// It has no head resource lifecycle, GCS cleanup or autoscaler to reconcile.
func (r *RayClusterReconciler) reconcileWorkerOnlyCluster(ctx context.Context, cluster *rayv1.RayCluster) (ctrl.Result, error) {
	if !cluster.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if r.options.BatchSchedulerManager != nil {
		scheduler, err := r.options.BatchSchedulerManager.GetScheduler()
		if err != nil {
			return ctrl.Result{}, err
		}
		if scheduler.Name() != schedulerinterface.GetDefaultPluginName() {
			return ctrl.Result{}, fmt.Errorf("workers-only RayClusters do not yet support batch schedulers")
		}
	}
	base := cluster.DeepCopy()
	err := r.pruneWorkerOnlyPods(ctx, cluster)
	if err == nil && !ptr.Deref(cluster.Spec.Suspend, false) {
		err = r.reconcileWorkerPods(ctx, cluster)
	}
	pods := &corev1.PodList{}
	if listErr := r.List(ctx, pods, common.RayClusterWorkerPodsAssociationOptions(cluster).ToListOptions()...); listErr != nil {
		return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, listErr
	}
	pods.Items = slices.DeleteFunc(pods.Items, func(p corev1.Pod) bool { return !metav1.IsControlledBy(&p, cluster) })
	calculateWorkerOnlyStatus(cluster, pods.Items, err)
	if !reflect.DeepEqual(base.Status, cluster.Status) {
		if patchErr := r.Status().Patch(ctx, cluster, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); patchErr != nil {
			return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, patchErr
		}
	}
	return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
}

func (r *RayClusterReconciler) pruneWorkerOnlyPods(ctx context.Context, cluster *rayv1.RayCluster) error {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, common.RayClusterWorkerPodsAssociationOptions(cluster).ToListOptions()...); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, cluster) || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		keep := !ptr.Deref(cluster.Spec.Suspend, false) && slices.ContainsFunc(cluster.Spec.WorkerGroupSpecs, func(g rayv1.WorkerGroupSpec) bool {
			return g.GroupName == pod.Labels[utils.RayNodeGroupLabelKey] && !ptr.Deref(g.Suspend, false)
		})
		if !keep {
			if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func calculateWorkerOnlyStatus(cluster *rayv1.RayCluster, pods []corev1.Pod, reconcileErr error) {
	status := &cluster.Status
	status.Head, status.Endpoints = rayv1.HeadInfo{}, nil
	if reconcileErr == nil {
		status.ObservedGeneration = cluster.Generation
	}
	status.ReadyWorkerReplicas, status.AvailableWorkerReplicas = 0, 0
	status.DesiredWorkerReplicas = utils.CalculateDesiredReplicas(cluster)
	status.MinWorkerReplicas, status.MaxWorkerReplicas = utils.CalculateMinReplicas(cluster), utils.CalculateMaxReplicas(cluster)
	ready := reconcileErr == nil
	for _, group := range cluster.Spec.WorkerGroupSpecs {
		groupReady := int32(0)
		for i := range pods {
			pod := &pods[i]
			if !utils.IsCurrentWorker(cluster, group, pod) {
				continue
			}
			if pod.Status.Phase == corev1.PodRunning {
				status.AvailableWorkerReplicas++
			}
			if utils.IsRunningAndReady(pod) {
				groupReady++
			}
		}
		status.ReadyWorkerReplicas += groupReady
		ready = ready && groupReady == utils.GetWorkerGroupDesiredReplicas(group)
	}
	resources := utils.CalculateDesiredResources(cluster)
	status.DesiredCPU, status.DesiredMemory = resources[corev1.ResourceCPU], resources[corev1.ResourceMemory]
	status.DesiredGPU, status.DesiredTPU = sumGPUs(resources), resources[corev1.ResourceName("google.com/tpu")]
	setCondition := func(kind string, value bool, reason string) {
		state := metav1.ConditionFalse
		if value {
			state = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{Type: kind, Status: state, Reason: reason, Message: reason, ObservedGeneration: cluster.Generation})
	}
	meta.RemoveStatusCondition(&status.Conditions, string(rayv1.HeadPodReady))
	meta.RemoveStatusCondition(&status.Conditions, "WorkerProvisioningAllowed")
	suspended := ptr.Deref(cluster.Spec.Suspend, false)
	setCondition(string(rayv1.RayClusterSuspending), suspended && len(pods) > 0, "WorkerSuspensionObserved")
	setCondition(string(rayv1.RayClusterSuspended), suspended && len(pods) == 0, "WorkerSuspensionObserved")
	ready = ready && !suspended
	setCondition(utils.WorkersReady, ready, "WorkerCapacityObserved")
	if suspended || !meta.IsStatusConditionTrue(status.Conditions, string(rayv1.RayClusterProvisioned)) {
		setCondition(string(rayv1.RayClusterProvisioned), ready && status.DesiredWorkerReplicas > 0, "WorkerProvisioningObserved")
	}
	status.State, status.Reason = "", ""
	if ready {
		status.State = rayv1.Ready
	}
	if suspended && len(pods) == 0 {
		status.State = rayv1.Suspended
	}
	if reconcileErr != nil {
		status.State, status.Reason = rayv1.Failed, reconcileErr.Error()
	}
	if reconcileErr == nil && (status.LastUpdateTime == nil || time.Since(status.LastUpdateTime.Time) >= 30*time.Second) {
		status.LastUpdateTime = new(metav1.Now())
	}
}
