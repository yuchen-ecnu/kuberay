package federation

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"reflect"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

type memberClientProvider interface {
	Get(context.Context, string, string) (client.Client, error)
}

// FederatedReconciler manages RayClusters across member APIs.
// Business worker Pods remain owned and reconciled by the member RayCluster controller.
type FederatedReconciler struct {
	client.Client
	Reader  client.Reader
	Scheme  *runtime.Scheme
	Members memberClientProvider
}

// +kubebuilder:rbac:groups=ray.io,resources=federatedrayclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=federatedrayclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=federatedrayclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *FederatedReconciler) SetupWithManager(mgr ctrl.Manager, concurrency int) error {
	r.Client, r.Reader, r.Scheme = mgr.GetClient(), mgr.GetAPIReader(), mgr.GetScheme()
	r.Members = &MemberClients{Reader: r.Reader, Scheme: r.Scheme}
	return ctrl.NewControllerManagedBy(mgr).For(&rayv1.FederatedRayCluster{}).
		Owns(&rayv1.RayCluster{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretRequests)).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrency}).Complete(r)
}

func (r *FederatedReconciler) secretRequests(ctx context.Context, object client.Object) []reconcile.Request {
	list := &rayv1.FederatedRayClusterList{}
	if err := r.List(ctx, list, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for _, frc := range list.Items {
		usesSecret := false
		for _, member := range frc.Spec.MemberClusters {
			if member.KubeconfigSecretRef != nil && member.KubeconfigSecretRef.Name == object.GetName() {
				usesSecret = true
			}
		}
		for _, member := range frc.Status.MemberClusterStatuses {
			if member.KubeconfigSecretRef != nil && member.KubeconfigSecretRef.Name == object.GetName() {
				usesSecret = true
			}
		}
		if usesSecret {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&frc)})
		}
	}
	return requests
}

func (r *FederatedReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	frc := &rayv1.FederatedRayCluster{}
	if err := r.Reader.Get(ctx, request.NamespacedName, frc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !frc.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, frc)
	}
	if !controllerutil.ContainsFinalizer(frc, Finalizer) {
		base := frc.DeepCopy()
		controllerutil.AddFinalizer(frc, Finalizer)
		return ctrl.Result{RequeueAfter: pollInterval}, r.Patch(ctx, frc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	base := frc.DeepCopy()
	meta.RemoveStatusCondition(&frc.Status.Conditions, "MemberDataPlaneReady")
	for i := range frc.Status.MemberClusterStatuses {
		meta.RemoveStatusCondition(&frc.Status.MemberClusterStatuses[i].Conditions, "MemberDataPlaneReady")
	}
	normalizeNetworking(&frc.Spec.Networking)
	err := ValidateFederation(frc)
	if err == nil {
		err = r.reconcile(ctx, frc)
	}
	if snapshotErr := r.publishAutoscalerSnapshot(ctx, frc, err); snapshotErr != nil && err == nil {
		err = snapshotErr
	}
	if err != nil {
		condition(&frc.Status.Conditions, frc.Generation, "Ready", metav1.ConditionFalse, "ReconcileFailed", err.Error())
	}
	frc.Status.ObservedGeneration = frc.Generation
	if !reflect.DeepEqual(base.Status, frc.Status) {
		if patchErr := r.Status().Patch(ctx, frc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); patchErr != nil {
			return ctrl.Result{}, patchErr
		}
	}
	return ctrl.Result{RequeueAfter: pollInterval}, err
}

func destinationEqual(status rayv1.FederationMemberStatus, member rayv1.FederationMemberCluster) bool {
	return status.Name == member.Name && status.Namespace == member.Namespace && (status.KubeconfigSecretRef == nil) == (member.KubeconfigSecretRef == nil)
}

// syncInventory persists destinations before creating children. Removed or moved
// members are cleaned up before their inventory entry can disappear.
func (r *FederatedReconciler) syncInventory(ctx context.Context, frc *rayv1.FederatedRayCluster) (bool, error) {
	ready := true
	var failures []error
	var kept []rayv1.FederationMemberStatus
	for _, previous := range frc.Status.MemberClusterStatuses {
		member := *previous.DeepCopy()
		changed, err := r.prepareMemberDestination(ctx, frc, &member)
		if err != nil {
			failures = append(failures, fmt.Errorf("member %s: %w", member.Name, err))
		}
		desired := slices.ContainsFunc(frc.Spec.MemberClusters, func(m rayv1.FederationMemberCluster) bool { return destinationEqual(member, m) })
		if !desired && err == nil && !changed {
			var done bool
			done, err = r.cleanupMember(ctx, frc, member)
			if err != nil {
				failures = append(failures, err)
			}
			if done {
				ready = false
				continue
			}
		}
		ready = ready && desired && !changed && err == nil
		kept = append(kept, member)
	}
	frc.Status.MemberClusterStatuses = kept
	for _, member := range frc.Spec.MemberClusters {
		// A move must release its old destination before binding the new one.
		if slices.ContainsFunc(kept, func(s rayv1.FederationMemberStatus) bool { return s.Name == member.Name }) {
			continue
		}
		ready = false
		status := rayv1.FederationMemberStatus{Name: member.Name, Namespace: member.Namespace, KubeconfigSecretRef: member.KubeconfigSecretRef.DeepCopy()}
		if member.KubeconfigSecretRef != nil {
			remote, uid, err := r.memberCluster(ctx, frc.Namespace, member.KubeconfigSecretRef.Name)
			if err != nil {
				failures = append(failures, fmt.Errorf("member %s: %w", member.Name, err))
				continue
			}
			status.ClusterUID = uid
			status.RayClusterName, err = availableMemberName(ctx, remote, frc, member)
			if err != nil {
				failures = append(failures, fmt.Errorf("member %s: %w", member.Name, err))
				continue
			}
		}
		frc.Status.MemberClusterStatuses = append(frc.Status.MemberClusterStatuses, status)
	}
	return ready && len(failures) == 0, errors.Join(failures...)
}

// prepareMemberDestination performs only reads and updates the in-memory
// checkpoint. The caller must persist changes before writing to that member.
// This is shared by normal reconciliation and finalization.
func (r *FederatedReconciler) prepareMemberDestination(ctx context.Context, frc *rayv1.FederatedRayCluster, status *rayv1.FederationMemberStatus) (bool, error) {
	previous := status.DeepCopy()
	for _, desired := range frc.Spec.MemberClusters {
		if desired.Name != status.Name {
			continue
		}
		if (desired.KubeconfigSecretRef == nil) != (status.KubeconfigSecretRef == nil) {
			return false, fmt.Errorf("member %s management cannot change in place; remove the member before re-adding it", status.Name)
		}
		if desired.Namespace != status.Namespace {
			continue
		}
		if !reflect.DeepEqual(desired.KubeconfigSecretRef, status.KubeconfigSecretRef) {
			candidate := *status.DeepCopy()
			candidate.KubeconfigSecretRef = desired.KubeconfigSecretRef.DeepCopy()
			if candidate.ClusterUID == "" {
				if err := r.bindLegacyMember(ctx, frc, &candidate); err != nil {
					return false, err
				}
			} else if _, err := r.boundMemberClient(ctx, frc.Namespace, candidate); err != nil {
				return false, err
			}
			*status = candidate
		}
		break
	}
	if status.KubeconfigSecretRef != nil && status.ClusterUID == "" {
		if err := r.bindLegacyMember(ctx, frc, status); err != nil {
			return false, err
		}
	}
	if status.KubeconfigSecretRef != nil && status.RayClusterName == "" {
		if err := r.bindMemberName(ctx, frc, status); err != nil {
			return false, err
		}
	}
	return !reflect.DeepEqual(previous, status), nil
}

func (r *FederatedReconciler) reconcile(ctx context.Context, frc *rayv1.FederatedRayCluster) error {
	if err := r.ValidatePrimaryUpdate(ctx, frc); err != nil {
		return err
	}
	beforeInventory := frc.Status.DeepCopy()
	ready, inventoryErr := r.syncInventory(ctx, frc)
	if !reflect.DeepEqual(beforeInventory.MemberClusterStatuses, frc.Status.MemberClusterStatuses) {
		condition(&frc.Status.Conditions, frc.Generation, "Ready", metav1.ConditionFalse, "ReconcilingTopology", "Persisting member destinations before remote writes")
		return inventoryErr
	}
	if err := r.ensureAutoscalerResources(ctx, frc); err != nil {
		return err
	}
	primary, err := r.ensurePrimary(ctx, frc)
	if err != nil {
		return err
	}
	headReady := primary.Status.ObservedGeneration == primary.Generation && meta.IsStatusConditionTrue(primary.Status.Conditions, string(rayv1.HeadPodReady))
	workersReady := primary.Status.ReadyWorkerReplicas >= utils.CalculateDesiredReplicas(primary) && headReady
	controlReady := ready
	for _, member := range frc.Spec.MemberClusters {
		statusIndex := slices.IndexFunc(frc.Status.MemberClusterStatuses, func(status rayv1.FederationMemberStatus) bool { return destinationEqual(status, member) })
		if statusIndex < 0 {
			controlReady, workersReady = false, false
			continue // An unbound member cannot stop already bound members.
		}
		status := &frc.Status.MemberClusterStatuses[statusIndex]
		if !reflect.DeepEqual(status.KubeconfigSecretRef, member.KubeconfigSecretRef) {
			controlReady, workersReady = false, false
			continue // Wait for this member's credential checkpoint.
		}
		if member.KubeconfigSecretRef == nil {
			markManualMember(status, frc.Generation)
			continue
		}
		// Each member converges independently. Observation failures affect readiness,
		// but never gate another member's desired state or local Pod lifecycle.
		remote, err := r.boundMemberClient(ctx, frc.Namespace, *status)
		if err == nil {
			var cluster *rayv1.RayCluster
			desired := desiredMember(frc, primary, member)
			desired.Name = status.RayClusterName
			cluster, err = syncMember(ctx, remote, frc, desired)
			if err == nil {
				status.RayClusterUID = cluster.UID
				err = observeMemberWorkers(ctx, remote, cluster, status, frc.Generation)
			}
		}
		if err != nil {
			controlReady, workersReady = false, false
			condition(&status.Conditions, frc.Generation, "MemberControlPlaneReachable", metav1.ConditionFalse, "MemberAPIUnavailable", err.Error())
			condition(&status.Conditions, frc.Generation, "WorkersReady", metav1.ConditionUnknown, "ObservationUnavailable", "Previous capacity is retained; current member state is unknown")
			continue
		}
		condition(&status.Conditions, frc.Generation, "MemberControlPlaneReachable", metav1.ConditionTrue, "MemberAPIObserved", "Member API observation succeeded")
		workersReady = workersReady && meta.IsStatusConditionTrue(status.Conditions, "WorkersReady")
	}

	// Aggregate readiness covers the primary and managed members. Manual members
	// report management mode only and do not participate in health observations.
	setBooleanCondition(&frc.Status.Conditions, frc.Generation, "MemberControlPlaneReachable", controlReady, "MemberAPIsObserved")
	setBooleanCondition(&frc.Status.Conditions, frc.Generation, "HeadEndpointReady", headReady, "PrimaryHeadReady")
	setBooleanCondition(&frc.Status.Conditions, frc.Generation, "WorkersReady", workersReady, "WorkerCapacityObserved")
	setBooleanCondition(&frc.Status.Conditions, frc.Generation, "Ready", controlReady && headReady && workersReady, "FederationReconciled")
	if headReady {
		endpoint := frc.Spec.Networking.HeadEndpoint
		frc.Status.HeadEndpoint = &endpoint
	} else {
		frc.Status.HeadEndpoint = nil
	}
	return inventoryErr
}

func markManualMember(status *rayv1.FederationMemberStatus, generation int64) {
	status.ObservedGeneration = generation
	status.RayClusterName = ""
	status.RayClusterUID = ""
	status.LastUpdateTime = nil
	status.WorkerGroupStatuses = nil
	status.Conditions = slices.DeleteFunc(status.Conditions, func(c metav1.Condition) bool { return c.Type != "ManuallyManaged" })
	condition(&status.Conditions, generation, "ManuallyManaged", metav1.ConditionTrue, "ManuallyManaged",
		"No member credential is configured; member health is excluded from federation readiness")
}

func setBooleanCondition(conditions *[]metav1.Condition, generation int64, kind string, ready bool, reason string) {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	condition(conditions, generation, kind, status, reason, kind)
}

func (r *FederatedReconciler) ensurePrimary(ctx context.Context, frc *rayv1.FederatedRayCluster) (*rayv1.RayCluster, error) {
	if err := ValidateFederation(frc); err != nil {
		return nil, err
	}
	primary := &rayv1.RayCluster{}
	err := r.Reader.Get(ctx, client.ObjectKeyFromObject(frc), primary)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	exists := err == nil
	if exists && !metav1.IsControlledBy(primary, frc) {
		return nil, fmt.Errorf("primary RayCluster already exists with another owner")
	}
	base := primary.DeepCopy()
	primary.Name, primary.Namespace = frc.Name, frc.Namespace
	spec := desiredPrimarySpec(frc)
	for i := range spec.WorkerGroupSpecs {
		group := &spec.WorkerGroupSpecs[i]
		normalizeGroup(group)
		// The PRC owns runtime intent in both modes. FRC values only bootstrap
		// newly introduced groups; preserve existing replicas and deletions.
		for _, old := range primary.Spec.WorkerGroupSpecs {
			if old.GroupName == group.GroupName && ptr.Deref(old.ManagedBy, rayv1.WorkerGroupManagedByRayCluster) == ptr.Deref(group.ManagedBy, rayv1.WorkerGroupManagedByRayCluster) {
				group.Replicas = nil
				if old.Replicas != nil {
					group.Replicas = new(*old.Replicas)
				}
				group.ScaleStrategy = *old.ScaleStrategy.DeepCopy()
				break
			}
		}
	}
	// Keep removed local groups suspended until the primary controller has drained
	// their Pods. All Pod writes remain with that controller.
	for _, old := range primary.Spec.WorkerGroupSpecs {
		if old.IsExternallyManaged() || slices.ContainsFunc(spec.WorkerGroupSpecs, func(group rayv1.WorkerGroupSpec) bool { return group.GroupName == old.GroupName }) {
			continue
		}
		pods := &corev1.PodList{}
		if err := r.Reader.List(ctx, pods, common.RayClusterGroupPodsAssociationOptions(primary, old.GroupName).ToListOptions()...); err != nil {
			return nil, err
		}
		if slices.ContainsFunc(pods.Items, func(pod corev1.Pod) bool { return metav1.IsControlledBy(&pod, primary) }) {
			old = *old.DeepCopy()
			old.Suspend, old.Replicas, old.MinReplicas, old.MaxReplicas = new(true), ptr.To[int32](0), ptr.To[int32](0), ptr.To[int32](0)
			if utils.IsAutoscalingEnabled(&spec) {
				old.Suspend = nil
			}
			spec.WorkerGroupSpecs = append(spec.WorkerGroupSpecs, old)
		}
	}
	repair := false
	if exists {
		repair, err = r.canRepairInvalidPrimary(ctx, primary)
		if err != nil {
			return nil, err
		}
	}
	if err := recordPrimaryConfiguration(primary, spec, exists && !repair); err != nil {
		return nil, err
	}
	configureFederationAutoscaler(&spec, autoscalerConfigMapName(frc))
	setAutoscalerMemberMapping(primary, frc)
	primary.Spec = spec
	if err := ctrl.SetControllerReference(frc, primary, r.Scheme); err != nil {
		return nil, err
	}
	if !exists {
		return primary, r.Create(ctx, primary)
	}
	if !reflect.DeepEqual(base.Spec, spec) || !reflect.DeepEqual(base.Annotations, primary.Annotations) {
		return primary, r.Patch(ctx, primary, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	return primary, nil
}

func desiredMember(frc *rayv1.FederatedRayCluster, primary *rayv1.RayCluster, member rayv1.FederationMemberCluster) *rayv1.RayCluster {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: memberRayClusterName(frc.Name, member.Name), Namespace: member.Namespace,
			Labels: map[string]string{OwnerLabel: string(frc.UID), utils.FederationMemberLabel: member.Name, "ray.io/federation-name": frc.Name, "ray.io/federation-namespace": frc.Namespace},
		},
		Spec: rayv1.RayClusterSpec{RayVersion: primary.Spec.RayVersion, EnableInTreeAutoscaling: new(false), AuthOptions: &rayv1.AuthOptions{Mode: rayv1.AuthModeDisabled}},
	}
	endpoint := frc.Spec.Networking.HeadEndpoint
	address := net.JoinHostPort(endpoint.Address, strconv.Itoa(int(endpoint.GCSPort)))
	// Membership belongs to the FRC topology. The primary RayCluster carries only
	// execution responsibility and replica intent, joined by federation-wide group name.
	groups := make(map[string]bool, len(member.WorkerGroups))
	for _, group := range member.WorkerGroups {
		groups[group.GroupName] = true
	}
	for _, group := range primary.Spec.WorkerGroupSpecs {
		if group.IsExternallyManaged() && groups[group.GroupName] {
			group = *group.DeepCopy()
			group.ManagedBy = nil
			// Scheduling policy belongs to the global autoscaler on the primary.
			// Members execute replica intent without running a second autoscaler.
			if utils.IsAutoscalingEnabled(&primary.Spec) {
				group.IdleTimeoutSeconds, group.Priority = nil, ptr.To[int32](0)
				// Bounds are scheduling policy, too. Clamping replicas at the member
				// would bypass the global autoscaler's drain and exact Pod selection
				// when the user lowers maxReplicas or raises minReplicas on the FRC.
				group.MinReplicas, group.MaxReplicas = ptr.To[int32](0), ptr.To[int32](2147483647)
			}
			if group.RayStartParams == nil {
				group.RayStartParams = map[string]string{}
			}
			group.RayStartParams["address"] = address
			cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, group)
		}
	}
	return cluster
}

func syncMember(ctx context.Context, remote client.Client, frc *rayv1.FederatedRayCluster, desired *rayv1.RayCluster) (*rayv1.RayCluster, error) {
	existing := &rayv1.RayCluster{}
	if err := remote.Get(ctx, client.ObjectKeyFromObject(desired), existing); apierrors.IsNotFound(err) {
		return desired, remote.Create(ctx, desired)
	} else if err != nil {
		return nil, err
	}
	if existing.Labels[OwnerLabel] != string(frc.UID) {
		return nil, fmt.Errorf("member RayCluster %s/%s has a different federation owner", desired.Namespace, desired.Name)
	}
	if existing.Spec.HeadGroupSpec != nil || existing.Labels[utils.FederationMemberLabel] != desired.Labels[utils.FederationMemberLabel] {
		return nil, fmt.Errorf("member destination is already bound to %s", existing.Labels[utils.FederationMemberLabel])
	}
	if !existing.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("member RayCluster is still being deleted")
	}
	base := existing.DeepCopy()
	existing.Spec = desired.Spec
	maps.Copy(existing.Labels, desired.Labels)
	if !reflect.DeepEqual(base.Spec, existing.Spec) || !reflect.DeepEqual(base.Labels, existing.Labels) {
		if err := remote.Patch(ctx, existing, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

func (r *FederatedReconciler) cleanupMember(ctx context.Context, frc *rayv1.FederatedRayCluster, member rayv1.FederationMemberStatus) (bool, error) {
	if member.KubeconfigSecretRef == nil {
		return true, nil
	}
	if member.RayClusterName == "" {
		return false, fmt.Errorf("member %s has no persisted RayCluster name", member.Name)
	}
	remote, err := r.boundMemberClient(ctx, frc.Namespace, member)
	if err != nil {
		return false, err
	}
	mrc := &rayv1.RayCluster{}
	if err := remote.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: member.RayClusterName}, mrc); apierrors.IsNotFound(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	// Releasing the remote object and persisting local inventory cannot be atomic.
	// A release receipt makes retries safe after a lost response or status conflict.
	if frc.Spec.MemberCleanupPolicy == "Orphan" && mrc.Labels[OwnerLabel] == "" &&
		mrc.Labels[orphanedByLabel] == string(frc.UID) && mrc.Spec.HeadGroupSpec == nil &&
		mrc.Labels[utils.FederationMemberLabel] == member.Name {
		return true, nil
	}
	if mrc.Labels[OwnerLabel] != string(frc.UID) {
		if (member.RayClusterUID == "" && member.LastUpdateTime == nil) || (member.RayClusterUID != "" && member.RayClusterUID != mrc.UID) {
			// A foreign collision was never ours, or the recorded object is gone.
			// Do not delete the replacement or retain cleanup responsibility for it.
			return true, nil
		}
		return false, fmt.Errorf("refusing to clean up member %s: federation owner does not match", member.Name)
	}
	if mrc.Spec.HeadGroupSpec != nil {
		return false, fmt.Errorf("refusing to clean up a different member binding")
	}
	if binding := mrc.Labels[utils.FederationMemberLabel]; binding != member.Name {
		// A failed duplicate destination never owned this object. Keep the actual
		// owner's inventory entry responsible for cleanup and forget only the
		// duplicate entry, so reverting the invalid spec can make progress.
		tracked := slices.ContainsFunc(frc.Status.MemberClusterStatuses, func(candidate rayv1.FederationMemberStatus) bool {
			return candidate.Name == binding &&
				candidate.Namespace == member.Namespace &&
				candidate.RayClusterName == member.RayClusterName &&
				candidate.ClusterUID != "" &&
				candidate.ClusterUID == member.ClusterUID &&
				candidate.KubeconfigSecretRef != nil
		})
		if tracked {
			return true, nil
		}
		return false, fmt.Errorf("refusing to clean up a different member binding")
	}
	if frc.Spec.MemberCleanupPolicy == "Orphan" {
		base := mrc.DeepCopy()
		mrc.Labels[orphanedByLabel] = string(frc.UID)
		delete(mrc.Labels, OwnerLabel)
		delete(mrc.Labels, "ray.io/federation-name")
		delete(mrc.Labels, "ray.io/federation-namespace")
		return true, remote.Patch(ctx, mrc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	if mrc.DeletionTimestamp.IsZero() {
		if err := remote.Delete(ctx, mrc, client.Preconditions{UID: &mrc.UID, ResourceVersion: &mrc.ResourceVersion}, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
			return false, client.IgnoreNotFound(err)
		}
	}
	return false, nil
}

func (r *FederatedReconciler) finalize(ctx context.Context, frc *rayv1.FederatedRayCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(frc, Finalizer) {
		return ctrl.Result{}, nil
	}
	base := frc.DeepCopy()
	var remaining []rayv1.FederationMemberStatus
	var failures []error
	for _, previous := range frc.Status.MemberClusterStatuses {
		member := *previous.DeepCopy()
		changed, err := r.prepareMemberDestination(ctx, frc, &member)
		done := false
		if !changed {
			// A rejected replacement credential need not prevent cleanup through
			// the previously verified credential, if it still works.
			var cleanupErr error
			done, cleanupErr = r.cleanupMember(ctx, frc, member)
			err = errors.Join(err, cleanupErr)
		}
		if err != nil && !done {
			failures = append(failures, fmt.Errorf("member %s: %w", member.Name, err))
		}
		if !done {
			remaining = append(remaining, member)
		}
	}
	frc.Status.MemberClusterStatuses = remaining
	if len(remaining) > 0 {
		message := "Waiting for member cleanup or credential checkpoints"
		if err := errors.Join(failures...); err != nil {
			message = err.Error()
		}
		condition(&frc.Status.Conditions, frc.Generation, "Ready", metav1.ConditionFalse, "DeletionBlocked", message)
	}
	if !reflect.DeepEqual(base.Status, frc.Status) {
		// Persist cleared destinations and rotated credentials before the next
		// operation. A crash/retry cannot repeat or lose cleanup responsibility.
		return ctrl.Result{RequeueAfter: pollInterval}, r.Status().Patch(ctx, frc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}
	if len(remaining) != 0 {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	controllerutil.RemoveFinalizer(frc, Finalizer)
	return ctrl.Result{}, r.Patch(ctx, frc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}
