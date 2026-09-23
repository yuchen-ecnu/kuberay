package federation

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// The adapter runs in the existing Ray image. Only this controller reads member
// credentials; the autoscaler can read a single, namespaced observation ConfigMap.
//
//go:embed autoscaler/federation_autoscaler.py
var federationAutoscalerSource string

const (
	autoscalerSnapshotKey    = "snapshot.json"
	autoscalerSnapshotMaxAge = 30 * time.Second
)

type autoscalerSnapshot struct {
	SchemaVersion     int          `json:"schemaVersion"`
	AdapterRevision   string       `json:"adapterRevision"`
	PrimaryUID        string       `json:"primaryUID"`
	PrimaryGeneration int64        `json:"primaryGeneration"`
	ObservedAt        time.Time    `json:"observedAt"`
	Error             string       `json:"error,omitempty"`
	Pods              []corev1.Pod `json:"pods"`
}

func autoscalerConfigMapName(frc *rayv1.FederatedRayCluster) string {
	return memberRayClusterName(frc.Name, "autoscaler")
}

// Workers-only RayClusters register member-name/Pod-name with GCS. Keep the
// mapping beside the primary's groups so the adapter can translate provider IDs
// to Kubernetes Pod names even before an empty group has provisioned a Pod.
func setAutoscalerMemberMapping(primary *rayv1.RayCluster, frc *rayv1.FederatedRayCluster) {
	if !ptr.Deref(frc.Spec.PrimaryCluster.EnableInTreeAutoscaling, false) {
		return
	}
	members := map[string]string{}
	for _, member := range frc.Spec.MemberClusters {
		if member.KubeconfigSecretRef != nil {
			for _, group := range member.WorkerGroups {
				members[group.GroupName] = member.Name
			}
		}
	}
	data, _ := json.Marshal(members)
	if primary.Annotations == nil {
		primary.Annotations = map[string]string{}
	}
	primary.Annotations[utils.FederationAutoscalerMembersAnnotation] = string(data)
}

func configureFederationAutoscaler(spec *rayv1.RayClusterSpec, name string) {
	if !utils.IsAutoscalingEnabled(spec) {
		return
	}
	if spec.AutoscalerOptions == nil {
		spec.AutoscalerOptions = &rayv1.AutoscalerOptions{}
	}
	options := spec.AutoscalerOptions
	options.Version = ptr.To(rayv1.AutoscalerVersionV2)
	options.Command = []string{"python"}
	options.Args = []string{utils.FederationAutoscalerPath}
	options.Env = append(options.Env, corev1.EnvVar{Name: utils.FederationAutoscalerSnapshotEnv, Value: name})
	options.VolumeMounts = append(options.VolumeMounts, corev1.VolumeMount{Name: utils.FederationAutoscalerVolume, MountPath: utils.FederationAutoscalerDirectory, ReadOnly: true})
	spec.HeadGroupSpec.Template.Spec.Volumes = append(spec.HeadGroupSpec.Template.Spec.Volumes, corev1.Volume{
		Name: utils.FederationAutoscalerVolume,
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Items:                []corev1.KeyToPath{{Key: "federation_autoscaler.py", Path: "federation_autoscaler.py"}},
		}},
	})
}

// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update

func (r *FederatedReconciler) ensureAutoscalerResources(ctx context.Context, frc *rayv1.FederatedRayCluster) error {
	if !ptr.Deref(frc.Spec.PrimaryCluster.EnableInTreeAutoscaling, false) {
		return nil
	}
	name := autoscalerConfigMapName(frc)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: frc.Namespace}}
	role := &rbacv1.Role{ObjectMeta: *cm.ObjectMeta.DeepCopy()}
	binding := &rbacv1.RoleBinding{ObjectMeta: *cm.ObjectMeta.DeepCopy()}
	for _, object := range []client.Object{cm, role, binding} {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
			if object.GetResourceVersion() != "" && !metav1.IsControlledBy(object, frc) {
				return fmt.Errorf("autoscaler resource %s already exists with another owner", name)
			}
			if err := ctrl.SetControllerReference(frc, object, r.Scheme); err != nil {
				return err
			}
			switch value := object.(type) {
			case *corev1.ConfigMap:
				if value.Data == nil {
					value.Data = map[string]string{autoscalerSnapshotKey: `{"schemaVersion":2,"error":"Waiting for federation observation"}`}
				}
				value.Data["federation_autoscaler.py"] = federationAutoscalerSource
			case *rbacv1.Role:
				value.Rules = []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{name}, Verbs: []string{"get"}}}
			case *rbacv1.RoleBinding:
				primary := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: frc.Name, Namespace: frc.Namespace}, Spec: rayv1.RayClusterSpec{HeadGroupSpec: frc.Spec.PrimaryCluster.HeadGroupSpec}}
				value.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}
				value.Subjects = []rbacv1.Subject{{Kind: "ServiceAccount", Name: utils.CheckName(utils.GetHeadGroupServiceAccountName(primary)), Namespace: frc.Namespace}}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Snapshot publication is separate from replica propagation. A partial or failed
// observation is explicitly unusable, rather than falsely reporting zero workers.
func (r *FederatedReconciler) publishAutoscalerSnapshot(ctx context.Context, frc *rayv1.FederatedRayCluster, reconcileErr error) error {
	if !ptr.Deref(frc.Spec.PrimaryCluster.EnableInTreeAutoscaling, false) {
		return nil
	}
	cm := &corev1.ConfigMap{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: frc.Namespace, Name: autoscalerConfigMapName(frc)}, cm); err != nil {
		return client.IgnoreNotFound(err) // Runtime resources may not have been created yet.
	}
	if !metav1.IsControlledBy(cm, frc) {
		return fmt.Errorf("autoscaler observation ConfigMap has a different owner")
	}
	base := cm.DeepCopy()
	snapshot := autoscalerSnapshot{SchemaVersion: 2, AdapterRevision: fmt.Sprintf("%x", sha256.Sum256([]byte(federationAutoscalerSource))), ObservedAt: time.Now().UTC(), Pods: []corev1.Pod{}}
	err := reconcileErr
	if err == nil && slices.ContainsFunc(frc.Status.Conditions, func(c metav1.Condition) bool { return c.Type == "Ready" && c.Reason == "ReconcilingTopology" }) {
		err = fmt.Errorf("waiting for member destination inventory")
	}
	if err == nil {
		err = r.collectAutoscalerPods(ctx, frc, &snapshot)
	}
	if err != nil {
		snapshot.Error, snapshot.Pods = err.Error(), []corev1.Pod{}
	}
	data, marshalErr := encodeAutoscalerSnapshot(&snapshot, time.Now())
	if marshalErr != nil {
		return marshalErr
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[autoscalerSnapshotKey] = string(data)
	if err := r.Patch(ctx, cm, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	ready := snapshot.Error == ""
	reason, message := "CompleteObservation", "The global autoscaler can observe primary and member Pods"
	if !ready {
		reason, message = "ObservationUnavailable", snapshot.Error
	}
	status := metav1.ConditionTrue
	if !ready {
		status = metav1.ConditionFalse
	}
	meta.RemoveStatusCondition(&frc.Status.Conditions, "AutoscalerReady")
	condition(&frc.Status.Conditions, frc.Generation, "AutoscalerObservationReady", status, reason, message)
	if !ready {
		condition(&frc.Status.Conditions, frc.Generation, "Ready", metav1.ConditionFalse, reason, message)
	}
	return nil
}

func encodeAutoscalerSnapshot(snapshot *autoscalerSnapshot, now time.Time) ([]byte, error) {
	if snapshot.Error == "" && now.Sub(snapshot.ObservedAt) > autoscalerSnapshotMaxAge {
		snapshot.Error = "Federation autoscaler observation exceeded the 30 second collection budget"
		snapshot.Pods = []corev1.Pod{}
	}
	data, err := json.Marshal(snapshot)
	// Reserve space below the 1 MiB ConfigMap limit for adapter source. Do not
	// truncate live workers or report an already expired observation as ready.
	if len(data) > 900*1024 {
		snapshot.Error, snapshot.Pods = "Federation autoscaler observation exceeds 900 KiB", []corev1.Pod{}
		return json.Marshal(snapshot)
	}
	return data, err
}

func (r *FederatedReconciler) collectAutoscalerPods(ctx context.Context, frc *rayv1.FederatedRayCluster, snapshot *autoscalerSnapshot) error {
	primary := &rayv1.RayCluster{}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(frc), primary); err != nil {
		return err
	}
	if !metav1.IsControlledBy(primary, frc) || !utils.IsFederationAutoscalingConfigured(&primary.Spec) {
		return fmt.Errorf("primary is not configured for federation autoscaling")
	}
	snapshot.PrimaryUID, snapshot.PrimaryGeneration = string(primary.UID), primary.Generation
	if len(frc.Status.MemberClusterStatuses) != len(frc.Spec.MemberClusters) {
		return fmt.Errorf("waiting for member destination inventory")
	}
	if err := appendAutoscalerPods(ctx, r.Reader, primary, snapshot); err != nil {
		return err
	}
	for _, member := range frc.Spec.MemberClusters {
		index := slices.IndexFunc(frc.Status.MemberClusterStatuses, func(s rayv1.FederationMemberStatus) bool { return destinationEqual(s, member) })
		if index < 0 {
			return fmt.Errorf("waiting for member %s destination inventory", member.Name)
		}
		status := frc.Status.MemberClusterStatuses[index]
		remote, err := r.boundMemberClient(ctx, frc.Namespace, status)
		if err != nil {
			return fmt.Errorf("member %s observation unavailable: %w", member.Name, err)
		}
		mrc := &rayv1.RayCluster{}
		if err := remote.Get(ctx, client.ObjectKey{Namespace: status.Namespace, Name: status.RayClusterName}, mrc); err != nil {
			return fmt.Errorf("member %s observation unavailable: %w", member.Name, err)
		}
		if mrc.Labels[OwnerLabel] != string(frc.UID) || mrc.Labels[utils.FederationMemberLabel] != member.Name || mrc.Spec.HeadGroupSpec != nil || !mrc.DeletionTimestamp.IsZero() {
			return fmt.Errorf("member %s no longer matches its bound destination", member.Name)
		}
		desired := desiredMember(frc, primary, member)
		if !sameReplicaIntent(desired, mrc) {
			return fmt.Errorf("waiting for member %s to receive the primary's replica intent", member.Name)
		}
		if err := appendAutoscalerPods(ctx, remote, mrc, snapshot); err != nil {
			return fmt.Errorf("member %s Pod observation unavailable: %w", member.Name, err)
		}
	}
	// A concurrent scale or topology edit invalidates the entire collected view.
	current := &rayv1.RayCluster{}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(primary), current); err != nil {
		return err
	}
	if current.UID != primary.UID || current.Generation != primary.Generation {
		return fmt.Errorf("primary changed while collecting federation Pods")
	}
	return nil
}

func sameReplicaIntent(desired, observed *rayv1.RayCluster) bool {
	if len(desired.Spec.WorkerGroupSpecs) != len(observed.Spec.WorkerGroupSpecs) {
		return false
	}
	for _, group := range desired.Spec.WorkerGroupSpecs {
		index := slices.IndexFunc(observed.Spec.WorkerGroupSpecs, func(g rayv1.WorkerGroupSpec) bool { return g.GroupName == group.GroupName })
		if index < 0 {
			return false
		}
		actual := observed.Spec.WorkerGroupSpecs[index]
		if ptr.Deref(group.Replicas, 0) != ptr.Deref(actual.Replicas, 0) || ptr.Deref(group.Suspend, false) != ptr.Deref(actual.Suspend, false) || !slices.Equal(group.ScaleStrategy.WorkersToDelete, actual.ScaleStrategy.WorkersToDelete) {
			return false
		}
	}
	return true
}

func appendAutoscalerPods(ctx context.Context, reader client.Reader, cluster *rayv1.RayCluster, snapshot *autoscalerSnapshot) error {
	list := &corev1.PodList{}
	if err := reader.List(ctx, list, common.RayClusterAllPodsAssociationOptions(cluster).ToListOptions()...); err != nil {
		return err
	}
	for i := range list.Items {
		pod := &list.Items[i]
		if !metav1.IsControlledBy(pod, cluster) || pod.Labels["ray.io/federation-probe"] == "true" {
			continue
		}
		kind, group := pod.Labels[utils.RayNodeTypeLabelKey], pod.Labels[utils.RayNodeGroupLabelKey]
		if kind == "head" {
			if cluster.Spec.HeadGroupSpec == nil || group != utils.RayNodeHeadGroupLabelValue {
				continue
			}
		} else if kind != "worker" || !slices.ContainsFunc(cluster.Spec.WorkerGroupSpecs, func(g rayv1.WorkerGroupSpec) bool { return !g.IsExternallyManaged() && g.GroupName == group }) {
			continue
		}
		member := ""
		if cluster.Spec.HeadGroupSpec == nil {
			member = cluster.Labels[utils.FederationMemberLabel]
		}
		if slices.ContainsFunc(snapshot.Pods, func(p corev1.Pod) bool { return p.Name == pod.Name && p.Labels[utils.FederationMemberLabel] == member }) {
			return fmt.Errorf("duplicate Ray Pod name %s across federation destinations", pod.Name)
		}
		labels := map[string]string{utils.RayNodeTypeLabelKey: kind, utils.RayNodeGroupLabelKey: group}
		if member != "" {
			labels[utils.FederationMemberLabel] = member
		}
		// Publish only fields consumed by the native provider. Pod specs may contain
		// user environment values and are deliberately excluded from this transport.
		snapshot.Pods = append(snapshot.Pods, corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, UID: pod.UID, Namespace: pod.Namespace, DeletionTimestamp: pod.DeletionTimestamp, Labels: labels},
			Status:     corev1.PodStatus{Phase: pod.Status.Phase, PodIP: pod.Status.PodIP, ContainerStatuses: minimalContainerStatuses(pod.Status.ContainerStatuses)},
		})
	}
	return nil
}

func minimalContainerStatuses(statuses []corev1.ContainerStatus) []corev1.ContainerStatus {
	result := make([]corev1.ContainerStatus, 0, len(statuses))
	for _, status := range statuses {
		result = append(result, corev1.ContainerStatus{Name: status.Name, State: *status.State.DeepCopy()})
	}
	return result
}
