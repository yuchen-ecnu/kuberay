// Package federation separates cross-cluster desired-state propagation from
// member-local Pod lifecycle management.
package federation

import (
	"fmt"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	OwnerLabel      = utils.FederationOwnerLabel
	orphanedByLabel = "ray.io/federation-orphaned-by"
	CredentialLabel = "ray.io/federation-credential" // #nosec G101 -- Kubernetes label key, not a credential value.
	Finalizer       = "ray.io/federation-finalizer"
	pollInterval    = 10 * time.Second
	staleAfter      = 90 * time.Second
)

func condition(conditions *[]metav1.Condition, generation int64, kind string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: kind, Status: status,
		ObservedGeneration: generation, Reason: reason, Message: message,
	})
}

func normalizeNetworking(n *rayv1.FederationNetworking) {
	if n.HeadEndpoint.GCSPort == 0 {
		n.HeadEndpoint.GCSPort = 6379
	}
	if n.HeadEndpoint.Mode == "" {
		n.HeadEndpoint.Mode = "UserProvided"
	}
}

func normalizeGroup(group *rayv1.WorkerGroupSpec) {
	if group.NumOfHosts == 0 {
		group.NumOfHosts = 1
	}
	if group.MinReplicas == nil {
		group.MinReplicas = ptr.To[int32](0)
	}
	if group.MaxReplicas == nil {
		group.MaxReplicas = ptr.To[int32](2147483647)
	}
	if group.Replicas == nil {
		group.Replicas = ptr.To[int32](0)
	}
}

func validateHeadEndpoint(n rayv1.FederationNetworking) error {
	if n.HeadEndpoint.Mode != "UserProvided" {
		return fmt.Errorf("only UserProvided head endpoints are supported")
	}
	address := n.HeadEndpoint.Address
	if ip := net.ParseIP(address); ip != nil {
		if !ip.IsPrivate() {
			return fmt.Errorf("head endpoint must use a private address")
		}
	} else if errs := validation.IsDNS1123Subdomain(address); len(errs) > 0 {
		return fmt.Errorf("head endpoint must be an IP address or DNS name without a scheme or port")
	}
	if n.HeadEndpoint.GCSPort < 1 || n.HeadEndpoint.GCSPort > 65535 {
		return fmt.Errorf("invalid GCS port")
	}
	return nil
}

func validateWorkerGroups(groups []rayv1.WorkerGroupSpec, names map[string]bool) error {
	for _, group := range groups {
		if errs := validation.IsDNS1123Label(group.GroupName); len(errs) > 0 {
			return fmt.Errorf("invalid worker group name %q", group.GroupName)
		}
		if names[group.GroupName] {
			return fmt.Errorf("worker group names must be federation-wide unique: %s", group.GroupName)
		}
		if group.GroupName == utils.RayNodeHeadGroupLabelValue {
			return fmt.Errorf("worker group name %s is reserved for the head", group.GroupName)
		}
		names[group.GroupName] = true
		if ptr.Deref(group.ManagedBy, rayv1.WorkerGroupManagedByRayCluster) != rayv1.WorkerGroupManagedByRayCluster {
			return fmt.Errorf("managedBy is assigned by the federation; omit it or use %s in workerGroups", rayv1.WorkerGroupManagedByRayCluster)
		}
		if group.NumOfHosts > 1 {
			return fmt.Errorf("federation currently supports single-host worker groups")
		}
		if len(group.Template.Spec.Containers) == 0 {
			return fmt.Errorf("worker group %s requires a container", group.GroupName)
		}
		if ptr.Deref(group.Replicas, 0) < 0 || ptr.Deref(group.MinReplicas, 0) < 0 || ptr.Deref(group.MaxReplicas, 2147483647) < ptr.Deref(group.MinReplicas, 0) {
			return fmt.Errorf("invalid replica bounds for group %s", group.GroupName)
		}
		for _, name := range group.ScaleStrategy.WorkersToDelete {
			if len(validation.IsDNS1123Subdomain(name)) > 0 {
				return fmt.Errorf("workersToDelete must contain Pod names within the worker group's member, not qualified instance IDs")
			}
		}
	}
	return nil
}

// ValidateFederation validates the complete user-authored federation spec. It is
// shared by admission and reconciliation so both paths enforce the same rules.
func ValidateFederation(frc *rayv1.FederatedRayCluster) error {
	if err := utils.ValidateRayClusterMetadata(frc.ObjectMeta); err != nil {
		return err
	}
	if err := validateHeadEndpoint(frc.Spec.Networking); err != nil {
		return err
	}
	if frc.Spec.PrimaryCluster.HeadGroupSpec == nil || len(frc.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("primary head requires a container")
	}
	groups := map[string]bool{}
	if err := validateWorkerGroups(frc.Spec.PrimaryCluster.WorkerGroups, groups); err != nil {
		return err
	}
	members := map[string]bool{}
	if len(frc.Spec.MemberClusters) == 0 || len(frc.Spec.MemberClusters) > 16 {
		return fmt.Errorf("a federation requires 1 to 16 members")
	}
	for _, member := range frc.Spec.MemberClusters {
		if members[member.Name] || len(validation.IsDNS1123Label(member.Name)) > 0 {
			return fmt.Errorf("invalid or duplicate member name %q", member.Name)
		}
		members[member.Name] = true
		if len(validation.IsDNS1123Label(member.Namespace)) > 0 {
			return fmt.Errorf("member %s requires a valid namespace", member.Name)
		}
		if member.KubeconfigSecretRef == nil {
			if len(member.WorkerGroups) != 0 {
				return fmt.Errorf("manual member %s must define workerGroupSpecs on its member RayCluster", member.Name)
			}
			continue
		}
		if member.KubeconfigSecretRef.Name == "" {
			return fmt.Errorf("member %s requires a nonempty kubeconfig Secret name", member.Name)
		}
		if len(member.WorkerGroups) == 0 {
			return fmt.Errorf("member %s requires a worker group", member.Name)
		}
		if err := validateWorkerGroups(member.WorkerGroups, groups); err != nil {
			return err
		}
	}
	if err := validateFederationAutoscaling(frc); err != nil {
		return err
	}
	return validateProjectedClusters(frc)
}

func validateFederationAutoscaling(frc *rayv1.FederatedRayCluster) error {
	primary := &frc.Spec.PrimaryCluster
	if !ptr.Deref(primary.EnableInTreeAutoscaling, false) {
		if primary.AutoscalerOptions != nil {
			return fmt.Errorf("autoscalerOptions requires enableInTreeAutoscaling: true")
		}
		return nil
	}
	rayVersion, err := version.ParseSemantic(primary.RayVersion)
	if err != nil || rayVersion.Major() != 2 || rayVersion.Minor() != 56 || rayVersion.Patch() != 0 || rayVersion.PreRelease() != "" || rayVersion.BuildMetadata() != "" {
		return fmt.Errorf("federation autoscaling currently requires Ray 2.56.0")
	}
	if frc.Spec.Networking.HeadEndpoint.GCSPort != 6379 {
		return fmt.Errorf("federation autoscaling currently requires networking.headEndpoint.gcsPort: 6379")
	}
	if port, present := primary.HeadGroupSpec.RayStartParams["port"]; present {
		value, err := strconv.ParseInt(port, 10, 32)
		if err != nil || value != 6379 {
			return fmt.Errorf("federation autoscaling currently requires the local GCS port to be 6379; omit primaryCluster.headGroupSpec.rayStartParams.port or set it to 6379")
		}
	}
	for _, member := range frc.Spec.MemberClusters {
		if member.KubeconfigSecretRef == nil {
			return fmt.Errorf("federation autoscaling requires kubeconfigSecretRef for member %s; manual members cannot be autoscaled", member.Name)
		}
	}
	options := primary.AutoscalerOptions.DeepCopy()
	if options == nil {
		options = &rayv1.AutoscalerOptions{}
	}
	if options.Version != nil && *options.Version != rayv1.AutoscalerVersionV2 {
		return fmt.Errorf("federation autoscaling requires autoscalerOptions.version: v2")
	}
	options.Version = ptr.To(rayv1.AutoscalerVersionV2)
	if len(options.Command) > 0 || len(options.Args) > 0 {
		return fmt.Errorf("federation manages autoscalerOptions.command and args; omit both fields")
	}
	for _, env := range options.Env {
		switch env.Name {
		case utils.FederationAutoscalerSnapshotEnv, utils.RAY_CLUSTER_NAME, utils.RAY_CLUSTER_NAMESPACE,
			"RAY_HEAD_POD_NAME", "KUBERAY_CRD_VER", utils.KUBERAY_GEN_AUTOSCALER_START_CMD:
			return fmt.Errorf("autoscalerOptions.env must not override federation-managed variable %s", env.Name)
		}
	}
	for _, mount := range options.VolumeMounts {
		mountPath := path.Clean(mount.MountPath)
		if mount.Name == utils.FederationAutoscalerVolume || mountPath == utils.FederationAutoscalerDirectory || strings.HasPrefix(mountPath, utils.FederationAutoscalerDirectory+"/") {
			return fmt.Errorf("autoscalerOptions.volumeMounts must not override the federation autoscaler mount")
		}
	}
	for _, volume := range primary.HeadGroupSpec.Template.Spec.Volumes {
		if volume.Name == utils.FederationAutoscalerVolume {
			return fmt.Errorf("primary head must not define the reserved %s volume", utils.FederationAutoscalerVolume)
		}
	}

	// Validate the global autoscaler's view before creating the primary. Groups
	// are still local here; the federation assigns their managers afterwards.
	spec := rayv1.RayClusterSpec{
		RayVersion: primary.RayVersion, HeadGroupSpec: primary.HeadGroupSpec,
		EnableInTreeAutoscaling: new(true), AutoscalerOptions: options,
		AuthOptions: &rayv1.AuthOptions{Mode: rayv1.AuthModeDisabled},
	}
	spec.WorkerGroupSpecs = append(spec.WorkerGroupSpecs, primary.WorkerGroups...)
	for _, member := range frc.Spec.MemberClusters {
		spec.WorkerGroupSpecs = append(spec.WorkerGroupSpecs, member.WorkerGroups...)
	}
	if err := utils.ValidateRayClusterSpec(&spec, nil); err != nil {
		return fmt.Errorf("invalid federation autoscaler configuration: %w", err)
	}
	return nil
}
