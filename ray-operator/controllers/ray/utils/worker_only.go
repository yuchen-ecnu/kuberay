package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

const (
	FederationOwnerLabel    = "ray.io/federation-owner"
	FederationMemberLabel   = "ray.io/federation-member"
	WorkerTemplateHashLabel = "ray.io/worker-template"
	WorkersReady            = "WorkersReady"
)

// ExternalHeadAddress is shared by validation, the start command and GCS readiness.
// Literal host:port values keep the init container and Ray process consistent.
func ExternalHeadAddress(spec *rayv1.RayClusterSpec) (string, string, error) {
	if len(spec.WorkerGroupSpecs) == 0 {
		return "", "", fmt.Errorf("workers-only RayClusters require workerGroupSpecs")
	}
	address := spec.WorkerGroupSpecs[0].RayStartParams["address"]
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.ContainsAny(host, "$%/\\") {
		return "", "", fmt.Errorf("workers-only rayStartParams.address must be a literal host:port")
	}
	if net.ParseIP(host) == nil && len(validation.IsDNS1123Subdomain(host)) != 0 {
		return "", "", fmt.Errorf("invalid external head hostname %q", host)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return "", "", fmt.Errorf("external head port must be between 1 and 65535")
	}
	for _, group := range spec.WorkerGroupSpecs {
		if group.RayStartParams["address"] != address {
			return "", "", fmt.Errorf("all workers-only groups must use the same rayStartParams.address")
		}
	}
	return host, port, nil
}

// ValidateWorkerOnlySpec validates configuration independently of the local
// operator's feature gates, so federation can validate a remote projection too.
// Admission of a local RayCluster additionally checks RayFederation.
func ValidateWorkerOnlySpec(spec *rayv1.RayClusterSpec, annotations map[string]string) error {
	if IsAutoscalingEnabled(spec) || spec.AutoscalerOptions != nil {
		return fmt.Errorf("workers-only RayClusters cannot configure autoscaling")
	}
	if spec.GcsFaultToleranceOptions != nil || spec.HistoryServerOptions != nil ||
		len(spec.HeadServiceAnnotations) != 0 || spec.TLSOptions != nil ||
		annotations[RayFTEnabledAnnotationKey] != "" || annotations[RayExternalStorageNSAnnotationKey] != "" {
		return fmt.Errorf("workers-only RayClusters cannot configure local head services, storage, history server, or managed TLS")
	}
	if _, _, err := ExternalHeadAddress(spec); err != nil {
		return err
	}
	names := map[string]bool{}
	for _, group := range spec.WorkerGroupSpecs {
		if ptr.Deref(group.ManagedBy, rayv1.WorkerGroupManagedByRayCluster) != rayv1.WorkerGroupManagedByRayCluster {
			return fmt.Errorf("workers-only groups must be managed by %s", rayv1.WorkerGroupManagedByRayCluster)
		}
		if group.IdleTimeoutSeconds != nil || ptr.Deref(group.Priority, 0) != 0 {
			return fmt.Errorf("workers-only groups cannot configure autoscaler idle timeout or priority")
		}
		if group.NumOfHosts > 1 {
			return fmt.Errorf("workers-only groups currently support single-host replicas")
		}
		if names[group.GroupName] || len(validation.IsDNS1123Label(group.GroupName)) != 0 || group.GroupName == RayNodeHeadGroupLabelValue {
			return fmt.Errorf("workers-only group names must be unique, valid DNS labels and cannot be headgroup")
		}
		names[group.GroupName] = true
		if _, present := group.RayStartParams["head"]; present {
			return fmt.Errorf("workers-only rayStartParams cannot set head")
		}
		if ptr.Deref(group.Replicas, 0) < 0 {
			return fmt.Errorf("worker replicas cannot be negative")
		}
	}
	if err := validateWorkerGroupSpecs(spec); err != nil {
		return err
	}
	if err := validateAuthOptions(spec); err != nil {
		return err
	}
	if IsK8sAuthEnabled(spec.AuthOptions) {
		return fmt.Errorf("workers-only RayClusters cannot configure Kubernetes token auth")
	}
	if IsAuthEnabled(spec) && ptr.Deref(spec.AuthOptions.SecretName, "") == "" {
		return fmt.Errorf("workers-only token auth requires a pre-created authOptions.secretName matching the head")
	}
	if spec.NetworkPolicy != nil && !features.Enabled(features.RayClusterNetworkPolicy) {
		return fmt.Errorf("spec.networkPolicy requires the RayClusterNetworkPolicy feature gate")
	}
	if spec.NetworkPolicy != nil && spec.NetworkPolicy.Head != nil {
		return fmt.Errorf("workers-only RayClusters cannot configure head network policy")
	}
	return validateNetworkPolicy(spec)
}

// ConfigurationHash identifies normalized configuration while preserving alpha revision compatibility.
func ConfigurationHash(value any) string {
	data, _ := json.Marshal(value)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])[:32]
}

// WorkerTemplateHash excludes scale decisions, so scaling never rolls existing workers.
func WorkerTemplateHash(cluster *rayv1.RayCluster, group rayv1.WorkerGroupSpec) string {
	group = *group.DeepCopy()
	if !group.IsExternallyManaged() {
		group.ManagedBy = nil
	}
	group.Replicas, group.MinReplicas, group.MaxReplicas, group.Suspend = nil, nil, nil, nil
	group.ScaleStrategy = rayv1.ScaleStrategy{}
	return ConfigurationHash([]any{group, cluster.Spec.RayVersion, cluster.Spec.AuthOptions, cluster.Annotations[RayOverwriteContainerCmdAnnotationKey], cluster.Labels[FederationMemberLabel]})
}

// IsCurrentWorker excludes old templates, terminating Pods and targeted removals
// from readiness, even while the API still reports those old Pods as Ready.
func IsCurrentWorker(cluster *rayv1.RayCluster, group rayv1.WorkerGroupSpec, pod *corev1.Pod) bool {
	return metav1.IsControlledBy(pod, cluster) && pod.DeletionTimestamp.IsZero() &&
		pod.Labels[RayNodeGroupLabelKey] == group.GroupName &&
		pod.Labels[WorkerTemplateHashLabel] == WorkerTemplateHash(cluster, group) &&
		!ptr.Deref(cluster.Spec.Suspend, false) && !ptr.Deref(group.Suspend, false) &&
		!slices.Contains(group.ScaleStrategy.WorkersToDelete, pod.Name)
}
