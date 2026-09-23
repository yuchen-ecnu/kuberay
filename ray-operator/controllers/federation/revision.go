package federation

import (
	"fmt"

	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// FederationPrimaryRevision records the configuration that a primary can run
// without a coordinated restart of the entire Ray runtime. Scaling and topology
// changes remain mutable; replacing an existing primary template requires a new FRC.
type FederationPrimaryRevision struct {
	Head         string            `json:"head"`
	WorkerGroups map[string]string `json:"workerGroups,omitempty"`
}

func FederationPrimaryConfiguration(spec rayv1.FederatedRayClusterSpec) FederationPrimaryRevision {
	primary := spec.PrimaryCluster
	revision := FederationPrimaryRevision{
		Head:         utils.ConfigurationHash([]any{primary.RayVersion, primary.HeadGroupSpec}),
		WorkerGroups: make(map[string]string, len(primary.WorkerGroups)),
	}
	// Preserve the revision format for existing, non-autoscaling federations.
	// Container settings need a runtime restart; scheduling policy can hot reload.
	if ptr.Deref(primary.EnableInTreeAutoscaling, false) {
		options := primary.AutoscalerOptions.DeepCopy()
		if options == nil {
			options = &rayv1.AutoscalerOptions{}
		}
		options.Version = ptr.To(rayv1.AutoscalerVersionV2)
		options.IdleTimeoutSeconds, options.UpscalingMode = nil, nil
		revision.Head = utils.ConfigurationHash([]any{primary.RayVersion, primary.HeadGroupSpec, true, options})
	}
	for _, group := range primary.WorkerGroups {
		if group.IsExternallyManaged() {
			continue
		}
		group = *group.DeepCopy()
		group.ManagedBy = nil
		group.Replicas, group.MinReplicas, group.MaxReplicas, group.Suspend = nil, nil, nil, nil
		group.ScaleStrategy = rayv1.ScaleStrategy{}
		if ptr.Deref(primary.EnableInTreeAutoscaling, false) {
			group.IdleTimeoutSeconds, group.Priority = nil, nil
		}
		revision.WorkerGroups[group.GroupName] = utils.ConfigurationHash(group)
	}
	return revision
}

func ValidateFederationPrimaryRevision(previous, desired FederationPrimaryRevision) error {
	if previous.Head != desired.Head {
		return fmt.Errorf("primaryCluster.rayVersion, headGroupSpec and autoscaler runtime configuration cannot change in place; create a new FederatedRayCluster for a runtime upgrade")
	}
	for group, revision := range desired.WorkerGroups {
		if old, exists := previous.WorkerGroups[group]; exists && old != revision {
			return fmt.Errorf("primary worker group %s configuration cannot change in place; create a new FederatedRayCluster for a runtime upgrade", group)
		}
	}
	return nil
}
