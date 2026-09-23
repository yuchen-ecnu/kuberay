package federation

import (
	"fmt"

	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// desiredPrimarySpec builds user configuration without mutating the FRC or
// reading runtime replica targets. Validation and reconciliation share it.
func desiredPrimarySpec(frc *rayv1.FederatedRayCluster) rayv1.RayClusterSpec {
	spec := rayv1.RayClusterSpec{
		RayVersion:              frc.Spec.PrimaryCluster.RayVersion,
		HeadGroupSpec:           frc.Spec.PrimaryCluster.HeadGroupSpec.DeepCopy(),
		EnableInTreeAutoscaling: new(ptr.Deref(frc.Spec.PrimaryCluster.EnableInTreeAutoscaling, false)),
		AutoscalerOptions:       frc.Spec.PrimaryCluster.AutoscalerOptions.DeepCopy(),
		AuthOptions:             &rayv1.AuthOptions{Mode: rayv1.AuthModeDisabled},
	}
	for _, group := range frc.Spec.PrimaryCluster.WorkerGroups {
		spec.WorkerGroupSpecs = append(spec.WorkerGroupSpecs, *group.DeepCopy())
	}
	for _, member := range frc.Spec.MemberClusters {
		if member.KubeconfigSecretRef == nil {
			continue
		}
		for _, group := range member.WorkerGroups {
			group = *group.DeepCopy()
			group.ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
			spec.WorkerGroupSpecs = append(spec.WorkerGroupSpecs, group)
		}
	}

	for i := range spec.WorkerGroupSpecs {
		normalizeGroup(&spec.WorkerGroupSpecs[i])
	}
	return spec
}

func validateProjectedClusters(frc *rayv1.FederatedRayCluster) error {
	primary := &rayv1.RayCluster{Spec: desiredPrimarySpec(frc)}
	configureFederationAutoscaler(&primary.Spec, autoscalerConfigMapName(frc))
	if err := utils.ValidateRayClusterSpec(&primary.Spec, nil); err != nil {
		return fmt.Errorf("invalid primary RayCluster: %w", err)
	}
	for _, member := range frc.Spec.MemberClusters {
		if member.KubeconfigSecretRef == nil {
			continue
		}
		projected := desiredMember(frc, primary, member)
		if err := utils.ValidateWorkerOnlySpec(&projected.Spec, nil); err != nil {
			return fmt.Errorf("invalid member %s RayCluster: %w", member.Name, err)
		}
	}
	return nil
}
