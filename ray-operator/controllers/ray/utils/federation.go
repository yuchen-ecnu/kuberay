package utils

import (
	"slices"

	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

const (
	FederationAutoscalerSnapshotEnv       = "KUBERAY_FEDERATION_SNAPSHOT"
	FederationAutoscalerMembersAnnotation = "ray.io/federation-autoscaler-members"
	FederationAutoscalerVolume            = "kuberay-federation-autoscaler"
	FederationAutoscalerDirectory         = "/etc/kuberay/federation"
	FederationAutoscalerPath              = FederationAutoscalerDirectory + "/federation_autoscaler.py"
)

// The stock KubeRay provider cannot observe delegated Pods. Permit autoscaling
// only with the federation adapter installed through existing autoscaler options.
func IsFederationAutoscalingConfigured(spec *rayv1.RayClusterSpec) bool {
	options := spec.AutoscalerOptions
	if !IsAutoscalingEnabled(spec) || options == nil || ptr.Deref(options.Version, "") != rayv1.AutoscalerVersionV2 ||
		!slices.Equal(options.Command, []string{"python"}) || !slices.Equal(options.Args, []string{FederationAutoscalerPath}) {
		return false
	}
	env, present := EnvVarByName(FederationAutoscalerSnapshotEnv, options.Env)
	return present && env.Value != "" && env.ValueFrom == nil
}
