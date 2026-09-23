package federation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func autoscalingFederation() *rayv1.FederatedRayCluster {
	frc := federationWithLocalWorkers()
	frc.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(true)
	return frc
}

func TestFederationAutoscalingValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*rayv1.FederatedRayCluster)
		message string
	}{
		{"default v2", func(*rayv1.FederatedRayCluster) {}, ""},
		{"explicit v2", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{Version: ptr.To(rayv1.AutoscalerVersionV2)}
		}, ""},
		{"patch version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "2.56.1" }, "Ray 2.56.0"},
		{"unsupported version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "2.55.0" }, "Ray 2.56.0"},
		{"future minor version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "2.57.0" }, "Ray 2.56.0"},
		{"missing version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "" }, "Ray 2.56.0"},
		{"invalid version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "2.56.invalid" }, "Ray 2.56.0"},
		{"prerelease version", func(f *rayv1.FederatedRayCluster) { f.Spec.PrimaryCluster.RayVersion = "2.56.0rc1" }, "Ray 2.56.0"},
		{"custom GCS port", func(f *rayv1.FederatedRayCluster) { f.Spec.Networking.HeadEndpoint.GCSPort = 12345 }, "gcsPort: 6379"},
		{"explicit local GCS port", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = map[string]string{"port": "6379"}
		}, ""},
		{"custom local GCS port", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = map[string]string{"port": "6380"}
		}, "local GCS port to be 6379"},
		{"invalid local GCS port", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.RayStartParams = map[string]string{"port": "invalid"}
		}, "local GCS port to be 6379"},
		{"v1", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{Version: ptr.To(rayv1.AutoscalerVersionV1)}
		}, "version: v2"},
		{"manual member", func(f *rayv1.FederatedRayCluster) {
			f.Spec.MemberClusters[0].KubeconfigSecretRef = nil
			f.Spec.MemberClusters[0].WorkerGroups = nil
		}, "manual members cannot be autoscaled"},
		{"disabled with options", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(false)
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{}
		}, "requires enableInTreeAutoscaling"},
		{"omitted enable with options", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.EnableInTreeAutoscaling = nil
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{}
		}, "requires enableInTreeAutoscaling"},
		{"command override", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{Command: []string{"sh"}}
		}, "command and args"},
		{"args override", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{Args: []string{"echo"}}
		}, "command and args"},
		{"negative global timeout", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{IdleTimeoutSeconds: ptr.To[int32](-1)}
		}, "idleTimeoutSeconds must be non-negative"},
		{"head restart policy supported", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
		}, ""},
		{"head v2 env conflicts", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "RAY_enable_autoscaler_v2", Value: "false"}}
		}, "both .spec.autoscalerOptions.version"},
		{"reserved head volume", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.HeadGroupSpec.Template.Spec.Volumes = []corev1.Volume{{Name: "kuberay-federation-autoscaler"}}
		}, "reserved kuberay-federation-autoscaler volume"},
		{"reserved mount name", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{VolumeMounts: []corev1.VolumeMount{{Name: "kuberay-federation-autoscaler", MountPath: "/other"}}}
		}, "federation autoscaler mount"},
		{"reserved mount path", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{VolumeMounts: []corev1.VolumeMount{{Name: "other", MountPath: "/etc/kuberay/federation"}}}
		}, "federation autoscaler mount"},
		{"reserved nested mount path", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{VolumeMounts: []corev1.VolumeMount{{Name: "other", MountPath: "/etc/kuberay/federation/federation_autoscaler.py"}}}
		}, "federation autoscaler mount"},
		{"custom env and mount", func(f *rayv1.FederatedRayCluster) {
			f.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{
				Env:          []corev1.EnvVar{{Name: "AUTOSCALER_UPDATE_INTERVAL_S", Value: "2"}},
				EnvFrom:      []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "autoscaler-env"}}}},
				VolumeMounts: []corev1.VolumeMount{{Name: "custom", MountPath: "/etc/custom"}},
			}
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frc := autoscalingFederation()
			tt.mutate(frc)
			before, err := json.Marshal(frc)
			require.NoError(t, err)
			err = ValidateFederation(frc)
			if tt.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.message)
			}
			after, err := json.Marshal(frc)
			require.NoError(t, err)
			assert.JSONEq(t, string(before), string(after), "validation must not mutate the user's spec")
		})
	}
}

func TestFederationAutoscalerReservedEnvironment(t *testing.T) {
	for _, name := range []string{
		"KUBERAY_FEDERATION_SNAPSHOT", "RAY_CLUSTER_NAME", "RAY_CLUSTER_NAMESPACE",
		"RAY_HEAD_POD_NAME", "KUBERAY_CRD_VER", "KUBERAY_GEN_AUTOSCALER_START_CMD",
	} {
		t.Run(name, func(t *testing.T) {
			frc := autoscalingFederation()
			frc.Spec.PrimaryCluster.AutoscalerOptions = &rayv1.AutoscalerOptions{Env: []corev1.EnvVar{{Name: name, Value: "override"}}}
			require.ErrorContains(t, ValidateFederation(frc), "must not override federation-managed variable "+name)
		})
	}
}

func TestFederationAutoscalerWorkerValidation(t *testing.T) {
	for _, location := range []string{"primary", "member"} {
		for _, tt := range []struct {
			name    string
			mutate  func(*rayv1.WorkerGroupSpec)
			message string
		}{
			{"v2 scheduling options", func(g *rayv1.WorkerGroupSpec) { g.IdleTimeoutSeconds, g.Priority = ptr.To[int32](0), ptr.To[int32](2) }, ""},
			{"negative idle timeout", func(g *rayv1.WorkerGroupSpec) { g.IdleTimeoutSeconds = ptr.To[int32](-1) }, "must be non-negative"},
			{"negative max replicas", func(g *rayv1.WorkerGroupSpec) { g.MaxReplicas = ptr.To[int32](-1) }, "invalid replica bounds"},
			{"min exceeds max", func(g *rayv1.WorkerGroupSpec) { g.MinReplicas = ptr.To[int32](11) }, "invalid replica bounds"},
			{"optional min and max", func(g *rayv1.WorkerGroupSpec) { g.MinReplicas, g.MaxReplicas = nil, nil }, ""},
			{"restart policy supported", func(g *rayv1.WorkerGroupSpec) { g.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways }, ""},
			{"suspended group", func(g *rayv1.WorkerGroupSpec) { g.Suspend = new(true) }, "cannot be suspended"},
			{"duplicate resources", func(g *rayv1.WorkerGroupSpec) {
				g.Resources, g.RayStartParams = map[string]string{"CPU": "1"}, map[string]string{"num-cpus": "1"}
			}, "resource fields should not be set in both"},
		} {
			t.Run(location+"/"+tt.name, func(t *testing.T) {
				frc := autoscalingFederation()
				group := &frc.Spec.PrimaryCluster.WorkerGroups[0]
				if location == "member" {
					group = &frc.Spec.MemberClusters[0].WorkerGroups[0]
				}
				tt.mutate(group)
				err := ValidateFederation(frc)
				if tt.message == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tt.message)
				}
			})
		}
	}
}
