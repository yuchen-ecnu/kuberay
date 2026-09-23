package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const primaryRevisionAnnotation = "ray.io/federation-primary-revision"

func primaryConfiguration(spec rayv1.RayClusterSpec) FederationPrimaryRevision {
	return FederationPrimaryConfiguration(rayv1.FederatedRayClusterSpec{
		PrimaryCluster: rayv1.FederationPrimaryCluster{
			HeadGroupSpec: spec.HeadGroupSpec, RayVersion: spec.RayVersion, WorkerGroups: spec.WorkerGroupSpecs,
			EnableInTreeAutoscaling: spec.EnableInTreeAutoscaling, AutoscalerOptions: spec.AutoscalerOptions,
		},
	})
}

func recordedPrimaryConfiguration(primary *rayv1.RayCluster) (FederationPrimaryRevision, error) {
	if data := primary.Annotations[primaryRevisionAnnotation]; data != "" {
		var revision FederationPrimaryRevision
		if err := json.Unmarshal([]byte(data), &revision); err != nil || revision.Head == "" {
			return revision, fmt.Errorf("invalid primary configuration revision")
		}
		return revision, nil
	}
	// Migrate an existing alpha primary before accepting further template edits.
	spec := primary.Spec.DeepCopy()
	if utils.IsFederationAutoscalingConfigured(spec) {
		// Hash the user configuration, not generated adapter wiring. This makes
		// loss of the optional revision annotation recoverable without a restart.
		spec.AutoscalerOptions.Command, spec.AutoscalerOptions.Args = nil, nil
		spec.AutoscalerOptions.Env = slices.DeleteFunc(spec.AutoscalerOptions.Env, func(env corev1.EnvVar) bool {
			return env.Name == utils.FederationAutoscalerSnapshotEnv
		})
		spec.AutoscalerOptions.VolumeMounts = slices.DeleteFunc(spec.AutoscalerOptions.VolumeMounts, func(mount corev1.VolumeMount) bool {
			return mount.Name == utils.FederationAutoscalerVolume
		})
		spec.HeadGroupSpec.Template.Spec.Volumes = slices.DeleteFunc(spec.HeadGroupSpec.Template.Spec.Volumes, func(volume corev1.Volume) bool {
			return volume.Name == utils.FederationAutoscalerVolume
		})
	}
	return primaryConfiguration(*spec), nil
}

// Invalid alpha configurations may have been persisted before full projection
// validation existed. Correct them only when no owned Pod has been created.
func (r *FederatedReconciler) canRepairInvalidPrimary(ctx context.Context, primary *rayv1.RayCluster) (bool, error) {
	if utils.ValidateRayClusterSpec(&primary.Spec, primary.Annotations) == nil {
		return false, nil
	}
	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.InNamespace(primary.Namespace)); err != nil {
		return false, err
	}
	return !slices.ContainsFunc(pods.Items, func(p corev1.Pod) bool { return metav1.IsControlledBy(&p, primary) }), nil
}

// ValidatePrimaryUpdate runs before inventory cleanup and patching, so an
// invalid update cannot partially change the running federation without webhooks.
func (r *FederatedReconciler) ValidatePrimaryUpdate(ctx context.Context, frc *rayv1.FederatedRayCluster) error {
	primary := &rayv1.RayCluster{}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(frc), primary); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(primary, frc) {
		return fmt.Errorf("primary RayCluster already exists with another owner")
	}
	if repair, err := r.canRepairInvalidPrimary(ctx, primary); err != nil || repair {
		return err
	}
	desired := frc.Spec.DeepCopy()
	for i := range desired.PrimaryCluster.WorkerGroups {
		normalizeGroup(&desired.PrimaryCluster.WorkerGroups[i])
	}
	previous, err := recordedPrimaryConfiguration(primary)
	if err != nil {
		return err
	}
	return ValidateFederationPrimaryRevision(previous, FederationPrimaryConfiguration(*desired))
}

func recordPrimaryConfiguration(primary *rayv1.RayCluster, spec rayv1.RayClusterSpec, exists bool) error {
	desired := primaryConfiguration(spec)
	if exists {
		previous, err := recordedPrimaryConfiguration(primary)
		if err != nil {
			return err
		}
		if err := ValidateFederationPrimaryRevision(previous, desired); err != nil {
			return err
		}
	}
	data, err := json.Marshal(desired)
	if err != nil {
		return err
	}
	if primary.Annotations == nil {
		primary.Annotations = map[string]string{}
	}
	primary.Annotations[primaryRevisionAnnotation] = string(data)
	return nil
}
