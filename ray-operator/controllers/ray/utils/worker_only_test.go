package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func workerOnlySpec() *rayv1.RayClusterSpec {
	return &rayv1.RayClusterSpec{RayVersion: "2.56.0", WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
		GroupName: "cpu", Replicas: ptr.To[int32](2), MinReplicas: ptr.To[int32](0), MaxReplicas: ptr.To[int32](10), NumOfHosts: 1,
		RayStartParams: map[string]string{"address": "head.private:6379"},
		Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "ray"}}}},
	}}}
}

func TestWorkerOnlyValidation(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayFederation, true)
	require.NoError(t, ValidateRayClusterSpec(workerOnlySpec(), nil))
	local := workerOnlySpec()
	local.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	require.NoError(t, ValidateRayClusterSpec(local, nil))
	for _, test := range []struct {
		name   string
		mutate func(*rayv1.RayClusterSpec)
		want   string
	}{
		{"no workers", func(s *rayv1.RayClusterSpec) { s.WorkerGroupSpecs = nil }, "workerGroupSpecs"},
		{"no containers", func(s *rayv1.RayClusterSpec) { s.WorkerGroupSpecs[0].Template.Spec.Containers = nil }, "container"},
		{"autoscale", func(s *rayv1.RayClusterSpec) { s.EnableInTreeAutoscaling = new(true) }, "autoscaling"},
		{"autoscaler options", func(s *rayv1.RayClusterSpec) { s.AutoscalerOptions = &rayv1.AutoscalerOptions{} }, "autoscaling"},
		{"federation manager", func(s *rayv1.RayClusterSpec) {
			s.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByFederatedRayCluster)
		}, "must be managed by"},
		{"empty manager", func(s *rayv1.RayClusterSpec) { s.WorkerGroupSpecs[0].ManagedBy = new("") }, "must be managed by"},
		{"bounds", func(s *rayv1.RayClusterSpec) { s.WorkerGroupSpecs[0].MinReplicas = ptr.To[int32](11) }, "greater"},
		{"duplicate", func(s *rayv1.RayClusterSpec) { s.WorkerGroupSpecs = append(s.WorkerGroupSpecs, s.WorkerGroupSpecs[0]) }, "unique"},
		{"head flag", func(s *rayv1.RayClusterSpec) { s.WorkerGroupSpecs[0].RayStartParams["head"] = "false" }, "cannot set head"},
		{"idle timeout", func(s *rayv1.RayClusterSpec) { s.WorkerGroupSpecs[0].IdleTimeoutSeconds = ptr.To[int32](30) }, "idle timeout"},
		{"local storage", func(s *rayv1.RayClusterSpec) { s.GcsFaultToleranceOptions = &rayv1.GcsFaultToleranceOptions{} }, "local head"},
		{"managed TLS", func(s *rayv1.RayClusterSpec) { s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)} }, "managed TLS"},
		{"different endpoints", func(s *rayv1.RayClusterSpec) {
			other := *s.WorkerGroupSpecs[0].DeepCopy()
			other.GroupName = "gpu"
			other.RayStartParams["address"] = "other:6379"
			s.WorkerGroupSpecs = append(s.WorkerGroupSpecs, other)
		}, "same"},
		{"generated token", func(s *rayv1.RayClusterSpec) { s.AuthOptions = &rayv1.AuthOptions{Mode: rayv1.AuthModeToken} }, "pre-created"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := workerOnlySpec()
			test.mutate(s)
			require.ErrorContains(t, ValidateRayClusterSpec(s, nil), test.want)
		})
	}
	for _, address := range []string{"", "auto", "local", "head", "head:0", "head:65536", "head:abc", "$(HEAD):6379", "head;echo injected:6379", "https://head:6379"} {
		s := workerOnlySpec()
		s.WorkerGroupSpecs[0].RayStartParams["address"] = address
		require.Error(t, ValidateRayClusterSpec(s, nil), address)
	}
	s := workerOnlySpec()
	s.AuthOptions = &rayv1.AuthOptions{Mode: rayv1.AuthModeToken, SecretName: new("shared-token")}
	require.NoError(t, ValidateRayClusterSpec(s, nil))
	// Empty head is distinct from an omitted head.
	s = workerOnlySpec()
	s.HeadGroupSpec = &rayv1.HeadGroupSpec{}
	require.ErrorContains(t, ValidateRayClusterSpec(s, nil), "headGroupSpec should")
	features.SetFeatureGateDuringTest(t, features.RayFederation, false)
	require.ErrorContains(t, ValidateRayClusterSpec(workerOnlySpec(), nil), "RayFederation")
}

func TestWorkerOnlyRequiresHeadForJobAndService(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayFederation, true)
	require.ErrorContains(t, ValidateRayJobSpec(&rayv1.RayJob{Spec: rayv1.RayJobSpec{RayClusterSpec: workerOnlySpec()}}), "headGroupSpec")
	require.ErrorContains(t, ValidateRayServiceSpec(&rayv1.RayService{Spec: rayv1.RayServiceSpec{RayClusterSpec: *workerOnlySpec()}}), "headGroupSpec")
}

func TestWorkerTemplateHashIgnoresReplicaAndClientMetadata(t *testing.T) {
	c := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "member"}, Spec: *workerOnlySpec()}
	before := WorkerTemplateHash(c, c.Spec.WorkerGroupSpecs[0])
	c.Spec.WorkerGroupSpecs[0].ManagedBy = new(rayv1.WorkerGroupManagedByRayCluster)
	assert.Equal(t, before, WorkerTemplateHash(c, c.Spec.WorkerGroupSpecs[0]), "explicit local management must not roll workers")
	c.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](4)
	c.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{"old-worker"}
	c.Annotations = map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "changed replicas"}
	assert.Equal(t, before, WorkerTemplateHash(c, c.Spec.WorkerGroupSpecs[0]))
	c.Spec.WorkerGroupSpecs[0].RayStartParams["address"] = "other.private:6379"
	assert.NotEqual(t, before, WorkerTemplateHash(c, c.Spec.WorkerGroupSpecs[0]))
}
