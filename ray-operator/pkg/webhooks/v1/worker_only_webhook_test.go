package v1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestWorkerOnlyWebhook(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayFederation, true)
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "member", Namespace: "default"},
		Spec: rayv1.RayClusterSpec{WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
			GroupName: "cpu", MinReplicas: ptr.To[int32](0), MaxReplicas: ptr.To[int32](10), RayStartParams: map[string]string{"address": "head.private:6379"},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-worker", Image: "rayproject/ray:2.56.0"}}}},
		}}},
	}
	w := &RayClusterWebhook{}
	_, err := w.ValidateCreate(context.Background(), cluster)
	require.NoError(t, err)
	for _, address := range []string{"auto", "$(HEAD):6379", "head.private:65536"} {
		invalid := cluster.DeepCopy()
		invalid.Spec.WorkerGroupSpecs[0].RayStartParams["address"] = address
		_, err := w.ValidateCreate(context.Background(), invalid)
		require.Error(t, err, "must reject %q", address)
	}
	inconsistent := cluster.DeepCopy()
	other := cluster.Spec.WorkerGroupSpecs[0].DeepCopy()
	other.GroupName, other.RayStartParams["address"] = "gpu", "another-head:6379"
	inconsistent.Spec.WorkerGroupSpecs = append(inconsistent.Spec.WorkerGroupSpecs, *other)
	_, err = w.ValidateCreate(context.Background(), inconsistent)
	require.ErrorContains(t, err, "same rayStartParams.address")
	full := cluster.DeepCopy()
	full.Spec.HeadGroupSpec = &rayv1.HeadGroupSpec{Template: cluster.Spec.WorkerGroupSpecs[0].Template}
	_, err = w.ValidateUpdate(context.Background(), cluster, full)
	require.ErrorContains(t, err, "adding or removing headGroupSpec")
	_, err = w.ValidateUpdate(context.Background(), full, cluster)
	require.ErrorContains(t, err, "adding or removing headGroupSpec")
}
