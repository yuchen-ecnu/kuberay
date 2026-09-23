package federation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestPrimaryReplicaBootstrap(t *testing.T) {
	for _, mode := range []struct {
		name        string
		autoscaling bool
	}{{"manual", false}, {"autoscaling", true}} {
		for _, scope := range []string{"primary", "member"} {
			for _, target := range []struct {
				name     string
				replicas *int32
			}{
				{"zero", new(int32(0))},
				{"scaled", new(int32(5))},
				{"omitted", nil},
				{"above-bound", new(int32(12))},
			} {
				t.Run(mode.name+"/"+scope+"/"+target.name, func(t *testing.T) {
					ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
					if !mode.autoscaling {
						frc.Spec.PrimaryCluster.EnableInTreeAutoscaling = new(false)
						frc.Spec.PrimaryCluster.AutoscalerOptions = nil
					}
					groups, index := &frc.Spec.PrimaryCluster.WorkerGroups, 0
					if scope == "member" {
						groups, index = &frc.Spec.MemberClusters[0].WorkerGroups, 1
					}
					(*groups)[0].ScaleStrategy.WorkersToDelete = []string{"initial-selection"}
					c := fake.NewClientBuilder().WithScheme(scheme).Build()
					r := &FederatedReconciler{Client: c, Reader: c, Scheme: scheme}
					primary, err := r.ensurePrimary(ctx, frc)
					require.NoError(t, err)
					assert.EqualValues(t, 2, *primary.Spec.WorkerGroupSpecs[index].Replicas)
					assert.Equal(t, []string{"initial-selection"}, primary.Spec.WorkerGroupSpecs[index].ScaleStrategy.WorkersToDelete)

					primary.Spec.WorkerGroupSpecs[index].Replicas = target.replicas
					primary.Spec.WorkerGroupSpecs[index].ScaleStrategy = rayv1.ScaleStrategy{}
					require.NoError(t, c.Update(ctx, primary))
					(*groups)[0].Replicas = new(int32(8))
					added := (*groups)[0].DeepCopy()
					added.GroupName = "added-workers"
					*groups = append(*groups, *added)
					primary, err = r.ensurePrimary(ctx, frc)
					require.NoError(t, err)
					for _, group := range primary.Spec.WorkerGroupSpecs {
						switch group.GroupName {
						case (*groups)[0].GroupName:
							assert.Equal(t, target.replicas, group.Replicas, "preserve PRC intent exactly, including nil, zero and values outside current bounds")
							assert.Empty(t, group.ScaleStrategy.WorkersToDelete, "do not replay a consumed deletion list")
						case added.GroupName:
							assert.EqualValues(t, 8, *group.Replicas, "initialize newly added groups")
							assert.Equal(t, added.ScaleStrategy, group.ScaleStrategy)
						}
					}
					require.Len(t, primary.Spec.WorkerGroupSpecs, 3)
					version := primary.ResourceVersion
					primary, err = r.ensurePrimary(ctx, frc)
					require.NoError(t, err)
					assert.Equal(t, version, primary.ResourceVersion, "reconciliation is idempotent")
				})
			}
		}
	}
}

func TestPrimaryDriftRepairDoesNotLoseConcurrentScaling(t *testing.T) {
	ctx, scheme, frc := context.Background(), testScheme(t), autoscalingFederation()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FederatedReconciler{Client: c, Reader: c, Scheme: scheme}
	primary, err := r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	primary.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Image = "drift"
	require.NoError(t, c.Update(ctx, primary))
	r.Client = interceptor.NewClient(c, interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, options ...client.PatchOption) error {
		latest := &rayv1.RayCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), latest))
		latest.Spec.WorkerGroupSpecs[0].Replicas = new(int32(4))
		latest.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{"concurrent-selection"}
		require.NoError(t, c.Update(ctx, latest))
		return c.Patch(ctx, obj, patch, options...)
	}})
	_, err = r.ensurePrimary(ctx, frc)
	require.True(t, apierrors.IsConflict(err), "retry instead of overwriting a newer scaling decision: %v", err)
	r.Client = c
	primary, err = r.ensurePrimary(ctx, frc)
	require.NoError(t, err)
	assert.EqualValues(t, 4, *primary.Spec.WorkerGroupSpecs[0].Replicas)
	assert.Equal(t, []string{"concurrent-selection"}, primary.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete)
	assert.NotEqual(t, "drift", primary.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Image)
}
