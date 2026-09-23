package federation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type snapshotReadMeter struct {
	client.Client
	delay time.Duration
	reads int
}

func (m *snapshotReadMeter) beforeRead(ctx context.Context) error {
	m.reads++
	if m.delay == 0 {
		return nil
	}
	timer := time.NewTimer(m.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *snapshotReadMeter) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if err := m.beforeRead(ctx); err != nil {
		return err
	}
	return m.Client.Get(ctx, key, object, opts...)
}

func (m *snapshotReadMeter) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := m.beforeRead(ctx); err != nil {
		return err
	}
	return m.Client.List(ctx, list, opts...)
}

// This is the actual collector with fake API storage and injected per-read
// latency. Report remote reads separately from production API-server QPS: Secret
// lookup, client construction, ordinary reconciliation and transport are absent.
// Run once with -run '^$' -bench BenchmarkSnapshotCollection -benchtime=1x.
func BenchmarkSnapshotCollection(b *testing.B) {
	for _, tc := range []struct {
		members int
		delay   time.Duration
	}{
		{1, 10 * time.Millisecond},
		{4, 10 * time.Millisecond},
		{16, 10 * time.Millisecond},
		{16, 650 * time.Millisecond},
	} {
		b.Run(fmt.Sprintf("members=%d/read=%s", tc.members, tc.delay), func(b *testing.B) {
			ctx, scheme, frc := context.Background(), runtime.NewScheme(), autoscalingFederation()
			require.NoError(b, corev1.AddToScheme(scheme))
			require.NoError(b, rayv1.AddToScheme(scheme))
			prototype := frc.Spec.MemberClusters[0].DeepCopy()
			frc.Spec.MemberClusters = nil
			for i := range tc.members {
				member := prototype.DeepCopy()
				member.Name, member.WorkerGroups[0].GroupName = fmt.Sprintf("member-%d", i), fmt.Sprintf("workers-%d", i)
				frc.Spec.MemberClusters = append(frc.Spec.MemberClusters, *member)
			}
			local := fake.NewClientBuilder().WithScheme(scheme).Build()
			remote := &snapshotReadMeter{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(testMemberClusterIdentity()).Build(), delay: tc.delay}
			r := &FederatedReconciler{Client: local, Reader: local, Scheme: scheme, Members: &fixedMembers{clients: map[string]client.Client{"member-credential": remote}}}
			primary, err := r.ensurePrimary(ctx, frc)
			require.NoError(b, err)
			primary.UID = "primary-uid"
			require.NoError(b, local.Update(ctx, primary))
			for _, member := range frc.Spec.MemberClusters {
				mrc := desiredMember(frc, primary, member)
				mrc.UID = types.UID(member.Name)
				require.NoError(b, remote.Create(ctx, mrc))
				frc.Status.MemberClusterStatuses = append(frc.Status.MemberClusterStatuses, rayv1.FederationMemberStatus{
					Name: member.Name, Namespace: member.Namespace, RayClusterName: mrc.Name,
					ClusterUID: testMemberClusterUID, KubeconfigSecretRef: member.KubeconfigSecretRef.DeepCopy(),
				})
				for i := range 100 {
					require.NoError(b, remote.Create(ctx, autoscalerTestPod(mrc, fmt.Sprintf("%s-%d", member.Name, i), "worker", member.WorkerGroups[0].GroupName)))
				}
			}
			b.ResetTimer()
			var last autoscalerSnapshot
			var elapsed time.Duration
			for b.Loop() {
				last = autoscalerSnapshot{SchemaVersion: 2, ObservedAt: time.Now()}
				require.NoError(b, r.collectAutoscalerPods(ctx, frc, &last))
				elapsed = time.Since(last.ObservedAt)
				data, err := encodeAutoscalerSnapshot(&last, time.Now())
				require.NoError(b, err)
				b.ReportMetric(float64(len(data)), "bytes/snapshot")
			}
			b.StopTimer()
			b.ReportMetric(float64(remote.reads)/float64(b.N), "remote-reads/snapshot")
			b.ReportMetric(float64(remote.reads)/float64(b.N)/elapsed.Seconds(), "remote-reads/sec")
			b.ReportMetric(elapsed.Seconds(), "collection-seconds")
			if tc.delay == 650*time.Millisecond {
				require.Contains(b, last.Error, "collection budget")
				require.Empty(b, last.Pods)
			} else {
				require.Empty(b, last.Error)
				require.Len(b, last.Pods, 100*tc.members)
			}
			// Removing the latency restores a full usable observation immediately;
			// an expired sample must not leave permanent error state behind.
			remote.delay = 0
			fresh := autoscalerSnapshot{SchemaVersion: 2, ObservedAt: time.Now()}
			require.NoError(b, r.collectAutoscalerPods(ctx, frc, &fresh))
			_, err = encodeAutoscalerSnapshot(&fresh, time.Now())
			require.NoError(b, err)
			require.Empty(b, fresh.Error)
			require.Len(b, fresh.Pods, 100*tc.members)
		})
	}
}
