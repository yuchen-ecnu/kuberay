package federation

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func sizedSnapshot(count int) autoscalerSnapshot {
	snapshot := autoscalerSnapshot{SchemaVersion: 2, ObservedAt: time.Now(), Pods: make([]corev1.Pod, count)}
	for i := range snapshot.Pods {
		snapshot.Pods[i] = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("worker-%d", i), Namespace: "member"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	}
	return snapshot
}

func TestSnapshotCollectionBudgetAndSize(t *testing.T) {
	snapshot := sizedSnapshot(1)
	_, err := encodeAutoscalerSnapshot(&snapshot, snapshot.ObservedAt.Add(31*time.Second))
	require.NoError(t, err)
	require.Contains(t, snapshot.Error, "collection budget")
	require.Empty(t, snapshot.Pods)

	// Find the exact serialized boundary. Oversized observations fail as a whole;
	// they must never present the autoscaler with a truncated worker inventory.
	previous := 0
	for count := 1000; count <= 10000; count += 1000 {
		snapshot = sizedSnapshot(count)
		data, err := encodeAutoscalerSnapshot(&snapshot, snapshot.ObservedAt)
		require.NoError(t, err)
		if snapshot.Error != "" {
			require.Contains(t, snapshot.Error, "900 KiB")
			require.Empty(t, snapshot.Pods)
			require.Less(t, len(data), 1024)
			t.Logf("synthetic minimal Pods: %d accepted, %d exceed limit", previous, count)
			return
		}
		require.Len(t, snapshot.Pods, count)
		previous = count
	}
	t.Fatal("test did not reach the snapshot size limit")
}

func BenchmarkSnapshotEncoding(b *testing.B) {
	for _, count := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			snapshot := sizedSnapshot(count)
			b.ResetTimer()
			for b.Loop() {
				data, err := encodeAutoscalerSnapshot(&snapshot, snapshot.ObservedAt)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(data)), "bytes/snapshot")
			}
		})
	}
}
