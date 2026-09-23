"""Run ordering and recovery regressions against the full native Ray state machine."""

import time
import unittest
from queue import Queue
from unittest.mock import Mock

import test_federation_autoscaler as fixtures
from federation_autoscaler import (
    SnapshotUnavailable,
    guard_reconcile,
    guarded_ray_stopper,
)


@unittest.skipIf(fixtures.ray is None, "Run in the supported Ray image")
class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        from ray.autoscaler._private.kuberay.autoscaling_config import (
            _derive_autoscaling_config_from_ray_cr,
        )
        from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
        from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
        from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
        from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
        from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
        from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
            CloudInstanceUpdater,
        )
        from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
            CloudResourceMonitor,
        )
        from ray.autoscaler.v2.scheduler import ResourceDemandScheduler
        from ray.core.generated.autoscaler_pb2 import NodeState, NodeStatus

        self.api = fixtures.RayProviderContractTests()
        self.api.setUp()
        self.addCleanup(self.api.doCleanups)
        template = {
            "spec": {
                "containers": [{"resources": {"limits": {"cpu": "1", "memory": "1Gi"}}}]
            }
        }
        self.api.primary["spec"]["headGroupSpec"] = {
            "rayStartParams": {"num-cpus": "0"},
            "template": template,
        }
        self.api.primary["spec"]["autoscalerOptions"]["idleTimeoutSeconds"] = 1
        for group in self.api.primary["spec"]["workerGroupSpecs"]:
            group["template"] = template
        self.head_pod = fixtures.pod("head", "headgroup")
        self.head_pod["metadata"]["labels"]["ray.io/node-type"] = "head"
        self.head = NodeState(
            node_id=b"\x02" * 28,
            instance_id="head",
            ray_node_type_name="headgroup",
            status=NodeStatus.RUNNING,
            total_resources={"node:__internal_head__": 1},
        )
        self.worker = NodeState(
            node_id=b"\x01" * 28,
            instance_id="member-b/remote-new",
            ray_node_type_name="remote",
            status=NodeStatus.RUNNING,
        )
        self.storage = InstanceStorage("federation-regression", InMemoryStorage())
        self.manager = InstanceManager(
            self.storage, [CloudInstanceUpdater(self.api.provider)]
        )
        self.arguments = dict(
            instance_manager=self.manager,
            scheduler=ResourceDemandScheduler(),
            cloud_provider=self.api.provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            autoscaling_config=AutoscalingConfig(
                _derive_autoscaling_config_from_ray_cr(self.api.primary)
            ),
        )
        self.reconcile = guard_reconcile(Reconciler.reconcile)

    def update(self, cloud=None):
        from ray.core.generated.autoscaler_pb2 import ClusterResourceState

        return self.reconcile(
            **self.arguments,
            ray_cluster_resource_state=ClusterResourceState(
                node_states=[self.head, self.worker]
            ),
            non_terminated_cloud_instances=(
                self.api.provider.get_non_terminated() if cloud is None else cloud
            )
        )

    def test_live_node_missing_from_snapshot_never_enters_terminated_state(self):
        from ray.core.generated.instance_manager_pb2 import Instance

        self.api.inventory["pods"] = [self.head_pod]
        for age in (2, 7):
            self.api.inventory["observedAt"] = fixtures.timestamp(fixtures.NOW - age)
            before = self.storage.get_instances()
            with self.assertRaisesRegex(SnapshotUnavailable, "live GCS instance"):
                self.update()
            self.assertEqual(self.storage.get_instances(), before)
            self.assertEqual(self.api.patches, [])

        self.api.inventory["pods"].append(fixtures.pod("remote-new"))
        self.api.inventory["observedAt"] = fixtures.timestamp()
        for _ in range(2):
            self.update()
        workers = [
            i
            for i in self.storage.get_instances()[0].values()
            if i.cloud_instance_id == self.worker.instance_id
        ]
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0].status, Instance.RAY_RUNNING)
        # Moving time forward cannot expose an orphan ALLOCATED instance.
        for instance in self.storage.get_instances()[0].values():
            for transition in instance.status_history:
                transition.timestamp_ns = time.time_ns() - 3601 * 10**9
            self.storage.upsert_instance(instance)
        self.update()
        self.assertEqual(self.api.patches, [])

    def test_pending_launch_survives_snapshot_lag(self):
        from ray.autoscaler.v2.instance_manager.common import InstanceUtil
        from ray.core.generated.instance_manager_pb2 import Instance

        requested = InstanceUtil.new_instance("requested", "remote", Instance.REQUESTED)
        requested.launch_request_id, requested.node_kind = "launch", 2
        self.storage.upsert_instance(requested)
        self.test_live_node_missing_from_snapshot_never_enters_terminated_state()

    def test_real_deletion_converges_after_gcs_declares_node_dead(self):
        from ray.core.generated.autoscaler_pb2 import NodeStatus
        from ray.core.generated.instance_manager_pb2 import Instance

        self.api.inventory["pods"] = [self.head_pod, fixtures.pod("remote-new")]
        self.update()
        self.api.inventory["pods"] = [self.head_pod]
        with self.assertRaises(SnapshotUnavailable):
            self.update()
        self.worker.status = NodeStatus.DEAD
        self.update()
        workers = [
            i
            for i in self.storage.get_instances()[0].values()
            if i.cloud_instance_id == self.worker.instance_id
        ]
        self.assertEqual([i.status for i in workers], [Instance.TERMINATED])

    def test_outage_after_provider_list_blocks_async_drain(self):
        from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
        from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import (
            RayStopper,
        )
        from ray.core.generated.autoscaler_pb2 import NodeStatus

        gcs, errors = Mock(), Queue()
        stopper = guarded_ray_stopper(RayStopper, lambda: self.api.client._snapshot())(
            gcs, errors
        )
        self.addCleanup(stopper._executor.shutdown, wait=True)
        self.manager = InstanceManager(self.storage, [stopper])
        self.arguments["instance_manager"] = self.manager
        self.api.inventory["pods"] = [self.head_pod, fixtures.pod("remote-new")]
        self.worker.total_resources["CPU"] = 1
        self.worker.available_resources["CPU"] = 1
        self.update()
        cloud = self.api.provider.get_non_terminated()
        self.api.inventory["error"] = "member API unavailable after list"
        self.worker.status, self.worker.idle_duration_ms = NodeStatus.IDLE, 600000
        self.update(cloud)
        stopper._executor.shutdown(wait=True)
        gcs.drain_node.assert_not_called()
        gcs.drain_nodes.assert_not_called()
        self.assertFalse(errors.empty())
        self.assertEqual(self.api.patches, [])


if __name__ == "__main__":
    unittest.main()
