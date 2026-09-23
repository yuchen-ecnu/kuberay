"""Contract tests for the federation inventory adapter, without a Ray install."""

import copy
import datetime
import json
import os
import types
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

from federation_autoscaler import (
    ADAPTER_REVISION,
    FEDERATION_MANAGER,
    MEMBER_LABEL,
    MEMBERS_ANNOTATION,
    SnapshotUnavailable,
    make_client_class,
    validate_ray_version,
    run_with_startup_retry,
)


NOW = 1_789_000_000


def timestamp(seconds=NOW):
    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).isoformat()


def primary():
    return {
        "metadata": {
            "name": "demo",
            "namespace": "ns",
            "uid": "primary-uid",
            "generation": 3,
            "resourceVersion": "13",
            "annotations": {MEMBERS_ANNOTATION: json.dumps({"remote": "member-b"})},
        },
        "spec": {
            "enableInTreeAutoscaling": True,
            "autoscalerOptions": {"version": "v2"},
            "workerGroupSpecs": [
                {"groupName": "local", "replicas": 1},
                {"groupName": "remote", "replicas": 1, "managedBy": FEDERATION_MANAGER},
            ],
        },
    }


def pod(name, group="remote", **metadata):
    labels = {"ray.io/node-type": "worker", "ray.io/group": group}
    member = {"remote": "member-b", "remote-c": "member-c"}.get(group)
    if member:
        labels[MEMBER_LABEL] = member
    return {
        "metadata": {
            "name": name,
            "labels": labels,
            **metadata,
        },
        "status": {
            "phase": "Running",
            "podIP": "10.0.0.1",
            "containerStatuses": [{"state": {"running": {}}}],
        },
    }


class NativeClient:
    def __init__(self, namespace, kuberay_crd_version):
        self._namespace, self._kuberay_crd_version = namespace, kuberay_crd_version
        self.primary = primary()
        self.native_gets = []
        self.native_patches = []

    def _get_refreshed_headers_and_verify(self):
        return {"Authorization": "Bearer rotating-token"}, "/serviceaccount/ca.crt"

    def get(self, path):
        self.native_gets.append(path)
        if path.startswith("pods/"):
            return {"metadata": {"resourceVersion": "17"}}
        return copy.deepcopy(self.primary)

    def patch(self, path, payload, content_type):
        self.native_patches.append((path, payload, content_type))
        result = copy.deepcopy(self.primary)
        result["metadata"]["resourceVersion"] = "14"
        result["metadata"]["generation"] += 1
        return result


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(
            os.environ,
            {
                "RAY_CLUSTER_NAME": "demo",
                "RAY_CLUSTER_NAMESPACE": "ns",
                "KUBERAY_FEDERATION_SNAPSHOT": "demo-autoscaler",
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.inventory = {
            "schemaVersion": 2,
            "adapterRevision": ADAPTER_REVISION,
            "primaryUID": "primary-uid",
            "primaryGeneration": 3,
            "observedAt": timestamp(),
            "pods": [pod("local-1", "local"), pod("remote-1")],
        }
        self.response = Mock()
        self.response.json.side_effect = lambda: {
            "metadata": {"resourceVersion": "snapshot-7"},
            "data": {"snapshot.json": json.dumps(self.inventory)},
        }
        self.transport = Mock()
        self.transport.get.return_value = self.response
        self.native = types.SimpleNamespace(
            KubernetesHttpApiClient=NativeClient,
            KUBERAY_CRD_VER="v1",
            KUBERAY_REQUEST_TIMEOUT_S=60,
            url_from_resource=lambda namespace, path, kuberay_crd_version: (
                "https://kubernetes.default/api/v1/namespaces/" + namespace + "/" + path
            ),
        )
        self.client = make_client_class(
            self.native, clock=lambda: NOW, transport=self.transport
        )("ns")
        self.client.get("rayclusters/demo")

    def listing(self):
        return self.client.get(
            "pods?labelSelector=ray.io%2Fcluster%3Ddemo&resourceVersion=100000&resourceVersionMatch=NotOlderThan"
        )

    def replica_patch(self):
        return [
            {"op": "replace", "path": "/spec/workerGroupSpecs/1/replicas", "value": 2}
        ]

    def test_merged_list_keeps_cloud_ids_and_never_forwards_cluster_resource_version(
        self,
    ):
        result = self.listing()
        self.assertEqual(
            [p["metadata"]["name"] for p in result["items"]],
            ["local-1", "member-b/remote-1"],
        )
        self.assertEqual(result["metadata"]["resourceVersion"], "snapshot-7")
        self.assertEqual(self.client.native_gets, ["rayclusters/demo"])
        self.transport.get.assert_called_once_with(
            "https://kubernetes.default/api/v1/namespaces/ns/configmaps/demo-autoscaler",
            headers={"Authorization": "Bearer rotating-token"},
            verify="/serviceaccount/ca.crt",
            timeout=60,
        )

    def test_pending_and_terminating_pods_preserve_native_provider_semantics(self):
        self.inventory["pods"] = [
            pod("pending"),
            pod("terminating", deletionTimestamp=timestamp()),
        ]
        self.inventory["pods"][0]["status"] = {"phase": "Pending"}
        expected = copy.deepcopy(self.inventory["pods"])
        for item in expected:
            item["metadata"]["name"] = "member-b/" + item["metadata"]["name"]
        self.assertEqual(self.listing()["items"], expected)

    def test_local_head_get_stays_native(self):
        self.assertEqual(
            self.client.get("pods/head")["metadata"]["resourceVersion"], "17"
        )
        self.transport.get.assert_not_called()

    def test_unavailable_or_inconsistent_snapshot_never_returns_partial_list(self):
        cases = [
            {"error": "member unreachable"},
            {"primaryUID": "recreated-primary"},
            {"primaryGeneration": 2},
            {"primaryGeneration": True},
            {"schemaVersion": 3},
            {"adapterRevision": "previous-adapter"},
            {"observedAt": timestamp(NOW - 31)},
            {"observedAt": timestamp(NOW + 6)},
            {"observedAt": "invalid"},
            {"observedAt": "2026-09-23T08:00:00"},
            {"pods": None},
            {"pods": [pod("duplicate"), pod("duplicate")]},
            {"pods": [pod("other", "unknown-group")]},
        ]
        original = copy.deepcopy(self.inventory)
        for changes in cases:
            with self.subTest(changes=changes):
                self.inventory = {**original, **changes}
                with self.assertRaises(SnapshotUnavailable):
                    self.listing()
        self.assertEqual(self.client.native_patches, [])

    def test_snapshot_nanosecond_timestamp_and_small_clock_skew(self):
        self.inventory["observedAt"] = timestamp(NOW + 2).replace(
            "+00:00", ".123456789Z"
        )
        self.assertEqual(len(self.listing()["items"]), 2)

    def test_snapshot_api_failure_propagates(self):
        self.response.raise_for_status.side_effect = RuntimeError("API unavailable")
        with self.assertRaisesRegex(RuntimeError, "API unavailable"):
            self.listing()

    def test_primary_must_be_read_first_and_deleting_primary_is_rejected(self):
        client = make_client_class(
            self.native, clock=lambda: NOW, transport=self.transport
        )("ns")
        with self.assertRaises(SnapshotUnavailable):
            client.get("pods?labelSelector=ray.io%2Fcluster%3Ddemo")
        client.primary["metadata"]["deletionTimestamp"] = timestamp()
        with self.assertRaisesRegex(SnapshotUnavailable, "being deleted"):
            client.get("rayclusters/demo")

    def test_list_rejects_another_cluster_selector(self):
        with self.assertRaises(ValueError):
            self.client.get("pods?labelSelector=ray.io%2Fcluster%3Dother")

    def test_scaling_patch_guards_observed_identity_and_resource_version(self):
        payload = self.replica_patch()
        self.client.patch("rayclusters/demo", payload)
        sent = self.client.native_patches[0][1]
        self.assertEqual(
            sent[:2],
            [
                {"op": "test", "path": "/metadata/uid", "value": "primary-uid"},
                {"op": "test", "path": "/metadata/resourceVersion", "value": "13"},
            ],
        )
        self.assertEqual(sent[2:], payload)
        self.assertEqual(len(payload), 1)
        with self.assertRaises(SnapshotUnavailable):
            self.client.patch("rayclusters/demo", payload)
        self.inventory["primaryGeneration"] = 4
        self.client.patch("rayclusters/demo", payload)
        self.assertEqual(self.client.native_patches[1][1][1]["value"], "14")

    def test_precise_downscale_and_delete_cleanup_are_allowed(self):
        for names in (["member-b/remote-1"], []):
            with self.subTest(names=names):
                self.client.get("rayclusters/demo")
                self.client.patch(
                    "rayclusters/demo",
                    [
                        {
                            "op": "replace",
                            "path": "/spec/workerGroupSpecs/1/scaleStrategy",
                            "value": {"workersToDelete": names},
                        }
                    ],
                )
        self.assertEqual(len(self.client.native_patches), 2)
        self.assertEqual(
            self.client.native_patches[0][1][-1]["value"],
            {"workersToDelete": ["remote-1"]},
        )

    def test_primary_view_qualifies_deletions_without_mutating_api_resource(self):
        self.client.primary["spec"]["workerGroupSpecs"][1]["scaleStrategy"] = {
            "workersToDelete": ["remote-1"]
        }
        result = self.client.get("rayclusters/demo")
        self.assertEqual(
            result["spec"]["workerGroupSpecs"][1]["scaleStrategy"],
            {"workersToDelete": ["member-b/remote-1"]},
        )
        self.assertEqual(
            self.client.primary["spec"]["workerGroupSpecs"][1]["scaleStrategy"],
            {"workersToDelete": ["remote-1"]},
        )
        self.assertEqual(
            self.client._observed_primary["spec"]["workerGroupSpecs"][1][
                "scaleStrategy"
            ],
            {"workersToDelete": ["remote-1"]},
        )
        self.transport.get.assert_not_called()

    def test_member_mapping_must_match_exactly_the_delegated_groups(self):
        for value in (
            None,
            "invalid",
            "null",
            "[]",
            "{}",
            '{"remote":"member-b", "local":"member-c"}',
            '{"remote":"bad/member"}',
            '{"remote":42}',
        ):
            with self.subTest(value=value):
                self.client.primary = primary()
                if value is None:
                    self.client.primary["metadata"]["annotations"] = {}
                else:
                    self.client.primary["metadata"]["annotations"][
                        MEMBERS_ANNOTATION
                    ] = value
                with self.assertRaises(SnapshotUnavailable):
                    self.client.get("rayclusters/demo")
                self.assertIsNone(self.client._observed_primary)

    def test_same_pod_name_in_local_and_remote_cluster_has_distinct_identity(self):
        self.inventory["pods"] = [pod("shared", "local"), pod("shared")]
        self.assertEqual(
            [p["metadata"]["name"] for p in self.listing()["items"]],
            ["shared", "member-b/shared"],
        )

    def test_snapshot_cannot_reassign_pod_to_a_different_member(self):
        for member in (None, "member-c"):
            with self.subTest(member=member):
                item = pod("remote-1")
                if member is None:
                    del item["metadata"]["labels"][MEMBER_LABEL]
                else:
                    item["metadata"]["labels"][MEMBER_LABEL] = member
                self.inventory["pods"] = [item]
                with self.assertRaisesRegex(SnapshotUnavailable, "member identity"):
                    self.listing()

    def test_downscale_ids_must_belong_to_the_group_member(self):
        for index, name in (
            (1, "remote-1"),
            (1, "member-c/remote-1"),
            (1, "member-b/"),
            (1, "member-b/member-b/remote-1"),
            (0, "member-b/local-1"),
        ):
            with self.subTest(index=index, name=name), self.assertRaises(ValueError):
                self.client.patch(
                    "rayclusters/demo",
                    [
                        {
                            "op": "replace",
                            "path": f"/spec/workerGroupSpecs/{index}/scaleStrategy",
                            "value": {"workersToDelete": [name]},
                        }
                    ],
                )
        self.assertEqual(self.client.native_patches, [])

    def test_snapshot_is_rechecked_before_write(self):
        self.listing()
        self.inventory["error"] = "member stopped responding"
        with self.assertRaises(SnapshotUnavailable):
            self.client.patch("rayclusters/demo", self.replica_patch())
        self.assertEqual(self.client.native_patches, [])

    def test_only_replica_intent_can_be_written(self):
        operations = [
            {"op": "replace", "path": "/metadata/annotations", "value": {}},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/1/groupName",
                "value": "other",
            },
            {"op": "replace", "path": "/spec/workerGroupSpecs/2/replicas", "value": 1},
            {"op": "replace", "path": "/spec/workerGroupSpecs/1/replicas", "value": -1},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/1/replicas",
                "value": True,
            },
            {"op": "remove", "path": "/spec/workerGroupSpecs/1/replicas"},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/1/scaleStrategy",
                "value": {"workersToDelete": "pod"},
            },
        ]
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                self.client.patch("rayclusters/demo", [operation])
        with self.assertRaises(ValueError):
            self.client.patch("pods/remote-1/resize", self.replica_patch())
        with self.assertRaises(ValueError):
            self.client.patch("rayclusters/other", self.replica_patch())
        with self.assertRaises(ValueError):
            self.client.patch("rayclusters/demo", {}, "application/merge-patch+json")
        self.assertEqual(self.client.native_patches, [])

    def test_version_contract_is_explicit(self):
        for version in ("2.56.0",):
            validate_ray_version(version)
        for version in ("2.56.1", "2.55.0", "2.57.0", "2.56.0.dev0", "unknown"):
            with self.subTest(version=version), self.assertRaises(RuntimeError):
                validate_ray_version(version)

    def test_startup_recovers_after_transient_api_failures(self):
        import requests

        start = Mock(side_effect=[requests.Timeout(), requests.ConnectionError(), None])
        with patch("federation_autoscaler.time.sleep") as sleep:
            run_with_startup_retry(start)
        self.assertEqual(start.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        with self.assertRaises(ValueError):
            run_with_startup_retry(Mock(side_effect=ValueError("invalid config")))

    def test_invalid_runtime_config_is_rejected_before_scaling(self):
        for change in (
            {"enableInTreeAutoscaling": False},
            {"autoscalerOptions": {"version": "v1"}},
            {"autoscalerOptions": {}},
        ):
            with self.subTest(change=change):
                self.client.primary = primary()
                self.client.primary["spec"].update(change)
                with self.assertRaisesRegex(
                    ValueError, "requires enableInTreeAutoscaling"
                ):
                    self.client.get("rayclusters/demo")
        self.client.primary = primary()
        self.client.primary["metadata"]["annotations"] = {"ray.io/ippr": "{}"}
        with self.assertRaisesRegex(ValueError, "in-place Pod resizing"):
            self.client.get("rayclusters/demo")

    def test_malformed_snapshot_is_not_returned(self):
        for value in (
            "not-json",
            "null",
            "[]",
            "{}",
            json.dumps({**self.inventory, "observedAt": None}),
            json.dumps({**self.inventory, "schemaVersion": True}),
        ):
            with self.subTest(value=value):
                self.response.json.side_effect = None
                self.response.json.return_value = {
                    "metadata": {"resourceVersion": "1"},
                    "data": {"snapshot.json": value},
                }
                with self.assertRaises(SnapshotUnavailable):
                    self.listing()


try:
    import ray
except ImportError:
    ray = None


@unittest.skipIf(ray is None, "Run in the Ray 2.56 image for native provider contracts")
class RayProviderContractTests(unittest.TestCase):
    """Run the actual stock provider over the adapter with an in-memory API."""

    def setUp(self):
        from ray.autoscaler._private.kuberay import node_provider
        from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay import (
            cloud_provider,
        )

        validate_ray_version(ray.__version__)
        self.native = node_provider
        self.cloud = cloud_provider
        self.env = patch.dict(
            os.environ,
            {
                "RAY_CLUSTER_NAME": "demo",
                "RAY_CLUSTER_NAMESPACE": "ns",
                "KUBERAY_FEDERATION_SNAPSHOT": "demo-autoscaler",
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.primary = primary()
        self.primary["spec"]["enableInTreeAutoscaling"] = True
        self.primary["spec"]["autoscalerOptions"] = {"version": "v2"}
        for group in self.primary["spec"]["workerGroupSpecs"]:
            group.update(minReplicas=0, maxReplicas=4)
        self.inventory = {
            "schemaVersion": 2,
            "adapterRevision": ADAPTER_REVISION,
            "primaryUID": "primary-uid",
            "primaryGeneration": 3,
            "observedAt": timestamp(),
            "pods": [pod("local-1", "local"), pod("remote-1")],
        }
        self.patches = []
        auth = patch.object(node_provider, "load_k8s_secrets", return_value=({}, True))
        auth.start()
        self.addCleanup(auth.stop)
        get = patch.object(node_provider.requests, "get", side_effect=self.get)
        get.start()
        self.addCleanup(get.stop)
        write = patch.object(node_provider.requests, "patch", side_effect=self.write)
        write.start()
        self.addCleanup(write.stop)
        self.client_type = make_client_class(node_provider, clock=lambda: NOW)
        self.client = self.client_type("ns")
        self.provider = cloud_provider.KubeRayProvider(
            "demo", {"namespace": "ns"}, Mock(), k8s_api_client=self.client
        )

    def get(self, url, **kwargs):
        response = Mock(status_code=200)
        if "/configmaps/" in url:
            value = {
                "metadata": {"resourceVersion": "snapshot-7"},
                "data": {"snapshot.json": json.dumps(self.inventory)},
            }
        elif "/rayclusters/demo" in url:
            value = self.primary
        else:
            value = {"metadata": {"resourceVersion": "15"}}
        response.json.return_value = copy.deepcopy(value)
        return response

    def write(self, url, body, **kwargs):
        payload = json.loads(body)
        self.patches.append(payload)
        self.assertEqual(payload[0]["value"], self.primary["metadata"]["uid"])
        self.assertEqual(
            payload[1]["value"], self.primary["metadata"]["resourceVersion"]
        )
        for operation in payload[2:]:
            _, _, _, index, field = operation["path"].split("/")
            self.primary["spec"]["workerGroupSpecs"][int(index)][field] = operation[
                "value"
            ]
        self.primary["metadata"]["generation"] += 1
        self.primary["metadata"]["resourceVersion"] = str(
            int(self.primary["metadata"]["resourceVersion"]) + 1
        )
        result = Mock(status_code=200)
        result.json.return_value = copy.deepcopy(self.primary)
        return result

    def refresh_snapshot(self):
        self.inventory["primaryGeneration"] = self.primary["metadata"]["generation"]

    def test_native_v2_lists_launches_and_terminates_remote_workers(self):
        instances = self.provider.get_non_terminated()
        self.assertEqual(set(instances), {"local-1", "member-b/remote-1"})
        self.assertEqual(instances["member-b/remote-1"].node_type, "remote")
        self.provider.launch({"remote": 1}, "launch-1")
        self.assertEqual(self.provider.poll_errors(), [])
        self.assertEqual(self.primary["spec"]["workerGroupSpecs"][1]["replicas"], 2)
        self.refresh_snapshot()
        self.inventory["pods"].append(pod("remote-2"))
        self.provider.terminate(["member-b/remote-1"], "terminate-1")
        self.assertEqual(self.provider.poll_errors(), [])
        remote = self.primary["spec"]["workerGroupSpecs"][1]
        self.assertEqual(remote["replicas"], 1)
        self.assertEqual(remote["scaleStrategy"], {"workersToDelete": ["remote-1"]})

    def test_native_two_members_with_same_pod_names_are_independently_deleted(self):
        self.primary["spec"]["workerGroupSpecs"].append(
            {
                "groupName": "remote-c",
                "managedBy": FEDERATION_MANAGER,
                "replicas": 1,
                "minReplicas": 0,
                "maxReplicas": 4,
            }
        )
        self.primary["metadata"]["annotations"][MEMBERS_ANNOTATION] = json.dumps(
            {"remote": "member-b", "remote-c": "member-c"}
        )
        self.inventory["pods"] = [
            pod("shared", "local"),
            pod("shared"),
            pod("shared", "remote-c"),
        ]
        instances = self.provider.get_non_terminated()
        self.assertEqual(
            set(instances), {"shared", "member-b/shared", "member-c/shared"}
        )
        self.provider.terminate(["member-c/shared"], "terminate-c")
        self.assertEqual(self.provider.poll_errors(), [])
        groups = self.primary["spec"]["workerGroupSpecs"]
        self.assertEqual(groups[2]["replicas"], 0)
        self.assertEqual(groups[2]["scaleStrategy"], {"workersToDelete": ["shared"]})
        self.assertEqual(groups[1]["replicas"], 1)
        self.assertNotIn("scaleStrategy", groups[1])

    def test_native_pending_delete_blocks_launch_until_snapshot_confirms_removal(self):
        self.primary["spec"]["workerGroupSpecs"][1]["scaleStrategy"] = {
            "workersToDelete": ["remote-1"]
        }
        self.provider.launch({"remote": 1}, "blocked")
        self.assertEqual(len(self.provider.poll_errors()), 1)
        self.assertEqual(self.patches, [])
        self.inventory["pods"] = [pod("local-1", "local")]
        self.provider.launch({"remote": 1}, "retry")
        self.assertEqual(self.provider.poll_errors(), [])
        self.assertEqual(
            self.primary["spec"]["workerGroupSpecs"][1]["scaleStrategy"],
            {"workersToDelete": []},
        )

    def test_native_stale_inventory_yields_no_scaling_write(self):
        self.inventory["error"] = "member unreachable"
        with self.assertRaises(SnapshotUnavailable):
            self.provider.get_non_terminated()
        self.provider.launch({"remote": 1}, "unavailable")
        self.assertEqual(len(self.provider.poll_errors()), 1)
        self.assertEqual(self.patches, [])

    def test_runner_patches_both_import_boundaries_and_uses_native_entrypoint(self):
        from ray.autoscaler._private.kuberay import run_autoscaler
        from ray.autoscaler._private.kuberay.autoscaling_config import (
            AutoscalingConfigProducer,
        )
        from federation_autoscaler import main
        from ray.autoscaler.v2 import autoscaler
        from ray.autoscaler.v2.instance_manager.reconciler import Reconciler

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(Reconciler, "reconcile", Reconciler.reconcile)
            )
            stack.enter_context(
                patch.object(autoscaler, "RayStopper", autoscaler.RayStopper)
            )
            stack.enter_context(
                patch.object(
                    self.native,
                    "KubernetesHttpApiClient",
                    self.native.KubernetesHttpApiClient,
                )
            )
            stack.enter_context(
                patch.object(
                    self.cloud,
                    "KubernetesHttpApiClient",
                    self.cloud.KubernetesHttpApiClient,
                )
            )
            stack.enter_context(
                patch.object(run_autoscaler, "Monitor", run_autoscaler.Monitor)
            )
            run = stack.enter_context(
                patch.object(run_autoscaler, "run_kuberay_autoscaler")
            )
            main()
            self.assertIs(
                self.native.KubernetesHttpApiClient, self.cloud.KubernetesHttpApiClient
            )
            producer = AutoscalingConfigProducer("demo", "ns")
            self.assertIsInstance(
                producer.kubernetes_api_client, self.native.KubernetesHttpApiClient
            )
            self.assertEqual(
                producer._fetch_ray_cr_from_k8s_with_retries()["metadata"]["uid"],
                "primary-uid",
            )
            run.assert_called_once_with("demo", "ns")
            with self.assertRaisesRegex(RuntimeError, "head must run autoscaler v2"):
                run_autoscaler.Monitor()


if __name__ == "__main__":
    unittest.main()
