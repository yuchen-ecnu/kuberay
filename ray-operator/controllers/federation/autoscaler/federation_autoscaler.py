"""Adapt Ray's KubeRay autoscaler to the federation controller's Pod inventory.

Ray still reads and scales the primary RayCluster through its normal Kubernetes
client. Only Pod listings come from the controller-owned ConfigMap; the head
never receives member kubeconfigs. This adapter deliberately supports the tested
Ray 2.56.0 / autoscaler v2 interface only.
"""

import copy
import datetime
import functools
import hashlib
import inspect
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit


SNAPSHOT_MAX_AGE_SECONDS = 30
MAX_CLOCK_SKEW_SECONDS = 5
MEMBERS_ANNOTATION = "ray.io/federation-autoscaler-members"
MEMBER_LABEL = "ray.io/federation-member"
FEDERATION_MANAGER = "ray.io/federated-raycluster-controller"
ADAPTER_REVISION = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class SnapshotUnavailable(RuntimeError):
    """An incomplete observation must stop this autoscaler iteration."""


def guard_reconcile(reconcile):
    """Check both observation sources before Ray mutates instance state.

    A recent Pod snapshot may predate a node joining GCS. Ray treats missing
    cloud instances as terminated, so TTL and primary generation alone cannot
    make this observation safe. Wait until the snapshot catches up, or GCS marks
    the missing node DEAD after a real deletion. Do not manufacture cloud nodes.
    """
    from ray.core.generated.autoscaler_pb2 import NodeStatus

    signature = inspect.signature(reconcile)

    @functools.wraps(reconcile)
    def guarded(*args, **kwargs):
        arguments = signature.bind(*args, **kwargs).arguments
        cloud = arguments["non_terminated_cloud_instances"]
        for node in arguments["ray_cluster_resource_state"].node_states:
            if node.status != NodeStatus.DEAD and node.instance_id not in cloud:
                raise SnapshotUnavailable(
                    "Waiting for Pod observation of live GCS instance "
                    + (node.instance_id or node.node_id.hex())
                )
        return reconcile(*args, **kwargs)

    return guarded


def guarded_ray_stopper(stopper, check_observation):
    """Recheck observation at the asynchronous GCS action boundary.

    Native RayStopper reports rejected actions through its error queue, allowing
    Ray to retry after observation recovers. An already dispatched RPC cannot be
    recalled; this check deliberately makes no distributed atomicity promise.
    """

    class GuardedGCS:
        def __init__(self, delegate):
            self.delegate = delegate

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        def drain_node(self, *args, **kwargs):
            check_observation()
            return self.delegate.drain_node(*args, **kwargs)

        def drain_nodes(self, *args, **kwargs):
            check_observation()
            return self.delegate.drain_nodes(*args, **kwargs)

    class FederationRayStopper(stopper):
        def __init__(self, gcs_client, error_queue):
            super().__init__(GuardedGCS(gcs_client), error_queue)

    return FederationRayStopper


def validate_ray_version(version):
    if version != "2.56.0":
        raise RuntimeError(
            "Federated autoscaling requires Ray 2.56.0 and autoscaler v2; "
            f"found Ray {version}"
        )


def observed_timestamp(value):
    # Go RFC3339Nano may carry any number of fractional digits up to nine.
    # Python < 3.11 fromisoformat does not accept all those forms.
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
        value,
    )
    if not match:
        raise ValueError("observedAt must be an RFC3339 timestamp with timezone")
    fraction = (match[2] or "").ljust(6, "0")[:6]
    offset = match[3].replace("Z", "+0000").replace(":", "")
    return datetime.datetime.strptime(
        match[1] + "." + fraction + offset, "%Y-%m-%dT%H:%M:%S.%f%z"
    ).timestamp()


def make_client_class(native, *, clock=time.time, transport=None):
    """Keep the native authentication/patch protocol behind one tested boundary.

    ``native`` is ray.autoscaler._private.kuberay.node_provider. Clock and HTTP
    transport are injectable so expiry and API failures can be tested without Ray
    or a Kubernetes installation.
    """
    if transport is None:
        import requests

        transport = requests
    native_client = native.KubernetesHttpApiClient

    class FederationKubernetesHttpApiClient(native_client):
        def __init__(self, namespace, kuberay_crd_version=native.KUBERAY_CRD_VER):
            super().__init__(namespace, kuberay_crd_version)
            self._federation_cluster = os.environ["RAY_CLUSTER_NAME"]
            self._snapshot_name = os.environ["KUBERAY_FEDERATION_SNAPSHOT"]
            if namespace != os.environ["RAY_CLUSTER_NAMESPACE"]:
                raise ValueError(
                    "Federation autoscaler namespace does not match its head"
                )
            if not self._snapshot_name or "/" in self._snapshot_name:
                raise ValueError("Invalid federation snapshot ConfigMap name")
            self._primary_path = "rayclusters/" + self._federation_cluster
            self._observed_primary = None
            self._members = {}

        def get(self, path):
            parsed = urlsplit(path)
            if parsed.path == "pods":
                query = parse_qs(parsed.query)
                expected = "ray.io/cluster=" + self._federation_cluster
                if query.get("labelSelector") != [expected]:
                    raise ValueError(
                        "Federation Pod listing requires its primary cluster selector"
                    )
                # The supplied resourceVersion belongs to the primary API. It
                # cannot constrain a list from a different Kubernetes cluster.
                configmap, snapshot = self._snapshot()
                return {
                    "apiVersion": "v1",
                    "kind": "PodList",
                    "metadata": {
                        "resourceVersion": configmap["metadata"]["resourceVersion"]
                    },
                    "items": self._provider_pods(snapshot["pods"]),
                }
            result = super().get(path)
            if path == self._primary_path:
                self._remember_primary(result)
                return self._provider_primary(result)
            return result

        def patch(self, path, payload, content_type="application/json-patch+json"):
            if (
                path != self._primary_path
                or content_type != "application/json-patch+json"
            ):
                raise ValueError(
                    "Federation autoscaler may only patch primary replica intent"
                )
            raw_payload = self._prepare_patch(payload)
            self._snapshot()
            primary = self._observed_primary["metadata"]
            guarded = [
                {"op": "test", "path": "/metadata/uid", "value": primary["uid"]},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": primary["resourceVersion"],
                },
                *raw_payload,
            ]
            result = super().patch(path, guarded, content_type)
            # A successful write advances RV/generation. A second write must use
            # that RV and wait for a snapshot of the new generation.
            self._remember_primary(result)
            return self._provider_primary(result)

        def _provider_primary(self, primary):
            # Workers-only RayClusters advertise member/pod as their GCS cloud
            # instance ID. Ray's provider uses Pod metadata.name as that ID, so
            # its PRC deletion lists must use the same qualified identities.
            # Persisted PRC/MRC intent continues to contain ordinary Pod names.
            result = copy.deepcopy(primary)
            for group in result.get("spec", {}).get("workerGroupSpecs", []):
                member = self._members.get(group["groupName"])
                names = group.get("scaleStrategy", {}).get("workersToDelete", [])
                for name in names:
                    if not isinstance(name, str) or not name or "/" in name:
                        raise SnapshotUnavailable(
                            "Primary deletion intent must use raw Pod names"
                        )
                if member and names:
                    group["scaleStrategy"]["workersToDelete"] = [
                        member + "/" + name for name in names
                    ]
            return result

        def _provider_pods(self, pods):
            result = copy.deepcopy(pods)
            for pod in result:
                member = pod["metadata"]["labels"].get(MEMBER_LABEL)
                if member:
                    pod["metadata"]["name"] = member + "/" + pod["metadata"]["name"]
            return result

        def _remember_primary(self, primary):
            metadata = primary.get("metadata", {})
            generation = metadata.get("generation")
            if (
                metadata.get("name") != self._federation_cluster
                or metadata.get("namespace") != self._namespace
                or not metadata.get("uid")
                or not metadata.get("resourceVersion")
                or type(generation) is not int
                or generation < 1
            ):
                self._observed_primary = None
                raise SnapshotUnavailable("Primary RayCluster identity is incomplete")
            if metadata.get("deletionTimestamp"):
                self._observed_primary = None
                raise SnapshotUnavailable("Primary RayCluster is being deleted")
            spec = primary.get("spec", {})
            if (
                spec.get("enableInTreeAutoscaling") is not True
                or spec.get("autoscalerOptions", {}).get("version") != "v2"
            ):
                self._observed_primary = None
                raise ValueError(
                    "Federated autoscaling requires enableInTreeAutoscaling=true "
                    "and autoscalerOptions.version=v2"
                )
            if "ray.io/ippr" in metadata.get("annotations", {}):
                self._observed_primary = None
                raise ValueError(
                    "Federated autoscaling does not support in-place Pod resizing"
                )
            self._observed_primary = None
            try:
                members = json.loads(
                    metadata.get("annotations", {})[MEMBERS_ANNOTATION]
                )
                delegated = {
                    group["groupName"]
                    for group in spec.get("workerGroupSpecs", [])
                    if group.get("managedBy") == FEDERATION_MANAGER
                }
                if not isinstance(members, dict) or set(members) != delegated:
                    raise ValueError(
                        "member mapping must match all delegated worker groups"
                    )
                for member in members.values():
                    if (
                        not isinstance(member, str)
                        or len(member) > 63
                        or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", member)
                    ):
                        raise ValueError(
                            "member mapping contains an invalid member name"
                        )
            except (KeyError, TypeError, ValueError) as error:
                raise SnapshotUnavailable(
                    "Invalid primary federation member mapping: " + str(error)
                ) from error
            self._members = members
            self._observed_primary = copy.deepcopy(primary)

        def _snapshot(self):
            if self._observed_primary is None:
                raise SnapshotUnavailable(
                    "Read the primary RayCluster before its Pod inventory"
                )
            # Ray's URL resolver supports Pods and RayClusters, but not ConfigMaps.
            # Reuse its HTTPS endpoint handling and the native rotating SA token.
            pods_url = native.url_from_resource(
                namespace=self._namespace,
                path="pods",
                kuberay_crd_version=self._kuberay_crd_version,
            )
            url = (
                pods_url.rsplit("/", 1)[0]
                + "/configmaps/"
                + quote(self._snapshot_name, safe="")
            )
            headers, verify = self._get_refreshed_headers_and_verify()
            response = transport.get(
                url,
                headers=headers,
                verify=verify,
                timeout=native.KUBERAY_REQUEST_TIMEOUT_S,
            )
            response.raise_for_status()
            configmap = response.json()
            try:
                snapshot = json.loads(configmap["data"]["snapshot.json"])
                self._validate_snapshot(snapshot)
                if not configmap["metadata"]["resourceVersion"]:
                    raise ValueError("ConfigMap resourceVersion is missing")
            except (KeyError, TypeError, ValueError, AttributeError) as error:
                raise SnapshotUnavailable(
                    f"Invalid federation snapshot: {error}"
                ) from error
            return configmap, snapshot

        def _validate_snapshot(self, snapshot):
            if (
                not isinstance(snapshot, dict)
                or type(snapshot.get("schemaVersion")) is not int
                or snapshot["schemaVersion"] != 2
            ):
                raise SnapshotUnavailable("Unsupported federation snapshot schema")
            if snapshot.get("adapterRevision") != ADAPTER_REVISION:
                raise SnapshotUnavailable(
                    "Federation adapter revision changed; restart the autoscaler "
                    "with the controller's adapter before resuming scaling"
                )
            if snapshot.get("error"):
                raise SnapshotUnavailable(
                    "Federation observation is unavailable: " + str(snapshot["error"])
                )
            primary = self._observed_primary["metadata"]
            if snapshot.get("primaryUID") != primary["uid"]:
                raise SnapshotUnavailable(
                    "Federation snapshot belongs to another primary RayCluster"
                )
            if (
                type(snapshot.get("primaryGeneration")) is not int
                or snapshot["primaryGeneration"] != primary["generation"]
            ):
                raise SnapshotUnavailable(
                    "Waiting for the current primary generation's Pod inventory"
                )
            age = clock() - observed_timestamp(snapshot["observedAt"])
            if age > SNAPSHOT_MAX_AGE_SECONDS or age < -MAX_CLOCK_SKEW_SECONDS:
                raise SnapshotUnavailable(
                    "Federation Pod inventory is stale or has an invalid timestamp"
                )
            pods = snapshot["pods"]
            if not isinstance(pods, list):
                raise ValueError("pods must be a list")
            groups = {
                group["groupName"]
                for group in self._observed_primary.get("spec", {}).get(
                    "workerGroupSpecs", []
                )
            }
            names = set()
            for pod in pods:
                metadata = pod["metadata"]
                name = metadata["name"]
                if not isinstance(name, str) or not name or "/" in name:
                    raise ValueError("Pod name is missing")
                labels = metadata["labels"]
                kind = labels.get("ray.io/node-type")
                group = labels.get("ray.io/group")
                if kind not in ("head", "worker") or not group:
                    raise ValueError("Snapshot contains a non-Ray Pod")
                if kind == "worker" and group not in groups:
                    raise SnapshotUnavailable(
                        "Snapshot contains an unknown worker group: " + str(group)
                    )
                member = labels.get(MEMBER_LABEL)
                expected_member = self._members.get(group) if kind == "worker" else None
                if member != expected_member:
                    raise SnapshotUnavailable(
                        "Pod member identity does not match its primary worker group"
                    )
                instance_id = member + "/" + name if member else name
                if instance_id in names:
                    raise SnapshotUnavailable(
                        "Duplicate federated instance ID: " + instance_id
                    )
                names.add(instance_id)
                if not isinstance(pod.get("status"), dict):
                    raise ValueError("Pod status must be an object")

        def _prepare_patch(self, payload):
            if not isinstance(payload, list):
                raise ValueError("Federation scaling requires a JSON Patch list")
            if self._observed_primary is None:
                raise SnapshotUnavailable("Read the primary RayCluster before scaling")
            groups = self._observed_primary.get("spec", {}).get("workerGroupSpecs", [])
            result = copy.deepcopy(payload)
            for operation in result:
                match = re.fullmatch(
                    r"/spec/workerGroupSpecs/(0|[1-9]\d*)/(replicas|scaleStrategy)",
                    operation.get("path", ""),
                )
                if (
                    operation.get("op") != "replace"
                    or not match
                    or int(match[1]) >= len(groups)
                ):
                    raise ValueError(
                        "Federation autoscaler may only replace worker replica intent"
                    )
                value = operation.get("value")
                if match[2] == "replicas":
                    if type(value) is not int or value < 0:
                        raise ValueError(
                            "Worker replicas must be a non-negative integer"
                        )
                elif (
                    not isinstance(value, dict)
                    or set(value) != {"workersToDelete"}
                    or not isinstance(value["workersToDelete"], list)
                    or any(
                        not isinstance(name, str) or not name
                        for name in value["workersToDelete"]
                    )
                ):
                    raise ValueError(
                        "scaleStrategy must contain a workersToDelete list"
                    )
                else:
                    group_name = groups[int(match[1])]["groupName"]
                    member = self._members.get(group_name)
                    raw_names = []
                    for instance_id in value["workersToDelete"]:
                        prefix = member + "/" if member else ""
                        if member and not instance_id.startswith(prefix):
                            raise ValueError(
                                "Deletion instance ID belongs to another member"
                            )
                        name = instance_id[len(prefix) :]
                        if not name or "/" in name:
                            raise ValueError(
                                "Deletion instance ID does not identify a Pod in its worker group"
                            )
                        raw_names.append(name)
                    value["workersToDelete"] = raw_names
            return result

    return FederationKubernetesHttpApiClient


def run_with_startup_retry(run):
    """Retry transient Kubernetes startup failures without exiting the sidecar."""
    import logging
    import requests

    while True:
        try:
            return run()
        except (requests.ConnectionError, requests.Timeout):
            logging.exception(
                "Kubernetes unavailable during autoscaler startup; retrying"
            )
            time.sleep(5)


def main():
    import ray
    from ray.autoscaler._private.kuberay import node_provider
    from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay import (
        cloud_provider,
    )

    validate_ray_version(ray.__version__)
    client_type = make_client_class(node_provider)
    node_provider.KubernetesHttpApiClient = client_type
    # v2 imports the class by name; updating the original module alone misses it.
    cloud_provider.KubernetesHttpApiClient = client_type
    cluster = os.environ["RAY_CLUSTER_NAME"]
    namespace = os.environ["RAY_CLUSTER_NAMESPACE"]
    from ray.autoscaler._private.kuberay import run_autoscaler
    from ray.autoscaler.v2 import autoscaler
    from ray.autoscaler.v2.instance_manager.reconciler import Reconciler

    Reconciler.reconcile = staticmethod(guard_reconcile(Reconciler.reconcile))

    def check_observation():
        # Each asynchronous action gets its own client: its remembered primary
        # must not race the provider's foreground read/patch sequence.
        client = client_type(namespace)
        client.get("rayclusters/" + cluster)
        client._snapshot()

    autoscaler.RayStopper = guarded_ray_stopper(
        autoscaler.RayStopper, check_observation
    )

    def reject_v1(*args, **kwargs):
        raise RuntimeError(
            "The Ray head must run autoscaler v2 for federated autoscaling"
        )

    # Native startup checks the running GCS configuration. Reject a mismatch
    # rather than silently starting v1 with different failure semantics.
    run_autoscaler.Monitor = reject_v1
    run_with_startup_retry(
        lambda: run_autoscaler.run_kuberay_autoscaler(cluster, namespace)
    )


if __name__ == "__main__":
    main()
