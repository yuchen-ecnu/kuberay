#!/usr/bin/env python3
"""Verify external managedBy without FRC, including a provider in the same cluster."""
import argparse
import copy
import json
import time
from pathlib import Path

from kind_e2e import Cluster, wait


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--artifacts", required=True)
    args = parser.parse_args()
    artifacts = Path(args.artifacts)
    artifacts.mkdir(parents=True, exist_ok=True)
    namespace, name = "ray-managedby-e2e", "ray-managedby"
    cluster = Cluster(args.kubeconfig, namespace)
    cluster.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
    if any(cluster.get(kind)["items"] for kind in ("raycluster", "frc", "pods")):
        raise RuntimeError("Existing managedBy test resources; inspect and clean up before rerunning")
    deployment = json.loads(cluster.run("get", "deployment", "kuberay-operator", "-n", "kuberay-system", "-o", "json"))
    original_args = deployment["spec"]["template"]["spec"]["containers"][0]["args"]
    disabled_args = [a.replace("RayFederation=true", "RayFederation=false") for a in original_args]
    assert any("RayFederation=false" in a for a in disabled_args)

    def set_operator_args(values):
        cluster.run("patch", "deployment", "kuberay-operator", "-n", "kuberay-system", "--type=json", "-p",
                    json.dumps([{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": values}]))
        cluster.run("rollout", "status", "deployment/kuberay-operator", "-n", "kuberay-system", "--timeout=90s", timeout=100)

    started = time.monotonic()
    try:
        set_operator_args(disabled_args)
        sample = Path(__file__).resolve().parents[2] / "config/samples/ray-federation.yaml"
        frc = json.loads(cluster.run("create", "--dry-run=client", "-f", str(sample), "-o", "json"))
        primary = frc["spec"]["primaryCluster"]
        local = primary["workerGroups"][0]
        external = copy.deepcopy(local)
        external.update({"groupName": "external-workers", "replicas": 4,
                         "managedBy": "ray.io/federated-raycluster-controller",
                         "scaleStrategy": {"workersToDelete": ["external-provider-worker"]}})
        raycluster = {"apiVersion": "ray.io/v1", "kind": "RayCluster", "metadata": {"name": name, "namespace": namespace},
                      "spec": {"rayVersion": primary["rayVersion"], "enableInTreeAutoscaling": False,
                               "headGroupSpec": primary["headGroupSpec"], "workerGroupSpecs": [local, external]}}
        for group in [primary["headGroupSpec"], local, external]:
            group["template"]["spec"]["containers"][0].update({"image": args.image, "imagePullPolicy": "IfNotPresent"})
        # A native object stands in for another controller's ownership; no new CRD.
        cluster.apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "external-provider", "namespace": namespace}})
        owner = cluster.get("configmap", "external-provider")["metadata"]
        provider_pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {
            "name": "external-provider-worker", "namespace": namespace,
            "labels": {"ray.io/cluster": name, "ray.io/node-type": "worker", "ray.io/group": "external-workers"},
            "ownerReferences": [{"apiVersion": "v1", "kind": "ConfigMap", "name": owner["name"], "uid": owner["uid"], "controller": True}]},
            "spec": {"containers": [{"name": "external-worker", "image": args.image, "imagePullPolicy": "IfNotPresent",
                     "command": ["python", "-c", "import time; time.sleep(3600)"],
                     "resources": {"requests": {"cpu": "20m", "memory": "64Mi"}, "limits": {"memory": "128Mi"}}}]}}
        cluster.apply(provider_pod)
        provider_uid = cluster.get("pod", "external-provider-worker")["metadata"]["uid"]
        cluster.apply(raycluster)

        def locally_ready():
            status = cluster.get("raycluster", name).get("status", {})
            return status.get("state") == "ready" and all(status.get(field, 0) == 1 for field in
                ("desiredWorkerReplicas", "readyWorkerReplicas", "availableWorkerReplicas"))

        wait("external managedBy works without federation and counts only the local worker", locally_ready)
        assert not cluster.get("frc")["items"]
        assert len(cluster.get("pods")["items"]) == 3, "only head, one local worker and the provider Pod should exist"
        assert cluster.get("pod", "external-provider-worker")["metadata"]["uid"] == provider_uid
        (artifacts / "raycluster-ready.yaml").write_text(cluster.run("get", "raycluster", name, "-o", "yaml"))
        (artifacts / "operator-args.json").write_text(json.dumps(disabled_args, indent=2))

        cluster.patch("raycluster", name, {"spec": {"suspend": True}})
        wait("local suspension completes and preserves the external provider Pod", lambda:
             cluster.get("raycluster", name).get("status", {}).get("state") == "suspended" and len(cluster.get("pods")["items"]) == 1)
        assert cluster.get("pod", "external-provider-worker")["metadata"]["uid"] == provider_uid
        cluster.patch("raycluster", name, {"spec": {"suspend": False}})
        wait("local resume preserves the external provider Pod", locally_ready)
        assert cluster.get("pod", "external-provider-worker")["metadata"]["uid"] == provider_uid
        cluster.run("delete", "raycluster", name, "--wait=false")
        wait("RayCluster deletion removes only its owned Pods", lambda:
             not cluster.get("raycluster")["items"] and len(cluster.get("pods")["items"]) == 1)
        assert cluster.get("pod", "external-provider-worker")["metadata"]["uid"] == provider_uid
        cluster.run("delete", "configmap", "external-provider", "--wait=false")
        wait("provider cleanup removes the final Pod", lambda: not cluster.get("pods")["items"])
        (artifacts / "results.json").write_text(json.dumps({
            "passed": ["no FRC or federation feature gate", "local replica accounting excludes external Pods",
                       "external deletion intent does not delete local provider Pods", "suspend and resume preserve provider Pod UID",
                       "RayCluster and provider cleanup respect separate ownership"],
            "elapsed_seconds": round(time.monotonic() - started, 1)}, indent=2))
        print("ALL EXTERNAL PLACEMENT KIND TESTS PASSED", flush=True)
    finally:
        try:
            (artifacts / "operator.log").write_text(cluster.run("logs", "deployment/kuberay-operator", "-n", "kuberay-system", "--tail=300"))
        finally:
            set_operator_args(original_args)


if __name__ == "__main__":
    main()
