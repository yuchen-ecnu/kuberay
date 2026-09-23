#!/usr/bin/env python3
"""Exercise manual federation members without sharing member API credentials."""

import argparse
import json
import time
from pathlib import Path

from kind_e2e import (
    Cluster, RUNTIME_TEST, SMOKE_TEST, condition, federation_pods,
    ray_job, ready_workers, scale_federation_group, wait, workers,
)


def load_sample(cluster, filename):
    path = Path(__file__).resolve().parents[2] / "config/samples" / filename
    return json.loads(cluster.run("create", "--dry-run=client", "-f", str(path), "-o", "json"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-kubeconfig", required=True)
    parser.add_argument("--member-kubeconfig", required=True)
    parser.add_argument("--image", default="rayproject/ray:2.56.0-py311-cpu")
    parser.add_argument("--artifacts", required=True)
    args = parser.parse_args()
    artifacts = Path(args.artifacts)
    artifacts.mkdir(parents=True, exist_ok=True)
    namespace, name = "ray-federation-manual-e2e", "ray-federation-manual"
    primary = Cluster(args.primary_kubeconfig, namespace)
    member = Cluster(args.member_kubeconfig, namespace)
    results, started = [], time.monotonic()
    for cluster in (primary, member):
        cluster.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
        if any(cluster.get(kind)["items"] for kind in ("frc", "raycluster")):
            raise RuntimeError("Existing test resources in {}: inspect and clean up before rerunning".format(namespace))
    assert not primary.get("secrets")["items"], "manual test must have no member credential Secret"
    frc = load_sample(primary, "ray-federation-manual.yaml")
    mrc = load_sample(member, "ray-member-standalone.yaml")
    for resource in (frc, mrc):
        resource["metadata"].update({"name": name, "namespace": namespace})
    frc["spec"]["networking"]["headEndpoint"]["address"] = "10.96.0.102"
    for group in mrc["spec"]["workerGroupSpecs"]:
        group["rayStartParams"]["address"] = "10.96.0.102:6379"
    frc["spec"]["memberClusters"][0]["namespace"] = namespace
    head = frc["spec"]["primaryCluster"]["headGroupSpec"]
    head["headService"] = {"metadata": {"name": "federation-head-manual"}, "spec": {"clusterIP": "10.96.0.102"}}
    templates = [head["template"]] + [g["template"] for g in frc["spec"]["primaryCluster"]["workerGroups"]]
    templates += [g["template"] for g in mrc["spec"]["workerGroupSpecs"]]
    for template in templates:
        template["spec"]["containers"][0]["image"] = args.image
        template["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    (artifacts / "frc-input.json").write_text(json.dumps(frc, indent=2))
    (artifacts / "mrc-input.json").write_text(json.dumps(mrc, indent=2))
    try:
        member.apply(mrc)
        wait("manual MRC creates workers before an FRC exists", lambda: len(workers(member, name)) == 2, 90)
        assert condition(member.get("raycluster", name), "WorkersReady") != "True"
        assert not primary.get("frc")["items"]
        assert not member.get("services")["items"]
        assert len(member.get("raycluster")["items"]) == 1
        results.append("manual MRC creates only workers, waits for GCS, and needs no FRC configuration")

        primary.apply(frc)
        wait("manual workers join the primary Ray runtime", lambda:
             ready_workers(primary, name, 1) and ready_workers(member, name, 2) and
             condition(primary.get("frc", name), "HeadEndpointReady") == "True" and
             condition(primary.get("frc", name), "Ready") == "True")
        observed = primary.get("frc", name)
        status = observed["status"]["memberClusterStatuses"][0]
        assert not status.get("workerGroupStatuses")
        assert not status.get("lastUpdateTime")
        assert len(status["conditions"]) == 1
        manual_condition = status["conditions"][0]
        assert manual_condition["type"] == "ManuallyManaged" and manual_condition["status"] == "True"
        assert manual_condition["reason"] == "ManuallyManaged"
        prc = primary.get("raycluster", name)
        assert prc["spec"]["enableInTreeAutoscaling"] is False
        assert all(not g.get("managedBy") for g in prc["spec"]["workerGroupSpecs"])
        observed_mrc = member.get("raycluster", name)
        assert not observed_mrc["metadata"].get("labels", {}).get("ray.io/federation-owner")
        assert not observed_mrc["spec"].get("provisionWorkers")
        assert "headGroupSpec" not in observed_mrc["spec"]
        assert "networking" not in observed_mrc["spec"]
        assert condition(observed_mrc, "WorkersReady") == "True"
        assert not member.get("leases")["items"]
        pod = workers(member, name)[0]
        output = member.run("exec", pod["metadata"]["name"], "-c", "ray-worker", "--", "python", "-c",
                            'import os; c=os.environ["KUBERAY_GEN_RAY_START_CMD"]; '
                            'assert "$(POD_IP)" not in c; '
                            'assert "--node-ip-address=" + os.environ["POD_IP"] in c; '
                            'assert "--resources=" in c; print(c)')
        (artifacts / "generated-start-command.log").write_text(output)
        output = ray_job(primary, name, RUNTIME_TEST)
        (artifacts / "ray-runtime.log").write_text(output)
        print(output, flush=True)
        results.append("generated command env expands Pod IP and JSON resources; tasks, actors, 64 MiB round trip, Ray Data")
        results.append("FRC is Ready when the primary is ready; manual members report only ManuallyManaged=True")

        frc["spec"]["primaryCluster"]["workerGroups"][0]["replicas"] = 3
        primary.apply(frc)
        wait("FRC seed edit leaves existing primary target unchanged", lambda:
             condition(primary.get("frc", name), "Ready") == "True" and
             primary.get("raycluster", name)["spec"]["workerGroupSpecs"][0]["replicas"] == 1)
        scale_federation_group(primary, name, prc["spec"]["workerGroupSpecs"][0]["groupName"], 3)
        wait("PRC replica edit scales the primary", lambda: ready_workers(primary, name, 3))
        assert ready_workers(primary, name, 3)
        primary_group = prc["spec"]["workerGroupSpecs"][0]["groupName"]
        for replicas in (2, 1):
            scale_federation_group(primary, name, primary_group, replicas)
            wait("manual federation primary scales through PRC to {}".format(replicas), lambda:
                 ready_workers(primary, name, replicas) and condition(primary.get("frc", name), "Ready") == "True")
        results.append("PRC owns primary targets while manual members retain independent targets")

        mrc["spec"]["workerGroupSpecs"][0]["replicas"] = 3
        member.apply(mrc)
        wait("manual replicas scale to three", lambda: ready_workers(member, name, 3))
        mrc["spec"]["workerGroupSpecs"][0]["replicas"] = 2
        member.apply(mrc)
        wait("manual replicas scale back to two", lambda: ready_workers(member, name, 2))
        preempted = workers(member, name)[0]
        member.run("delete", "pod", preempted["metadata"]["name"], "--wait=false")
        wait("manual worker is replaced after preemption", lambda: ready_workers(member, name, 2) and
             all(p["metadata"]["uid"] != preempted["metadata"]["uid"] for p in workers(member, name)))
        assert "REMOTE_TASK_OK" in ray_job(primary, name, SMOKE_TEST)
        results.append("manual 2 -> 3 -> 2 replica changes and local preemption recovery")

        before = {p["metadata"]["uid"] for p in workers(member, name)}
        mrc["spec"]["workerGroupSpecs"][0]["rayStartParams"]["address"] = "10.96.0.103:6379"
        member.apply(mrc)
        wait("replacement workers wait for an unavailable endpoint", lambda:
             len(workers(member, name)) == 2 and
             not ({p["metadata"]["uid"] for p in workers(member, name)} & before) and
             not ready_workers(member, name, 2))
        service = primary.get("service", "federation-head-manual")
        primary.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "manual-head-alternate", "namespace": namespace},
                       "spec": {"clusterIP": "10.96.0.103", "selector": service["spec"]["selector"], "ports": service["spec"]["ports"]}})
        wait("endpoint availability resumes worker startup", lambda: ready_workers(member, name, 2))
        assert "REMOTE_TASK_OK" in ray_job(primary, name, SMOKE_TEST)
        results.append("endpoint changes rebuild workers; GCS init wait recovers without federation intervention")

        assert not primary.get("secrets")["items"]
        (artifacts / "frc-ready.json").write_text(json.dumps(primary.get("frc", name), indent=2))
        (artifacts / "mrc-ready.json").write_text(json.dumps(member.get("raycluster", name), indent=2))
        member_uid = member.get("raycluster", name)["metadata"]["uid"]
        primary.run("delete", "frc", name, "--wait=false")
        wait("FRC deletion completes without member credentials", lambda:
             not primary.get("frc")["items"] and not primary.get("raycluster")["items"] and not federation_pods(primary, name), 180)
        assert member.get("raycluster", name)["metadata"]["uid"] == member_uid
        # A lost head may terminate a Ray process; local replacement is valid.
        # FRC deletion must preserve the MRC and its desired worker capacity.
        wait("manual MRC retains worker capacity after FRC deletion", lambda: len(workers(member, name)) == 2, 90)
        member.run("delete", "raycluster", name, "--wait=false")
        wait("local MRC deletion garbage collects member workers", lambda:
             not member.get("raycluster")["items"] and not federation_pods(member, name), 120)
        primary.run("delete", "service", "manual-head-alternate")
        results.append("FRC cleanup preserves manual MRC and workers; local MRC deletion cleans up its workers")
        (artifacts / "results.json").write_text(json.dumps({"passed": results, "elapsed_seconds": round(time.monotonic() - started, 1)}, indent=2))
        print("ALL STANDALONE FEDERATION KIND TESTS PASSED", flush=True)
    finally:
        for side, cluster in [("primary", primary), ("member", member)]:
            for resource in ("frc", "raycluster", "pods", "leases"):
                try:
                    (artifacts / (side + "-" + resource + ".json")).write_text(json.dumps(cluster.get(resource), indent=2))
                except RuntimeError:
                    pass
            try:
                (artifacts / (side + "-operator.log")).write_text(cluster.run(
                    "logs", "-n", "kuberay-system", "deployment/kuberay-operator", "--tail=500"))
            except RuntimeError:
                pass


if __name__ == "__main__":
    main()
