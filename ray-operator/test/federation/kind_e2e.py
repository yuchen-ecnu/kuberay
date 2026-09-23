#!/usr/bin/env python3
"""Exercise federation using two prepared kind clusters. Uses only the stdlib."""
import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
import time


def command(args, data=None, timeout=60):
    result = subprocess.run(args, input=data, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode:
        raise RuntimeError("{}: {}".format(" ".join(args[:8]), result.stderr.decode()))
    return result.stdout.decode()


class Cluster:
    def __init__(self, kubeconfig, namespace):
        self.base = ["kubectl", "--kubeconfig", kubeconfig, "-n", namespace]

    def run(self, *args, **kwargs):
        return command(self.base + list(args), **kwargs)

    def get(self, kind, name=None):
        args = ["get", kind]
        if name:
            args.append(name)
        return json.loads(self.run(*args, "-o", "json"))

    def apply(self, value):
        return self.run("apply", "--server-side", "--field-manager=frc-e2e", "-f", "-",
                        data=json.dumps(value).encode())

    def patch(self, kind, name, patch):
        return self.run("patch", kind, name, "--type=merge", "-p", json.dumps(patch))


def wait(description, predicate, timeout=480):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                print("PASS " + description, flush=True)
                return
        except (RuntimeError, KeyError, IndexError, StopIteration) as error:
            last_error = error
        time.sleep(2)
    raise AssertionError("{} timed out; last error: {}".format(description, last_error))


def condition(value, name):
    return next((c["status"] for c in value.get("status", {}).get("conditions", [])
                 if c["type"] == name and c.get("observedGeneration") == value["metadata"].get("generation")), "Unknown")


def workers(cluster, name):
    return [pod for pod in cluster.get("pods")["items"]
            if pod["metadata"].get("labels", {}).get("ray.io/cluster") == name
            and pod["metadata"]["labels"].get("ray.io/node-type") == "worker"
            and "deletionTimestamp" not in pod["metadata"]]


def ready_workers(cluster, name, count):
    pods = workers(cluster, name)
    return len(pods) == count and all(any(c["type"] == "Ready" and c["status"] == "True"
        for c in pod.get("status", {}).get("conditions", [])) for pod in pods)


def federation_pods(cluster, name):
    return [pod for pod in cluster.get("pods")["items"]
            if pod["metadata"].get("labels", {}).get("ray.io/cluster") == name]


def scale_federation_group(cluster, name, group_name, replicas):
    spec = cluster.get("raycluster", name)["spec"]
    groups = spec["workerGroupSpecs"]
    next(g for g in groups if g["groupName"] == group_name)["replicas"] = replicas
    cluster.patch("raycluster", name, {"spec": spec})


def ray_job(primary, name, code):
    head = next(p for p in primary.get("pods")["items"]
                if p["metadata"].get("labels", {}).get("ray.io/cluster") == name
                and p["metadata"]["labels"].get("ray.io/node-type") == "head")
    return primary.run("exec", "-i", head["metadata"]["name"], "-c", "ray-head",
                       "--", "python", "-", data=code.encode(), timeout=240)


RUNTIME_TEST = '''
import json, os, ray
ray.init(address="auto", log_to_driver=False)
nodes = [n for n in ray.nodes() if n["Alive"]]
assert len(nodes) == 4, nodes
@ray.remote(resources={"primary": 0.1})
def produce():
    return b"f" * (64 * 1024 * 1024)
@ray.remote(resources={"member": 0.1})
def transfer(value):
    assert len(value) == 64 * 1024 * 1024
    return value
@ray.remote(resources={"primary": 0.1})
def consume(value):
    assert value == b"f" * (64 * 1024 * 1024)
    return len(value)
assert ray.get(consume.remote(transfer.remote(produce.remote()))) == 64 * 1024 * 1024
@ray.remote(resources={"member": 0.1}, num_cpus=0)
class Counter:
    def __init__(self): self.value = 0
    def increment(self):
        self.value += 1
        return self.value, os.environ["RAY_CLOUD_INSTANCE_ID"]
counter = Counter.remote()
assert ray.get(counter.increment.remote())[0] == 1
value, identity = ray.get(counter.increment.remote())
assert value == 2 and identity.startswith("member-b/"), identity
dataset = ray.data.range(128).map(lambda row: {"id": row["id"] + 1}, resources={"member": 0.1})
assert dataset.sum("id") == 8256
print("FEDERATION_RESULT=" + json.dumps({"alive_nodes": len(nodes), "round_trip_bytes": 67108864,
      "actor_instance_id": identity, "ray_data_sum": 8256}))
ray.shutdown()
'''

SMOKE_TEST = '''
import ray
ray.init(address="auto", log_to_driver=False)
@ray.remote(resources={"member": 0.1})
def work(): return 42
assert ray.get(work.remote(), timeout=60) == 42
print("REMOTE_TASK_OK")
ray.shutdown()
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-kubeconfig", required=True)
    parser.add_argument("--member-kubeconfig", required=True)
    parser.add_argument("--member-node", default="frc-member-control-plane")
    parser.add_argument("--image", default="rayproject/ray:2.56.0-py311-cpu")
    parser.add_argument("--artifacts", required=True)
    args = parser.parse_args()
    artifacts = Path(args.artifacts)
    artifacts.mkdir(parents=True, exist_ok=True)
    namespace, name = "ray-federation-e2e", "ray-federation"
    member_ray_name = name + "-member-b"
    primary = Cluster(args.primary_kubeconfig, namespace)
    member = Cluster(args.member_kubeconfig, namespace)
    results = []
    started = time.monotonic()
    for cluster in (primary, member):
        assert not cluster.run("get", "crd", "memberrayclusters.ray.io", "--ignore-not-found", "-o", "name").strip(), "federation must not install a separate member CRD"
        cluster.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
        if cluster.get("frc")["items"] or cluster.get("raycluster")["items"]:
            raise RuntimeError("Existing federation resources in {}: clean up the previous test before rerunning".format(namespace))
    member.apply({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "federation-client", "namespace": namespace}})
    member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "federation-client", "namespace": namespace},
                  "rules": [{"apiGroups": ["ray.io"], "resources": ["rayclusters"],
                             "verbs": ["get", "list", "watch", "create", "patch", "update", "delete"]},
                            {"apiGroups": [""], "resources": ["pods"],
                             "verbs": ["get", "list", "watch"]}]})
    member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "federation-client", "namespace": namespace},
                  "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "federation-client"},
                  "subjects": [{"kind": "ServiceAccount", "name": "federation-client", "namespace": namespace}]})
    member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
                  "metadata": {"name": "federation-client-member-identity"},
                  "rules": [{"apiGroups": [""], "resources": ["namespaces"],
                             "resourceNames": ["kube-system"], "verbs": ["get"]}]})
    member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
                  "metadata": {"name": "federation-client-member-identity"},
                  "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                              "name": "federation-client-member-identity"},
                  "subjects": [{"kind": "ServiceAccount", "name": "federation-client", "namespace": namespace}]})
    config = json.loads(member.run("config", "view", "--raw", "--minify", "-o", "json"))
    node = json.loads(command(["docker", "inspect", args.member_node]))[0]
    server = "https://{}:6443".format(node["NetworkSettings"]["Networks"]["kind"]["IPAddress"])
    token = member.run("create", "token", "federation-client", "--duration=1h").strip()
    credential = {"apiVersion": "v1", "kind": "Config", "current-context": "member",
                  "contexts": [{"name": "member", "context": {"cluster": "member", "user": "member"}}],
                  "clusters": [{"name": "member", "cluster": {"server": server,
                               "certificate-authority-data": config["clusters"][0]["cluster"]["certificate-authority-data"]}}],
                  "users": [{"name": "member", "user": {"token": token}}]}
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "member-b-kubeconfig", "namespace": namespace,
              "labels": {"ray.io/federation-credential": "true"}}, "stringData": {"kubeconfig": json.dumps(credential)}}
    primary.apply(secret)
    sample = Path(__file__).resolve().parents[2] / "config/samples/ray-federation.yaml"
    frc = json.loads(primary.run("create", "--dry-run=client", "-f", str(sample), "-o", "json"))
    frc["metadata"] = {"name": name, "namespace": namespace}
    frc["spec"]["memberClusters"][0]["namespace"] = namespace
    frc["spec"]["networking"]["headEndpoint"]["address"] = "10.96.0.100"
    head = frc["spec"]["primaryCluster"]["headGroupSpec"]
    head["headService"] = {"metadata": {"name": "federation-head"}, "spec": {"clusterIP": "10.96.0.100"}}
    assert set(frc["spec"]["networking"]) == {"headEndpoint"}
    # Native Ray start parameters pass through to head, local and remote workers.
    # The test network routes the complete Pod CIDRs, including dynamic Ray ports.
    head["rayStartParams"]["node-manager-port"] = "6800"
    frc["spec"]["primaryCluster"]["workerGroups"][0]["rayStartParams"]["node-manager-port"] = "6801"
    remote_params = frc["spec"]["memberClusters"][0]["workerGroups"][0]["rayStartParams"]
    remote_params.update({"node-manager-port": "6802", "min-worker-port": "10102", "max-worker-port": "10112"})
    templates = [head["template"]] + [g["template"] for g in frc["spec"]["primaryCluster"]["workerGroups"]]
    templates += [g["template"] for g in frc["spec"]["memberClusters"][0]["workerGroups"]]
    for template in templates:
        template["spec"]["containers"][0]["image"] = args.image
        template["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    (artifacts / "frc-input.yaml").write_text(primary.run("create", "--dry-run=client", "-f", "-", "-o", "yaml", data=json.dumps(frc).encode()))
    primary.apply(frc)
    partitioned = False
    partition_destinations = []
    try:
        wait("federation initial Ready", lambda: condition(primary.get("frc", name), "Ready") == "True")
        assert len(workers(primary, name)) == 1
        assert len(workers(member, member_ray_name)) == 2
        rejected = copy.deepcopy(frc)
        rejected["spec"]["primaryCluster"]["headGroupSpec"]["template"]["spec"]["containers"][0]["image"] = args.image + "-upgrade"
        try:
            primary.run("apply", "--server-side", "--dry-run=server", "--field-manager=frc-e2e",
                        "-f", "-", data=json.dumps(rejected).encode())
        except RuntimeError as error:
            assert "cannot change in place" in str(error), error
        else:
            raise AssertionError("operator webhook accepted an in-place primary runtime change")
        native_network = copy.deepcopy(frc)
        native_network["spec"]["memberClusters"][0]["workerGroups"][0]["template"]["spec"]["hostNetwork"] = True
        primary.run("apply", "--server-side", "--dry-run=server", "--field-manager=frc-e2e",
                    "-f", "-", data=json.dumps(native_network).encode())
        results.append("webhook rejects primary runtime upgrades and accepts native host networking")
        primary_cluster = primary.get("raycluster", name)
        external_names = {g["groupName"] for m in frc["spec"]["memberClusters"] for g in m["workerGroups"]}
        for group in primary_cluster["spec"]["workerGroupSpecs"]:
            if group["groupName"] in external_names:
                assert group["managedBy"] == "ray.io/federated-raycluster-controller", group
            else:
                assert "managedBy" not in group, group
        (artifacts / "prc-ready.yaml").write_text(primary.run("get", "raycluster", name, "-o", "yaml"))
        assert len(member.get("rayclusters")["items"]) == 1
        member_cluster = member.get("raycluster", member_ray_name)
        assert member_cluster["kind"] == "RayCluster"
        assert "headGroupSpec" not in member_cluster["spec"]
        assert {g["groupName"] for g in member_cluster["spec"]["workerGroupSpecs"]} == external_names
        assert all("managedBy" not in g for g in member_cluster["spec"]["workerGroupSpecs"])
        assert member.get("services")["items"] == []
        assert all(p["metadata"].get("labels", {}).get("ray.io/node-type") != "head" for p in member.get("pods")["items"])
        assert primary_cluster["spec"]["headGroupSpec"]["rayStartParams"]["node-manager-port"] == "6800"
        assert member_cluster["spec"]["workerGroupSpecs"][0]["rayStartParams"]["node-manager-port"] == "6802"
        for cluster, cluster_name in ((primary, name), (member, member_ray_name)):
            assert not cluster.get("leases")["items"]
            assert len(federation_pods(cluster, cluster_name)) == 2, "only Ray head/worker Pods are created"
        assert not member.get("configmaps")["items"] or all(cm["metadata"]["name"] == "kube-root-ca.crt"
                                                              for cm in member.get("configmaps")["items"])
        results.append("native Ray port settings preserved; no network probe or provisioning resources")
        results.append("primary groups delegate to ray.io/federated-raycluster-controller; federation maps group names to locally managed member workers")
        output = ray_job(primary, name, RUNTIME_TEST)
        (artifacts / "ray-runtime.log").write_text(output)
        print(output, flush=True)
        results.append("initial reconciliation, no member head/service, tasks, actors, 64 MiB bidirectional transfer, Ray Data")

        # Members sharing one Kubernetes cluster and namespace own independent CRs.
        member_uid = member.get("raycluster", member_ray_name)["metadata"]["uid"]
        member_pods = {p["metadata"]["uid"] for p in workers(member, member_ray_name)}
        shared = copy.deepcopy(frc)
        second = copy.deepcopy(shared["spec"]["memberClusters"][0])
        second["name"] = "member-c"
        second["workerGroups"][0]["groupName"] = "member-cpu-c"
        second["workerGroups"][0]["replicas"] = 1
        shared["spec"]["memberClusters"].append(second)
        second_name = name + "-member-c"
        primary.apply(shared)
        wait("two members in one namespace become Ready", lambda:
             condition(primary.get("frc", name), "Ready") == "True" and
             ready_workers(member, second_name, 1))
        statuses = primary.get("frc", name)["status"]["memberClusterStatuses"]
        assert {m["rayClusterName"] for m in statuses} == {member_ray_name, second_name}
        assert len({m["clusterUID"] for m in statuses}) == 1
        scale_federation_group(primary, name, "member-cpu-c", 2)
        wait("second member scales independently", lambda:
             condition(primary.get("frc", name), "Ready") == "True" and
             ready_workers(member, second_name, 2) and ready_workers(member, member_ray_name, 2))
        assert {p["metadata"]["uid"] for p in workers(member, member_ray_name)} == member_pods
        primary.apply(frc)
        wait("removing one member preserves the other", lambda:
             condition(primary.get("frc", name), "Ready") == "True" and
             {m["metadata"]["name"] for m in member.get("rayclusters")["items"]} == {member_ray_name} and
             not federation_pods(member, second_name), 180)
        assert member.get("raycluster", member_ray_name)["metadata"]["uid"] == member_uid
        assert {p["metadata"]["uid"] for p in workers(member, member_ray_name)} == member_pods
        results.append("PRC replicas scale same-namespace members independently and cleanup preserves the other member")

        before = {p["metadata"]["uid"] for p in workers(member, member_ray_name)}
        broken = copy.deepcopy(secret)
        broken["stringData"]["kubeconfig"] = "invalid kubeconfig"
        primary.apply(broken)
        wait("credential failure is observable", lambda: condition(primary.get("frc", name), "MemberControlPlaneReachable") == "False", 90)
        assert {p["metadata"]["uid"] for p in workers(member, member_ray_name)} == before
        assert "REMOTE_TASK_OK" in ray_job(primary, name, SMOKE_TEST)
        # The member keeps reconciling its last received spec without central authorization.
        member_spec = member.get("raycluster", member_ray_name)["spec"]
        member_spec["workerGroupSpecs"][0]["replicas"] = 3
        member.patch("raycluster", member_ray_name, {"spec": member_spec})
        wait("member scales while federation credentials are unavailable", lambda: ready_workers(member, member_ray_name, 3))
        preempted = workers(member, member_ray_name)[0]
        member.run("delete", "pod", preempted["metadata"]["name"], "--wait=false")
        wait("member repairs preemption without federation access", lambda: ready_workers(member, member_ray_name, 3) and
             all(p["metadata"]["uid"] != preempted["metadata"]["uid"] for p in workers(member, member_ray_name)))
        assert "REMOTE_TASK_OK" in ray_job(primary, name, SMOKE_TEST)
        primary.apply(secret)
        wait("credential rotation restores the PRC target", lambda: ready_workers(member, member_ray_name, 2) and condition(primary.get("frc", name), "Ready") == "True", 180)
        results.append("credential failure permits local scale-up and preemption recovery; restored credentials resynchronize the PRC target")

        before_partition = {p["metadata"]["uid"] for p in workers(member, member_ray_name)}
        head_pod = next(p for p in federation_pods(primary, name)
                        if p["metadata"]["labels"].get("ray.io/node-type") == "head")
        partition_destinations = [head_pod["status"]["podIP"], frc["spec"]["networking"]["headEndpoint"]["address"]]
        for destination in partition_destinations:
            command(["docker", "exec", args.member_node, "iptables", "-I", "FORWARD", "1", "-d", destination, "-j", "DROP"])
        partitioned = True
        scale_federation_group(primary, name, "member-cpu", 3)
        wait("new replica intent reaches the member during partition", lambda: member.get("raycluster", member_ray_name)["spec"]["workerGroupSpecs"][0]["replicas"] == 3, 90)
        wait("partition does not block worker Pod creation", lambda: len(workers(member, member_ray_name)) == 3, 90)
        added = [p for p in workers(member, member_ray_name) if p["metadata"]["uid"] not in before_partition]
        assert len(added) == 1
        wait("new worker waits in the GCS init container", lambda: any(
            c["name"] == "wait-gcs-ready" and c.get("state", {}).get("running")
            for c in member.get("pod", added[0]["metadata"]["name"]).get("status", {}).get("initContainerStatuses", [])), 90)
        added = [member.get("pod", added[0]["metadata"]["name"])]
        assert not any(c["type"] == "Ready" and c["status"] == "True"
                       for c in added[0].get("status", {}).get("conditions", []))
        wait("waiting worker keeps federation unready", lambda: condition(primary.get("frc", name), "WorkersReady") == "False", 90)
        assert not any(c["type"] == "MemberDataPlaneReady"
                       for c in primary.get("frc", name).get("status", {}).get("conditions", []))
        for destination in partition_destinations:
            command(["docker", "exec", args.member_node, "iptables", "-D", "FORWARD", "-d", destination, "-j", "DROP"])
        partitioned = False
        wait("network recovery lets the new worker join Ray", lambda: ready_workers(member, member_ray_name, 3) and condition(primary.get("frc", name), "Ready") == "True")
        observed = primary.get("frc", name)
        assert observed["spec"]["memberClusters"][0]["workerGroups"][0]["replicas"] == 2
        assert next(g for g in primary.get("raycluster", name)["spec"]["workerGroupSpecs"]
                    if g["groupName"] == "member-cpu")["replicas"] == 3
        group_status = observed["status"]["memberClusterStatuses"][0]["workerGroupStatuses"][0]
        assert group_status["desiredReplicas"] == group_status["readyReplicas"] == 3
        results.append("network partition allows Pod creation while GCS readiness waits; recovery completes 2 -> 3 scale-up")

        scaling = primary.get("raycluster", name)
        selected = workers(member, member_ray_name)[0]["metadata"]["name"]
        untouched = {p["metadata"]["uid"] for p in workers(member, member_ray_name) if p["metadata"]["name"] != selected}
        for group in scaling["spec"]["workerGroupSpecs"]:
            if group["groupName"] == "member-cpu":
                group["replicas"] = 2
                group["scaleStrategy"] = {"workersToDelete": [selected]}
        primary.patch("raycluster", name, {"spec": scaling["spec"]})
        wait("PRC targeted scale-down converges", lambda: ready_workers(member, member_ray_name, 2) and condition(primary.get("frc", name), "Ready") == "True")
        assert {p["metadata"]["uid"] for p in workers(member, member_ray_name)} == untouched
        assert all(p["metadata"]["name"] != selected for p in workers(member, member_ray_name))
        time.sleep(15)
        assert len(workers(member, member_ray_name)) == 2, "replaying absolute targets must not double-scale"
        results.append("PRC replicas and targeted workersToDelete propagate idempotently")

        preempted = workers(member, member_ray_name)[0]
        member.run("delete", "pod", preempted["metadata"]["name"], "--wait=false")
        wait("member controller replaces a preempted worker", lambda: ready_workers(member, member_ray_name, 2) and
             all(p["metadata"]["uid"] != preempted["metadata"]["uid"] for p in workers(member, member_ray_name)) and
             condition(primary.get("frc", name), "Ready") == "True")
        assert "REMOTE_TASK_OK" in ray_job(primary, name, SMOKE_TEST)
        results.append("member-local preemption recovery and subsequent Ray task")

        old_uids = {p["metadata"]["uid"] for p in workers(member, member_ray_name)}
        service = primary.get("service", "federation-head")
        primary.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "federation-head-alternate", "namespace": namespace},
                       "spec": {"clusterIP": "10.96.0.101", "selector": service["spec"]["selector"], "ports": service["spec"]["ports"]}})
        primary.patch("frc", name, {"spec": {"networking": {"headEndpoint": {"address": "10.96.0.101"}}}})
        wait("endpoint change rebuilds member workers", lambda: ready_workers(member, member_ray_name, 2) and
             not ({p["metadata"]["uid"] for p in workers(member, member_ray_name)} & old_uids) and
             condition(primary.get("frc", name), "Ready") == "True")
        assert "REMOTE_TASK_OK" in ray_job(primary, name, SMOKE_TEST)
        results.append("endpoint change is validated and rebuilds remote workers")

        (artifacts / "frc-ready.json").write_text(json.dumps(primary.get("frc", name), indent=2))
        (artifacts / "mrc-ready.json").write_text(json.dumps(member.get("raycluster", member_ray_name), indent=2))
        primary.apply(broken)
        primary.run("delete", "frc", name, "--wait=false")
        wait("deletion is blocked while member credentials are unavailable", lambda: any(
            c.get("reason") == "DeletionBlocked" for c in primary.get("frc", name).get("status", {}).get("conditions", [])), 90)
        assert member.get("raycluster", member_ray_name)
        primary.apply(secret)
        wait("restored credentials finish FRC and remote cleanup", lambda: not primary.get("frc")["items"] and not member.get("raycluster")["items"], 180)
        wait("native GC removes primary and member Pods", lambda: not federation_pods(primary, name) and
             not federation_pods(member, member_ray_name) and not primary.get("raycluster")["items"], 180)
        primary.run("delete", "service", "federation-head-alternate")
        results.append("unreachable-member deletion is blocked; restoration completes foreground cleanup")

        # Remove the managed member with Orphan while keeping its head alive.
        # Its local operator must maintain workers after federation ownership is removed.
        orphan_frc = copy.deepcopy(frc)
        orphan_frc["spec"]["memberCleanupPolicy"] = "Orphan"
        orphan_frc["spec"]["memberClusters"][0]["workerGroups"][0]["replicas"] = 1
        primary.apply(orphan_frc)
        wait("federation for Orphan check is Ready", lambda: condition(primary.get("frc", name), "Ready") == "True")
        orphan_uid = member.get("raycluster", member_ray_name)["metadata"]["uid"]
        before_orphan = {p["metadata"]["uid"] for p in workers(member, member_ray_name)}
        primary.patch("frc", name, {"spec": {"memberClusters": [{"name": "manual-placeholder", "namespace": namespace}]}})
        wait("Orphan releases the member", lambda:
             condition(primary.get("frc", name), "Ready") == "True" and
             not member.get("raycluster", member_ray_name)["metadata"].get("labels", {}).get("ray.io/federation-owner"))
        assert member.get("raycluster", member_ray_name)["metadata"]["uid"] == orphan_uid
        assert {p["metadata"]["uid"] for p in workers(member, member_ray_name)} == before_orphan
        (artifacts / "orphan-member.json").write_text(json.dumps(member.get("raycluster", member_ray_name), indent=2))
        preempted = workers(member, member_ray_name)[0]
        member.run("delete", "pod", preempted["metadata"]["name"], "--wait=false")
        wait("orphaned member repairs workers independently", lambda: ready_workers(member, member_ray_name, 1) and
             all(p["metadata"]["uid"] != preempted["metadata"]["uid"] for p in workers(member, member_ray_name)))
        assert "REMOTE_TASK_OK" in ray_job(primary, name, SMOKE_TEST)
        member.run("delete", "raycluster", member_ray_name, "--wait=false")
        primary.run("delete", "frc", name, "--wait=false")
        wait("Orphan test resources are cleaned up explicitly", lambda:
             not primary.get("frc")["items"] and not primary.get("raycluster")["items"] and
             not member.get("raycluster")["items"] and not federation_pods(primary, name) and not federation_pods(member, member_ray_name), 180)
        results.append("Orphan preserves worker identities and supports local preemption recovery")
        (artifacts / "results.json").write_text(json.dumps({"passed": results, "elapsed_seconds": round(time.monotonic() - started, 1)}, indent=2))
        print("ALL FEDERATION KIND TESTS PASSED", flush=True)
    finally:
        if partitioned:
            for destination in partition_destinations:
                command(["docker", "exec", args.member_node, "iptables", "-D", "FORWARD", "-d", destination, "-j", "DROP"])
        for side, cluster in [("primary", primary), ("member", member)]:
            for resource in ["frc", "raycluster", "pods", "leases", "configmaps"]:
                try:
                    (artifacts / (side + "-" + resource + ".json")).write_text(json.dumps(cluster.get(resource), indent=2))
                except RuntimeError:
                    pass
            try:
                logs = cluster.run("logs", "-n", "kuberay-system", "deployment/kuberay-operator", "--tail=500")
                (artifacts / (side + "-operator.log")).write_text(logs)
            except RuntimeError:
                pass


if __name__ == "__main__":
    main()
