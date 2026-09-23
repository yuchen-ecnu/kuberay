#!/usr/bin/env python3
"""Bounded fault injection in two existing kind clusters.

Uses an isolated Ray namespace. Temporarily changes the primary operator's
replica count/leader election, restoring its original deployment in finally.
Network rules target only the test head endpoint and are always removed.
"""

import copy
import datetime
import json
import time

from autoscaling_kind_e2e import AutoscalingTest, main
from kind_e2e import Cluster, command, condition, ready_workers, scale_federation_group, wait, workers


class FaultTest(AutoscalingTest):
    def exercise(self):
        self.operator = Cluster(self.args.primary_kubeconfig, self.args.operator_namespace)
        deployment = self.operator.get("deployment", self.args.operator_deployment)
        self.original_deployment_spec = deployment["spec"]
        self.network_rules = []
        try:
            self.autoscaler_faults()
            self.declarative_lifecycle()
        finally:
            self.restore_network()
            self.operator.patch("deployment", self.args.operator_deployment,
                                {"spec": self.original_deployment_spec})
            self.operator.run("rollout", "status", "deployment/" + self.args.operator_deployment,
                              "--timeout=120s", timeout=150)

    def operator_replicas(self, replicas):
        self.operator.run("scale", "deployment/" + self.args.operator_deployment,
                          "--replicas=" + str(replicas))
        if replicas == 0:
            labels = self.original_deployment_spec["selector"]["matchLabels"]
            selector = ",".join(k + "=" + v for k, v in labels.items())
            wait("primary operator paused", lambda: not json.loads(self.operator.run(
                "get", "pods", "-l", selector, "-o", "json"))["items"], 90)
        else:
            self.operator.run("rollout", "status", "deployment/" + self.args.operator_deployment,
                              "--timeout=120s", timeout=150)

    def publish_test_snapshot(self, snapshot):
        snapshot = copy.deepcopy(snapshot)
        primary = self.primary.get("raycluster", self.args.name)
        snapshot["primaryGeneration"] = primary["metadata"]["generation"]
        snapshot["observedAt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.primary.patch("configmap", self.args.name + "-autoscaler",
                           {"data": {"snapshot.json": json.dumps(snapshot)}})

    def autoscaler_faults(self):
        zero = {g: 0 for g in self.groups}
        self.wait_capacity("initial autoscaled federation", zero)
        self.create_actors([("local", "primary"), ("busy-b", "member_b"), ("busy-c", "member_c")])
        self.wait_capacity("busy actors reserve all groups", {g: 1 for g in self.groups})
        identities = self.identities(["local", "busy-b", "busy-c"])
        head_uid = self.head()["metadata"]["uid"]
        member_uids = {name: self.member.get("raycluster", name)["metadata"]["uid"]
                       for name in self.members.values()}

        # Real two-replica leader handoff; child identities and busy work survive.
        spec = copy.deepcopy(self.original_deployment_spec)
        args = spec["template"]["spec"]["containers"][0]["args"]
        args[:] = [a for a in args if not a.startswith("--enable-leader-election=")]
        args.append("--enable-leader-election=true")
        spec["replicas"] = 2
        self.operator.patch("deployment", self.args.operator_deployment, {"spec": spec})
        self.operator.run("rollout", "status", "deployment/" + self.args.operator_deployment,
                          "--timeout=120s", timeout=150)
        lease = lambda: self.operator.get("lease", "ray-operator-leader")["spec"].get("holderIdentity", "")
        wait("leader acquired", lambda: bool(lease()) and bool(self.operator.run(
            "get", "pod", lease().split("_")[0], "--ignore-not-found", "-o", "name").strip()), 90)
        previous_leader = lease()
        self.operator.run("delete", "pod", previous_leader.split("_")[0], "--wait=false")
        self.create_actors([("after-leader", "member_b")])
        wait("leader handoff", lambda: bool(lease()) and lease() != previous_leader, 90)
        self.wait_capacity("scaling after leader handoff", {
            "primary-cpu": 1, "member-b-cpu": 2, "member-c-cpu": 1})
        assert self.identities(["local", "busy-b", "busy-c"]) == identities
        assert self.head()["metadata"]["uid"] == head_uid
        assert {name: self.member.get("raycluster", name)["metadata"]["uid"]
                for name in self.members.values()} == member_uids
        self.kill_actors(["after-leader"])
        self.wait_capacity("idle extra worker removed", {g: 1 for g in self.groups})
        self.record("two operator replicas hand off leadership without duplicate MRCs, lost targets or busy actor replacement")

        # Hold a fresh but incomplete observation while a real worker joins GCS.
        # Native REQUESTED/allocation transitions are additionally covered by the
        # deterministic full-Reconciler unit tests; here actual Pods/GCS are used.
        self.operator_replicas(0)
        snapshot = json.loads(self.primary.get("configmap", self.args.name + "-autoscaler")["data"]["snapshot.json"])
        before_pods = {p["metadata"]["name"] for p in snapshot["pods"]}
        for cluster, name in ((self.primary, self.args.name), (self.member, self.members["member-b"])):
            resource = cluster.get("raycluster", name)
            for group in resource["spec"]["workerGroupSpecs"]:
                if group["groupName"] == "member-b-cpu":
                    group["replicas"] = 2
            cluster.patch("raycluster", name, {"spec": {"workerGroupSpecs": resource["spec"]["workerGroupSpecs"]}})
        self.publish_test_snapshot(snapshot)
        self.create_actors([("snapshot-lag", "member_b")])
        joined = self.identities(["snapshot-lag"])["snapshot-lag"]
        assert joined["pod"].split("/", 1)[-1] not in before_pods
        for _ in range(6):
            self.publish_test_snapshot(snapshot)
            time.sleep(3)
            assert self.identities(["busy-b"])["busy-b"] == identities["busy-b"]
        logs = self.primary.run("logs", self.head()["metadata"]["name"], "-c", "autoscaler", "--tail=1500")
        (self.artifacts / "fresh-snapshot-lag.log").write_text(logs)
        assert "Waiting for Pod observation of live GCS instance" in logs
        self.operator_replicas(2)
        self.wait_capacity("snapshot catch-up resumes reconciliation", {
            "primary-cpu": 1, "member-b-cpu": 2, "member-c-cpu": 1})
        assert self.identities(["snapshot-lag"])["snapshot-lag"] == joined
        self.kill_actors(["snapshot-lag"])
        self.wait_capacity("normal scale-down after snapshot catch-up", {g: 1 for g in self.groups})
        self.record("real GCS worker missing from a repeatedly refreshed snapshot pauses reconciliation and survives catch-up")

        # Controller/adapter disagreement pauses the old process; a rollback of
        # the observation protocol resumes it without a runtime restart.
        self.operator_replicas(0)
        snapshot = json.loads(self.primary.get("configmap", self.args.name + "-autoscaler")["data"]["snapshot.json"])
        snapshot["adapterRevision"] = "incompatible-upgrade"
        self.publish_test_snapshot(snapshot)
        time.sleep(12)
        logs = self.primary.run("logs", self.head()["metadata"]["name"], "-c", "autoscaler", "--tail=500")
        assert "adapter revision changed" in logs
        assert self.identities(["local", "busy-b", "busy-c"]) == identities
        self.operator_replicas(2)
        wait("compatible observation restored", lambda: condition(
            self.primary.get("frc", self.args.name), "AutoscalerObservationReady") == "True", 90)
        self.record("adapter revision mismatch pauses decisions; compatible controller rollback preserves busy actors")

        self.data_plane_partition()
        self.sidecar_and_head_failure()

    def restore_network(self):
        while self.network_rules:
            node, rule = self.network_rules.pop()
            command(["docker", "exec", node, "iptables", "-D", "FORWARD"] + rule)

    def data_plane_partition(self):
        head = self.head()
        endpoint = self.primary.get("frc", self.args.name)["spec"]["networking"]["headEndpoint"]["address"]
        # kind masquerades some cross-node replies to the node IP. Cover both
        # directions on the primary as well as member-initiated GCS connections.
        primary_node, head_ip = head["spec"]["nodeName"], head["status"]["podIP"]
        rules = [(self.args.member_node, ["-d", head_ip]),
                 (self.args.member_node, ["-d", endpoint]),
                 (primary_node, ["-s", head_ip, "-o", "eth0"]),
                 (primary_node, ["-d", head_ip, "-i", "eth0"])]
        for node, match in rules:
            rule = match + ["-j", "DROP"]
            command(["docker", "exec", node, "iptables", "-I", "FORWARD", "1"] + rule)
            self.network_rules.append((node, rule))
        try:
            result = self.ray('''
try:
    ray.get(ray.get_actor("busy-b").identity.remote(), timeout=8)
    blocked = False
except (ray.exceptions.RayError, ValueError):
    blocked = True
print("RESULT=" + json.dumps({"blocked": blocked}))
''')
            for node in (primary_node, self.args.member_node):
                counters = command(["docker", "exec", node, "iptables", "-L", "FORWARD", "-nvx"])
                (self.artifacts / (node + "-partition-counters.txt")).write_text(counters)
            assert result["blocked"], result
            # Keep the partition long enough to cover Ray's failure detection,
            # recording actual actor recovery instead of equating Running to health.
            until = time.monotonic() + 100
            while time.monotonic() < until:
                assert self.identities(["local"])["local"]["group"] == "primary-cpu"
                time.sleep(5)
        finally:
            self.restore_network()
        recovered = self.ray('''
result = {}
for name in ["busy-b", "busy-c"]:
    try:
        result[name] = ray.get(ray.get_actor(name).identity.remote(), timeout=10)
    except (ray.exceptions.RayError, ValueError) as error:
        result[name] = {"lost": type(error).__name__}
print("RESULT=" + json.dumps(result))
''')
        (self.artifacts / "partition-actors.json").write_text(json.dumps(recovered, indent=2))
        self.kill_actors(["busy-b", "busy-c"])
        self.create_actors([("network-b", "member_b"), ("network-c", "member_c")])
        self.wait_capacity("remote workers recover after long data-plane partition", {g: 1 for g in self.groups})
        assert self.identities(["network-b", "network-c"])["network-b"]["group"] == "member-b-cpu"
        transfer = self.ray('''
import hashlib
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
node = ray.get(ray.get_actor("network-b").identity.remote())["node_id"]
@ray.remote
def echo(value):
    return value
data = b"federation" * (2 * 1024 * 1024)
out = ray.get(echo.options(num_cpus=0, scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node, soft=False)).remote(ray.put(data)), timeout=90)
assert hashlib.sha256(out).digest() == hashlib.sha256(data).digest()
print("RESULT=" + json.dumps({"round_trip_bytes": len(out)}))
''')
        assert transfer["round_trip_bytes"] > 16 * 1024 * 1024
        self.record("100s isolated data-plane partition blocks remote actors; local work survives and remote tasks/object transfer recover (actor losses recorded)")

    def sidecar_and_head_failure(self):
        head = self.head()
        container = next(c for c in head["status"]["containerStatuses"] if c["name"] == "autoscaler")
        # A PID-namespace init can ignore an unhandled signal from a sibling
        # exec. Stop it through the node runtime so this injects a real exit.
        command(["docker", "exec", head["spec"]["nodeName"], "crictl", "stop", "--timeout=1",
                 container["containerID"].split("://", 1)[1]])
        wait("autoscaler container exit observed", lambda: any(
            c["name"] == "autoscaler" and c.get("state", {}).get("terminated")
            for c in self.head()["status"]["containerStatuses"]), 90)
        assert self.head()["metadata"]["uid"] == head["metadata"]["uid"]
        assert self.identities(["local"])["local"]["group"] == "primary-cpu"
        self.collect("sidecar-exited")
        # restartPolicy Never has no automatic sidecar restart. Recovery is an
        # explicit head replacement; without GCS FT its runtime state is lost.
        self.primary.run("delete", "pod", head["metadata"]["name"], "--wait=false")
        wait("new head becomes ready", lambda:
             self.head()["metadata"]["uid"] != head["metadata"]["uid"] and
             any(c["type"] == "Ready" and c["status"] == "True" for c in self.head()["status"].get("conditions", [])), self.args.timeout)
        lost = self.ray('''
try:
    ray.get_actor("local")
    lost = False
except ValueError:
    lost = True
print("RESULT=" + json.dumps({"actor_state_lost": lost}))
''')
        assert lost["actor_state_lost"], lost
        self.create_actors([("new-head-b", "member_b")])
        self.wait_capacity("autoscaler and member recover after explicit head replacement", {
            "primary-cpu": 0, "member-b-cpu": 1, "member-c-cpu": 0})
        self.identities(["new-head-b"])
        self.record("sidecar exit leaves Ray running but requires explicit head replacement; new runtime recovers with old actor state lost")

    def declarative_lifecycle(self):
        frc = self.primary.get("frc", self.args.name)
        self.primary.run("delete", "frc", self.args.name, "--wait=false")
        wait("autoscaled runtime cleaned before declarative test", lambda:
             not self.primary.get("frc")["items"] and not self.primary.get("rayclusters")["items"] and
             not self.member.get("rayclusters")["items"] and not self.primary.get("pods")["items"] and
             not self.member.get("pods")["items"], self.args.timeout)
        frc["metadata"] = {"name": self.args.name, "namespace": self.args.namespace}
        frc.pop("status", None)
        frc["spec"]["primaryCluster"]["enableInTreeAutoscaling"] = False
        frc["spec"]["primaryCluster"].pop("autoscalerOptions", None)
        for member in frc["spec"]["memberClusters"]:
            member["workerGroups"][0]["replicas"] = 1
        self.primary.apply(frc)
        wait("declarative runtime ready", lambda: condition(self.primary.get("frc", self.args.name), "Ready") == "True", self.args.timeout)
        bad = copy.deepcopy(frc["spec"]["memberClusters"][0])
        bad["name"] = "offline-new"
        bad["kubeconfigSecretRef"]["name"] = "missing-credential"
        bad["workerGroups"][0]["groupName"] = "offline-workers"
        frc["spec"]["memberClusters"].append(bad)
        self.primary.apply(frc)
        scale_federation_group(self.primary, self.args.name, "member-b-cpu", 2)
        wait("healthy member scales despite unbound member", lambda:
             ready_workers(self.member, self.members["member-b"], 2), self.args.timeout)
        assert condition(self.primary.get("frc", self.args.name), "Ready") == "False"
        frc["spec"]["memberClusters"].pop()
        self.primary.apply(frc)
        wait("invalid member removal restores readiness", lambda:
             condition(self.primary.get("frc", self.args.name), "Ready") == "True", 90)
        self.record("PRC scale-up reaches healthy member while a newly added member has invalid credentials")

        broken = copy.deepcopy(self.credentials["member-b"])
        broken["stringData"]["kubeconfig"] = "expired"
        self.primary.apply(broken)
        self.primary.run("delete", "frc", self.args.name, "--wait=false")
        wait("healthy second member cleaned while first is unavailable", lambda:
             not self.member.run("get", "raycluster", self.members["member-c"], "--ignore-not-found", "-o", "name").strip(), 120)
        wait("deletion blocked on unavailable member", lambda:
             any(c["reason"] == "DeletionBlocked" for c in self.primary.get("frc", self.args.name)["status"]["conditions"]), 90)
        replacement = copy.deepcopy(self.credentials["member-b"])
        replacement["metadata"]["name"] = "replacement-credential"
        self.primary.apply(replacement)
        deleting = self.primary.get("frc", self.args.name)
        deleting["spec"]["memberClusters"][0]["kubeconfigSecretRef"]["name"] = "replacement-credential"
        self.primary.patch("frc", self.args.name, {"spec": deleting["spec"]})
        wait("replacement credential releases finalizer", lambda:
             not self.primary.get("frc")["items"] and not self.member.get("rayclusters")["items"], 180)
        self.record("deleting FRC cleans healthy members independently and accepts a new Secret reference for the same remote cluster")


if __name__ == "__main__":
    main(FaultTest)
