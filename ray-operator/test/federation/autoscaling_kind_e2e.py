#!/usr/bin/env python3
"""Exercise Ray v2 federation autoscaling against two prepared kind clusters.

The clusters must already run the federation operator and route each other's
Pod and Service networks. No operator deployment or node networking is changed.
Only a dedicated namespace and two uniquely named RBAC resources are created.
The Python standard library and kubectl are sufficient (Python 3.6+).
"""

import argparse
import copy
import json
import re
import time
import traceback
from pathlib import Path

from kind_e2e import Cluster, command, condition, ready_workers, wait, workers


ACTOR_CLASS = '''
@ray.remote
class Probe:
    def identity(self):
        return {"pod": os.environ["RAY_CLOUD_INSTANCE_ID"],
                "group": os.environ["RAY_NODE_TYPE_NAME"],
                "node_id": ray.get_runtime_context().get_node_id()}
'''


def actor_pod_name(identity):
    """Resolve Ray's existing member-qualified identity to a Kubernetes Pod."""
    prefix = {"primary-cpu": "", "member-b-cpu": "member-b/",
              "member-c-cpu": "member-c/"}[identity["group"]]
    instance_id = identity["pod"]
    assert instance_id.startswith(prefix), identity
    pod = instance_id[len(prefix):]
    assert pod and "/" not in pod, identity
    return pod


class AutoscalingTest:
    def __init__(self, args):
        self.args = args
        self.primary = Cluster(args.primary_kubeconfig, args.namespace)
        self.member = Cluster(args.member_kubeconfig, args.namespace)
        self.artifacts = Path(args.artifacts)
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.results = []
        self.samples = []
        self.created_namespaces = []
        self.credentials = {}
        self.started = time.monotonic()
        self.sequence = 0
        self.role_name = args.namespace + "-identity"
        self.members = {"member-b": args.name + "-member-b",
                        "member-c": args.name + "-member-c"}
        self.groups = {"primary-cpu": (self.primary, args.name),
                       "member-b-cpu": (self.member, self.members["member-b"]),
                       "member-c-cpu": (self.member, self.members["member-c"])}

    def record(self, description):
        self.results.append(description)
        print("PASS " + description, flush=True)

    def head(self):
        return next(p for p in self.primary.get("pods")["items"]
                    if p["metadata"].get("labels", {}).get("ray.io/cluster") == self.args.name
                    and p["metadata"]["labels"].get("ray.io/node-type") == "head"
                    and "deletionTimestamp" not in p["metadata"])

    def ray(self, code):
        self.sequence += 1
        script = ("import json, os, ray\n"
                  "ray.init(address='auto', namespace=" + repr(self.args.namespace) +
                  ", log_to_driver=False)\n" + code + "\nray.shutdown()\n")
        output = self.primary.run("exec", "-i", self.head()["metadata"]["name"],
                                  "-c", "ray-head", "--", "python", "-",
                                  data=script.encode(), timeout=self.args.timeout + 30)
        (self.artifacts / "ray-{:02d}.log".format(self.sequence)).write_text(output)
        return next((json.loads(line[len("RESULT="):]) for line in output.splitlines()
                     if line.startswith("RESULT=")), None)

    def create_actors(self, requests):
        # Detached actors retain their CPU/resource reservations after this
        # submitting driver exits. This avoids timing-dependent short tasks.
        self.ray(ACTOR_CLASS + "\nrequests = " + repr(requests) + '''
for name, resource in requests:
    Probe.options(name=name, lifetime="detached", num_cpus=1,
                  resources={resource: 1}).remote()
print("RESULT=" + json.dumps({"submitted": len(requests)}))
''')

    def identities(self, names):
        return self.ray("names = " + repr(names) + '''
identities = ray.get([ray.get_actor(name).identity.remote() for name in names], timeout=90)
print("RESULT=" + json.dumps(dict(zip(names, identities))))
''')

    def kill_actors(self, names):
        self.ray("names = " + repr(names) + '''
for name in names:
    try:
        ray.kill(ray.get_actor(name), no_restart=True)
    except ValueError:
        pass
print("RESULT=" + json.dumps({"killed": len(names)}))
''')

    def observe(self):
        prc = self.primary.get("raycluster", self.args.name)
        current = {g["groupName"]: g for g in prc["spec"]["workerGroupSpecs"]}
        sample = {"elapsed_seconds": round(time.monotonic() - self.started, 1), "groups": {}}
        for group, (cluster, name) in self.groups.items():
            resource = prc if cluster is self.primary else cluster.get("raycluster", name)
            group_spec = next(g for g in resource["spec"]["workerGroupSpecs"] if g["groupName"] == group)
            for intent in (current[group], group_spec):
                assert all("/" not in pod for pod in intent.get("scaleStrategy", {}).get("workersToDelete", [])), intent
            pods = workers(cluster, name)
            sample["groups"][group] = {
                "primary_replicas": current[group].get("replicas", 0),
                "member_replicas": group_spec.get("replicas", 0),
                "pods": sorted(p["metadata"]["name"] for p in pods),
                "pod_uids": sorted(p["metadata"]["uid"] for p in pods),
                "workers_to_delete": current[group].get("scaleStrategy", {}).get("workersToDelete", []),
            }
        self.samples.append(sample)
        return sample["groups"]

    def capacity(self, targets):
        observations = self.observe()
        for group, replicas in targets.items():
            observed = observations[group]
            cluster, name = self.groups[group]
            if (observed["primary_replicas"] != replicas or
                    observed["member_replicas"] != replicas or
                    not ready_workers(cluster, name, replicas)):
                return False
        return True

    def wait_capacity(self, description, targets):
        wait(description, lambda: self.capacity(targets), self.args.timeout)

    def assert_stable(self, description, check, seconds=None):
        deadline = time.monotonic() + (seconds or self.args.observation_seconds)
        while time.monotonic() < deadline:
            check(self.observe())
            time.sleep(2)
        self.record(description)

    def prepare(self):
        # Never remove a namespace that existed before this invocation.
        for cluster in (self.primary, self.member):
            existing = cluster.run("get", "namespace", self.args.namespace,
                                   "--ignore-not-found", "-o", "name").strip()
            if existing:
                raise RuntimeError("Namespace {} already exists; use --namespace for an isolated test".format(self.args.namespace))
        for cluster in (self.primary, self.member):
            cluster.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": self.args.namespace, "labels": {"app.kubernetes.io/managed-by": "frc-autoscaling-e2e"}}})
            self.created_namespaces.append(cluster)

        metadata = {"name": "federation-client", "namespace": self.args.namespace}
        self.member.apply({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata})
        self.member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
                           "metadata": metadata, "rules": [
                               {"apiGroups": ["ray.io"], "resources": ["rayclusters"],
                                "verbs": ["get", "list", "watch", "create", "patch", "update", "delete"]},
                               {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]}]})
        self.member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                           "metadata": metadata,
                           "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "federation-client"},
                           "subjects": [{"kind": "ServiceAccount", "name": "federation-client", "namespace": self.args.namespace}]})
        self.member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
                           "metadata": {"name": self.role_name}, "rules": [
                               {"apiGroups": [""], "resources": ["namespaces"],
                                "resourceNames": ["kube-system"], "verbs": ["get"]}]})
        self.member.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
                           "metadata": {"name": self.role_name},
                           "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": self.role_name},
                           "subjects": [{"kind": "ServiceAccount", "name": "federation-client", "namespace": self.args.namespace}]})
        config = json.loads(self.member.run("config", "view", "--raw", "--minify", "-o", "json"))
        server = self.args.member_server
        if not server:
            node = json.loads(command(["docker", "inspect", self.args.member_node]))[0]
            server = "https://{}:6443".format(node["NetworkSettings"]["Networks"]["kind"]["IPAddress"])
        token = self.member.run("create", "token", "federation-client", "--duration=24h").strip()
        credential = {"apiVersion": "v1", "kind": "Config", "current-context": "member",
                      "contexts": [{"name": "member", "context": {"cluster": "member", "user": "member"}}],
                      "clusters": [{"name": "member", "cluster": {"server": server,
                          "certificate-authority-data": config["clusters"][0]["cluster"]["certificate-authority-data"]}}],
                      "users": [{"name": "member", "user": {"token": token}}]}
        for name in self.members:
            secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {
                "name": name + "-kubeconfig", "namespace": self.args.namespace,
                "labels": {"ray.io/federation-credential": "true"}},
                "stringData": {"kubeconfig": json.dumps(credential)}}
            self.credentials[name] = secret
            self.primary.apply(secret)

        # This external endpoint selects the generated head. A separately
        # allocated Service avoids hard-coded IPs or adopting controller objects.
        self.primary.apply({"apiVersion": "v1", "kind": "Service", "metadata": {
            "name": "autoscaling-head-endpoint", "namespace": self.args.namespace},
            "spec": {"selector": {"ray.io/cluster": self.args.name, "ray.io/node-type": "head"},
                     "ports": [{"name": "gcs", "port": 6379, "targetPort": 6379}]}})
        address = self.primary.get("service", "autoscaling-head-endpoint")["spec"]["clusterIP"]
        sample = Path(__file__).resolve().parents[2] / "config/samples/ray-federation.yaml"
        frc = json.loads(self.primary.run("create", "--dry-run=client", "-f", str(sample), "-o", "json"))
        frc["metadata"] = {"name": self.args.name, "namespace": self.args.namespace}
        primary = frc["spec"]["primaryCluster"]
        frc["spec"]["primaryCluster"]["enableInTreeAutoscaling"] = True
        frc["spec"]["primaryCluster"]["autoscalerOptions"] = {"version": "v2", "idleTimeoutSeconds": 15}
        primary["headGroupSpec"]["rayStartParams"]["num-cpus"] = "0"
        local = primary["workerGroups"][0]
        local.update({"groupName": "primary-cpu", "replicas": 0, "minReplicas": 0,
                      "maxReplicas": 2, "resources": {"CPU": "1", "primary": "1"}})
        template = copy.deepcopy(frc["spec"]["memberClusters"][0])
        frc["spec"]["memberClusters"] = []
        for name in self.members:
            member = copy.deepcopy(template)
            member.update({"name": name, "namespace": self.args.namespace,
                           "kubeconfigSecretRef": {"name": name + "-kubeconfig"}})
            member["workerGroups"][0].update({"groupName": name + "-cpu", "replicas": 0,
                "minReplicas": 0, "maxReplicas": 2, "resources": {"CPU": "1", name.replace("-", "_"): "1"}})
            frc["spec"]["memberClusters"].append(member)
        frc["spec"]["networking"]["headEndpoint"].update({"mode": "UserProvided", "address": address, "gcsPort": 6379})
        templates = [primary["headGroupSpec"]["template"], local["template"]]
        templates += [m["workerGroups"][0]["template"] for m in frc["spec"]["memberClusters"]]
        for template in templates:
            container = template["spec"]["containers"][0]
            container["image"] = self.args.image
            container["imagePullPolicy"] = "IfNotPresent"
        (self.artifacts / "frc-input.json").write_text(json.dumps(frc, indent=2))
        self.primary.apply(frc)

    def exercise(self):
        zero = {name: 0 for name in self.groups}
        wait("autoscaling federation Ready with zero workers", lambda:
             condition(self.primary.get("frc", self.args.name), "Ready") == "True" and self.capacity(zero),
             self.args.timeout)
        head = self.head()
        assert [c["name"] for c in head["spec"]["containers"]].count("autoscaler") == 1
        for name in self.members.values():
            mrc = self.member.get("raycluster", name)
            assert mrc["spec"]["enableInTreeAutoscaling"] is False
            assert "headGroupSpec" not in mrc["spec"]
        self.record("one primary autoscaler owns local and two same-namespace remote groups")

        self.create_actors([("local-1", "primary"), ("b-1", "member_b"), ("c-1", "member_c")])
        self.wait_capacity("real actor demand scales all three groups 0 -> 1", {g: 1 for g in self.groups})
        first = self.identities(["local-1", "b-1", "c-1"])
        for actor, group in (("local-1", "primary-cpu"), ("b-1", "member-b-cpu"), ("c-1", "member-c-cpu")):
            cluster, name = self.groups[group]
            assert first[actor]["group"] == group, first
            assert actor_pod_name(first[actor]) in {p["metadata"]["name"] for p in workers(cluster, name)}, first
        assert len({value["node_id"] for value in first.values()}) == 3, first

        self.create_actors([("local-2", "primary"), ("b-2", "member_b"), ("c-2", "member_c")])
        self.wait_capacity("additional actor demand scales all groups 1 -> 2", {g: 2 for g in self.groups})
        second = self.identities(["local-2", "b-2", "c-2"])
        assert not ({v["pod"] for v in first.values()} & {v["pod"] for v in second.values()}), second
        self.record("real CPU/custom-resource demand independently scales local and both remote groups")

        self.create_actors([("b-over-limit", "member_b")])
        pending = self.ray('''
ready, pending = ray.wait([ray.get_actor("b-over-limit").identity.remote()], timeout=10)
print("RESULT=" + json.dumps({"ready": len(ready), "pending": len(pending)}))
''')
        assert pending == {"ready": 0, "pending": 1}, pending

        def bounded(observations):
            for group in observations.values():
                assert group["primary_replicas"] <= 2, group
                assert group["member_replicas"] <= 2, group
                assert len(group["pods"]) <= 2, group
        self.assert_stable("maxReplicas keeps excess real demand pending without overprovisioning", bounded)
        self.kill_actors(["b-over-limit"])

        self.kill_actors(["local-2", "b-2", "c-2"])
        self.wait_capacity("idle workers scale 2 -> 1 while busy actors survive", {g: 1 for g in self.groups})
        retained = self.identities(["local-1", "b-1", "c-1"])
        assert retained == first, {"before": first, "after": retained}
        observed = self.observe()
        for actor, group in (("local-1", "primary-cpu"), ("b-1", "member-b-cpu"), ("c-1", "member-c-cpu")):
            assert observed[group]["pods"] == [actor_pod_name(first[actor])], observed
        self.record("targeted idle scale-down preserves exact busy Pod and actor identities in all groups")

        # A broken member credential must not turn unknown capacity into zero
        # inventory or allow decisions to overwrite the last known target.
        broken = copy.deepcopy(self.credentials["member-b"])
        broken["stringData"]["kubeconfig"] = "invalid kubeconfig"
        self.primary.apply(broken)
        wait("member credential outage is reported", lambda:
             condition(self.primary.get("frc", self.args.name), "MemberControlPlaneReachable") == "False", 90)
        before = self.observe()["member-b-cpu"]
        self.create_actors([("b-after-outage", "member_b")])

        def frozen(observations):
            after = observations["member-b-cpu"]
            for key in ("primary_replicas", "member_replicas", "pods", "pod_uids"):
                assert after[key] == before[key], {"before": before, "after": after}
        self.assert_stable("unavailable member cannot scale up using stale inventory", frozen)
        self.kill_actors(["b-after-outage", "b-1"])
        self.assert_stable("unavailable member cannot scale down even after its worker becomes idle", frozen)
        self.create_actors([("b-recovered-1", "member_b"), ("b-recovered-2", "member_b")])
        self.primary.apply(self.credentials["member-b"])
        self.wait_capacity("credential recovery resumes demand-driven remote scale-up", {
            "primary-cpu": 1, "member-b-cpu": 2, "member-c-cpu": 1})
        self.identities(["b-recovered-1", "b-recovered-2"])
        wait("federation Ready after credential recovery", lambda:
             condition(self.primary.get("frc", self.args.name), "Ready") == "True", self.args.timeout)
        assert self.identities(["local-1", "c-1"]) == {key: first[key] for key in ("local-1", "c-1")}
        self.record("restored credentials resume scaling without replacing unaffected busy actors")

        self.kill_actors(["local-1", "c-1", "b-recovered-1", "b-recovered-2"])
        self.wait_capacity("all idle worker groups scale down to zero", zero)
        wait("Ray reports only the head after worker termination", lambda: len(self.ray(
            'print("RESULT=" + json.dumps([n["NodeID"] for n in ray.nodes() if n["Alive"]]))')) == 1,
            self.args.timeout)
        self.record("local and remote capacity returns to zero; only the Ray head remains")

        def set_minimum(replicas):
            frc = self.primary.get("frc", self.args.name)
            local = frc["spec"]["primaryCluster"]["workerGroups"]
            members = frc["spec"]["memberClusters"]
            groups = local + [group for member in members for group in member["workerGroups"]]
            for group in groups:
                assert group["replicas"] == 0, group
                group["minReplicas"] = replicas
            # Change only the FRC policy. Initial replicas remain zero, and the
            # autoscaler must write live PRC targets without manual intervention.
            self.primary.patch("frc", self.args.name, {"spec": {
                "primaryCluster": {"workerGroups": local}, "memberClusters": members}})

        set_minimum(1)
        self.wait_capacity("raising FRC minReplicas hot-scales idle groups 0 -> 1", {g: 1 for g in self.groups})
        minimum_pods = self.observe()

        def minimum_retained(observations):
            for group, observed in observations.items():
                assert observed["primary_replicas"] == observed["member_replicas"] == 1, observed
                assert len(observed["pods"]) == 1, observed
                assert observed["pod_uids"] == minimum_pods[group]["pod_uids"], observed
            assert self.head()["metadata"]["uid"] == head["metadata"]["uid"]

        self.assert_stable("minimum capacity survives beyond the idle timeout without actor demand", minimum_retained)
        set_minimum(0)
        self.wait_capacity("lowering FRC minReplicas hot-scales idle groups 1 -> 0", zero)
        assert self.head()["metadata"]["uid"] == head["metadata"]["uid"]
        self.record("FRC minReplicas hot reloads across local and both remote groups without changing initial replicas or restarting the head")
        self.collect("before-delete")
        self.primary.run("delete", "frc", self.args.name, "--wait=false")
        wait("FRC deletion removes generated RayClusters and Pods", lambda:
             not self.primary.get("frc")["items"] and not self.primary.get("rayclusters")["items"] and
             not self.member.get("rayclusters")["items"] and not self.primary.get("pods")["items"] and
             not self.member.get("pods")["items"], self.args.timeout)
        self.record("federation deletion cleans up both same-namespace members and autoscaler")

    def collect(self, prefix):
        directory = self.artifacts / prefix
        directory.mkdir(parents=True, exist_ok=True)
        for side, cluster in (("primary", self.primary), ("member", self.member)):
            # Secrets/kubeconfig data are deliberately never collected.
            for kind in ("frc", "rayclusters", "pods", "events", "services"):
                try:
                    (directory / (side + "-" + kind + ".json")).write_text(json.dumps(cluster.get(kind), indent=2))
                except RuntimeError:
                    pass
            try:
                output = cluster.run("logs", "-n", self.args.operator_namespace,
                                     "deployment/" + self.args.operator_deployment, "--tail=500")
                (directory / (side + "-operator.log")).write_text(output)
            except RuntimeError:
                pass
        try:
            head = self.head()["metadata"]["name"]
            for container in ("ray-head", "autoscaler"):
                (directory / (container + ".log")).write_text(self.primary.run(
                    "logs", head, "-c", container, "--tail=1000"))
        except (RuntimeError, StopIteration):
            pass
        (self.artifacts / "observations.json").write_text(json.dumps(self.samples, indent=2))

    def cleanup(self):
        if not self.created_namespaces:
            return
        # Restore test credentials first so an FRC finalizer can clean up remote
        # objects even when a credential-outage assertion fails.
        for secret in self.credentials.values():
            self.primary.apply(secret)
        self.primary.run("delete", "frc", self.args.name, "--ignore-not-found", "--wait=false")
        wait("test FRC finalizer finishes", lambda:
             not self.primary.run("get", "frc", self.args.name, "--ignore-not-found", "-o", "name").strip(),
             self.args.timeout)
        for kind in ("clusterrolebinding", "clusterrole"):
            self.member.run("delete", kind, self.role_name, "--ignore-not-found")
        for cluster in self.created_namespaces:
            cluster.run("delete", "namespace", self.args.namespace, "--wait=false")
        for cluster in self.created_namespaces:
            wait("test namespace removed", lambda cluster=cluster:
                 not cluster.run("get", "namespace", self.args.namespace, "--ignore-not-found", "-o", "name").strip(), 120)

    def run(self):
        succeeded = False
        try:
            self.prepare()
            self.exercise()
            succeeded = True
        except Exception:
            (self.artifacts / "failure.txt").write_text(traceback.format_exc())
            raise
        finally:
            if self.created_namespaces:
                self.collect("final")
            cleanup_error = None
            try:
                if succeeded or not self.args.keep_on_failure:
                    self.cleanup()
                elif self.created_namespaces:
                    print("Retained test namespace {} for inspection".format(self.args.namespace), flush=True)
            except Exception as error:
                cleanup_error = str(error)
                raise
            finally:
                (self.artifacts / "results.json").write_text(json.dumps({
                    "success": succeeded and cleanup_error is None, "passed": self.results,
                    "cleanup_error": cleanup_error,
                    "elapsed_seconds": round(time.monotonic() - self.started, 1)}, indent=2))
        print("ALL FEDERATION AUTOSCALING KIND TESTS PASSED", flush=True)


def main(test_class=AutoscalingTest):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary-kubeconfig", required=True)
    parser.add_argument("--member-kubeconfig", required=True)
    parser.add_argument("--member-node", default="frc-member-control-plane")
    parser.add_argument("--member-server", help="Member API HTTPS endpoint reachable from primary Pods")
    parser.add_argument("--namespace", default="ray-federation-autoscale-e2e")
    parser.add_argument("--name", default="ray-autoscale")
    parser.add_argument("--image", default="rayproject/ray:2.56.0-py311-cpu")
    parser.add_argument("--artifacts", required=True)
    parser.add_argument("--timeout", type=int, default=480, help="Seconds allowed for each convergence")
    parser.add_argument("--observation-seconds", type=int, default=45,
                        help="Each safety check must remain true for at least this duration")
    parser.add_argument("--operator-namespace", default="kuberay-system")
    parser.add_argument("--operator-deployment", default="kuberay-operator")
    parser.add_argument("--keep-on-failure", action="store_true")
    args = parser.parse_args()
    if args.timeout < 90 or args.observation_seconds < 30:
        parser.error("--timeout must be at least 90 and --observation-seconds at least 30")
    for field, limit in (("namespace", 63), ("name", 30)):
        value = getattr(args, field)
        if len(value) > limit or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", value):
            parser.error("--{} must be a DNS label of at most {} characters".format(field, limit))
    test_class(args).run()


if __name__ == "__main__":
    main()
