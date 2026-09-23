#!/usr/bin/env python3
"""Prepare a persistent demo on the two routed kind clusters from run-kind.sh."""
import argparse
import json
import os
from pathlib import Path
import subprocess


def run(args, value=None):
    result = subprocess.run(args, input=json.dumps(value).encode() if value else None,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr.decode())
    return result.stdout.decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary-kubeconfig", required=True)
    parser.add_argument("--member-kubeconfig", required=True)
    parser.add_argument("--member-node", default="frc-member-control-plane")
    parser.add_argument("--primary-label", default="frc-primary")
    parser.add_argument("--member-label", default="frc-member")
    parser.add_argument("--namespace", default="ray-federation-demo")
    parser.add_argument("--name", default="ray-demo")
    parser.add_argument("--head-address", default="10.96.0.110")
    parser.add_argument("--image", default="rayproject/ray:2.56.0-py311-cpu")
    parser.add_argument("--terminal-kubectl-host-path", default="/usr/bin/kubectl")
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args()
    state = Path(args.state_dir).resolve()
    state.mkdir(parents=True, exist_ok=True)
    os.chmod(str(state), 0o700)
    configs = [str(Path(p).resolve()) for p in (args.primary_kubeconfig, args.member_kubeconfig)]
    bases = [["kubectl", "--kubeconfig", p, "-n", args.namespace] for p in configs]
    primary, member = bases

    def apply(base, resource):
        run(base + ["apply", "--server-side", "--field-manager=federation-demo", "-f", "-"], resource)

    for base in bases:
        apply(base, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": args.namespace}})

    # The browser terminal enters this disposable toolbox instead of a host shell.
    # Its ServiceAccount is deliberately limited to the demo namespace.
    terminal_name = "federation-terminal"
    terminal_labels = {"app.kubernetes.io/name": terminal_name}
    terminal_metadata = {"name": terminal_name, "namespace": args.namespace}
    bashrc = """\
[[ -f /etc/bash.bashrc ]] && source /etc/bash.bashrc
export HISTFILE=/workspace/.bash_history
export HISTCONTROL=ignoredups:erasedups
export HISTSIZE=2000
shopt -s histappend
PROMPT_COMMAND='history -a'
alias k=kubectl
cd /workspace
PS1='\\[\\e[38;5;62m\\]${DEMO_CLUSTER}\\[\\e[0m\\]:\\[\\e[38;5;242m\\]\\w\\[\\e[0m\\] $ '
"""
    for base, label in zip(bases, (args.primary_label, args.member_label)):
        apply(base, {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": terminal_metadata})
        apply(base, {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": terminal_metadata,
            "rules": [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}],
        })
        apply(base, {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": terminal_metadata,
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": terminal_name},
            "subjects": [{"kind": "ServiceAccount", "name": terminal_name, "namespace": args.namespace}],
        })
        apply(base, {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": terminal_metadata,
            "data": {"bashrc": bashrc},
        })
        apply(base, {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": terminal_metadata,
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": terminal_labels},
                "template": {
                    "metadata": {"labels": terminal_labels},
                    "spec": {
                        "serviceAccountName": terminal_name,
                        "containers": [{
                            "name": "shell",
                            "image": args.image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/bash", "-lc"],
                            "args": ["""
set -euo pipefail
mkdir -p "$HOME/.kube" /workspace
cat > "$HOME/.kube/config" <<EOF
apiVersion: v1
kind: Config
current-context: demo
clusters:
- name: demo
  cluster:
    server: https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT_HTTPS}
    certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
users:
- name: terminal
  user:
    tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
contexts:
- name: demo
  context:
    cluster: demo
    user: terminal
    namespace: ${DEMO_NAMESPACE}
EOF
chmod 0600 "$HOME/.kube/config"
touch /workspace/.bash_history
exec sleep infinity
"""],
                            "env": [
                                {"name": "DEMO_CLUSTER", "value": label},
                                {"name": "DEMO_NAMESPACE", "value": args.namespace},
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "runAsNonRoot": True,
                            },
                            "volumeMounts": [
                                {"name": "kubectl", "mountPath": "/usr/local/bin/kubectl", "readOnly": True},
                                {"name": "bashrc", "mountPath": "/etc/federation-terminal", "readOnly": True},
                                {"name": "workspace", "mountPath": "/workspace"},
                            ],
                        }],
                        "volumes": [
                            {"name": "kubectl", "hostPath": {"path": args.terminal_kubectl_host_path, "type": "File"}},
                            {"name": "bashrc", "configMap": {"name": terminal_name}},
                            {"name": "workspace", "emptyDir": {}},
                        ],
                    },
                },
            },
        })
        run(base + ["rollout", "status", "deployment/" + terminal_name, "--timeout=120s"])
    metadata = {"name": "federation-demo", "namespace": args.namespace}
    apply(member, {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata})
    apply(member, {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": metadata,
                   "rules": [{"apiGroups": ["ray.io"], "resources": ["rayclusters"],
                              "verbs": ["get", "list", "watch", "create", "patch", "update", "delete"]},
                             {"apiGroups": [""], "resources": ["pods"],
                              "verbs": ["get", "list", "watch"]}]})
    apply(member, {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": metadata,
                   "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "federation-demo"},
                   "subjects": [{"kind": "ServiceAccount", "name": "federation-demo", "namespace": args.namespace}]})
    identity_role = "federation-demo-member-identity"
    apply(member, {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
                   "metadata": {"name": identity_role},
                   "rules": [{"apiGroups": [""], "resources": ["namespaces"],
                              "resourceNames": ["kube-system"], "verbs": ["get"]}]})
    apply(member, {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
                   "metadata": {"name": identity_role},
                   "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": identity_role},
                   "subjects": [{"kind": "ServiceAccount", "name": "federation-demo", "namespace": args.namespace}]})
    config = json.loads(run(member + ["config", "view", "--raw", "--minify", "-o", "json"]))
    node = json.loads(run(["docker", "inspect", args.member_node]))[0]
    address = node["NetworkSettings"]["Networks"]["kind"]["IPAddress"]
    token = run(member + ["create", "token", "federation-demo", "--duration=24h"]).strip()
    credential = {"apiVersion": "v1", "kind": "Config", "current-context": "member",
                  "contexts": [{"name": "member", "context": {"cluster": "member", "user": "demo"}}],
                  "clusters": [{"name": "member", "cluster": {"server": "https://{}:6443".format(address),
                               "certificate-authority-data": config["clusters"][0]["cluster"]["certificate-authority-data"]}}],
                  "users": [{"name": "demo", "user": {"token": token}}]}
    apply(primary, {"apiVersion": "v1", "kind": "Secret",
                    "metadata": {"name": "member-b-kubeconfig", "namespace": args.namespace,
                                 "labels": {"ray.io/federation-credential": "true"}},
                    "stringData": {"kubeconfig": json.dumps(credential)}})
    existing = run(primary + ["get", "frc", args.name, "--ignore-not-found", "-o", "json"])
    if not existing.strip():
        root = Path(__file__).resolve().parents[3]
        sample = root / "ray-operator/config/samples/ray-federation.yaml"
        frc = json.loads(run(primary + ["create", "--dry-run=client", "-f", str(sample), "-o", "json"]))
        frc["metadata"] = {"name": args.name, "namespace": args.namespace}
        # Keep the default demo declarative while showing the FRC API location
        # used by an autoscaled federation.
        frc["spec"]["primaryCluster"]["enableInTreeAutoscaling"] = False
        frc["spec"]["memberClusters"][0]["namespace"] = args.namespace
        frc["spec"]["networking"]["headEndpoint"]["address"] = args.head_address
        head = frc["spec"]["primaryCluster"]["headGroupSpec"]
        head["headService"] = {"metadata": {"name": args.name + "-head"}, "spec": {"clusterIP": args.head_address}}
        templates = [head["template"]] + [g["template"] for g in frc["spec"]["primaryCluster"]["workerGroups"]]
        templates += [g["template"] for g in frc["spec"]["memberClusters"][0]["workerGroups"]]
        for template in templates:
            container = template["spec"]["containers"][0]
            container["image"] = args.image
            container["imagePullPolicy"] = "IfNotPresent"
        apply(primary, frc)
        print("Created {}/{}: head + 1 local worker + 2 member workers".format(args.namespace, args.name))
    else:
        print("Existing FRC preserved; refreshed member credential.")
    public = {"name": args.name, "namespace": args.namespace, "clusters": [
        {"id": "primary", "label": args.primary_label, "role": "primary", "namespace": args.namespace,
         "kubeconfig": configs[0], "terminalTarget": "deployment/" + terminal_name, "terminalContainer": "shell"},
        {"id": "member-b", "label": args.member_label, "role": "member", "memberName": "member-b",
         "namespace": args.namespace, "kubeconfig": configs[1],
         "terminalTarget": "deployment/" + terminal_name, "terminalContainer": "shell"},
    ]}
    filename = state / "config.json"
    filename.write_text(json.dumps(public, indent=2) + "\n")
    os.chmod(str(filename), 0o600)
    yaml = run(primary + ["get", "frc", args.name, "-o", "yaml"])
    (state / "frc.yaml").write_text(yaml)
    print("FEDERATION_DEMO_CONFIG={}".format(filename))
    print("Requested a 24-hour member token. Re-run this script to refresh it.")


if __name__ == "__main__":
    main()
