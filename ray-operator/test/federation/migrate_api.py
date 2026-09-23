#!/usr/bin/env python3
"""Export, then apply the FRC alpha API migration during operator maintenance.

Stop the primary operator (and webhook admission, if installed), export the plan
before replacing the CRD, install the new CRD, apply the saved plan, then start
the new operator. Ray Pods continue running. No member credentials are exported.
"""

import argparse
import copy
import json
import subprocess
from pathlib import Path


def migrated_spec(frc, primary):
    owner = next((o for o in primary["metadata"].get("ownerReferences", [])
                  if o.get("controller")), {})
    if owner.get("uid") != frc["metadata"]["uid"]:
        raise ValueError("Primary RayCluster ownership does not match the FRC")
    spec = copy.deepcopy(frc["spec"])
    for key in ("enableInTreeAutoscaling", "autoscalerOptions"):
        if key in spec:
            legacy = spec.pop(key)
            if key in spec["primaryCluster"] and spec["primaryCluster"][key] != legacy:
                raise ValueError("Conflicting top-level and primaryCluster." + key)
            spec["primaryCluster"][key] = legacy
    return spec


def runtime_targets(primary):
    return {
        (group["groupName"], group.get("managedBy")):
        {field: copy.deepcopy(group.get(field)) for field in ("replicas", "scaleStrategy")}
        for group in primary["spec"].get("workerGroupSpecs", [])
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("export", "apply"))
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--plan", required=True, help="Local JSON backup and migration plan")
    args = parser.parse_args()

    def kubectl(*command, data=None):
        return subprocess.check_output(
            ["kubectl", "--kubeconfig", args.kubeconfig] + list(command),
            input=None if data is None else json.dumps(data).encode(), timeout=60)

    path = Path(args.plan)
    if args.action == "export":
        if path.exists():
            raise ValueError("Refusing to overwrite an existing migration backup")
        entries = []
        for frc in json.loads(kubectl("get", "frc", "-A", "-o", "json"))["items"]:
            namespace, name = frc["metadata"]["namespace"], frc["metadata"]["name"]
            primary = json.loads(kubectl("-n", namespace, "get", "raycluster", name, "-o", "json"))
            entries.append({"before": frc, "primary": primary,
                            "spec": migrated_spec(frc, primary)})
        path.write_text(json.dumps(entries, indent=2) + "\n")
        print("Saved {} FRC migrations and original resources to {}".format(len(entries), path))
        return
    for entry in json.loads(path.read_text()):
        frc, primary = entry["before"], entry["primary"]
        namespace, name = frc["metadata"]["namespace"], frc["metadata"]["name"]
        current = json.loads(kubectl("-n", namespace, "get", "raycluster", name, "-o", "json"))
        if current["metadata"]["uid"] != primary["metadata"]["uid"]:
            raise ValueError("Primary RayCluster was replaced after export")
        if runtime_targets(current) != runtime_targets(primary):
            raise ValueError("Primary targets changed after export; review a new plan")
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": frc["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": frc["metadata"]["resourceVersion"]},
            {"op": "replace", "path": "/spec", "value": entry["spec"]},
        ]
        kubectl("-n", namespace, "patch", "frc", name, "--type=json", "--patch-file=/dev/stdin", data=patch)
        print("Migrated {}/{}".format(namespace, name))


if __name__ == "__main__":
    main()
