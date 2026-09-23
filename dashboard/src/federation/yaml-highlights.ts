import {
  isMap,
  isNode,
  isScalar,
  isSeq,
  LineCounter,
  parseDocument,
} from "yaml";

export type DesignField =
  | "managedBy"
  | "rayStartParams.address"
  | "enableInTreeAutoscaling"
  | "primaryCluster.enableInTreeAutoscaling"
  | "primaryCluster.autoscalerOptions";

export interface YamlHighlight {
  field: DesignField;
  startLine: number;
  endLine: number;
}

/** Locate design fields by YAML path, without changing the displayed resource. */
export function resourceHighlights(source: string) {
  const fields: YamlHighlight[] = [];
  const lineCounter = new LineCounter();
  const document = parseDocument(source, { lineCounter });
  const spec = document.get("spec", true);
  if (
    document.errors.length ||
    document.get("apiVersion") !== "ray.io/v1" ||
    !isMap(spec)
  )
    return { fields, workersOnly: false };

  const add = (map: unknown, key: string, field: DesignField) => {
    if (!isMap(map)) return;
    const pair = map.items.find(
      (item) => isScalar(item.key) && item.key.value === key,
    );
    if (
      !pair ||
      !isNode(pair.key) ||
      !isNode(pair.value) ||
      !pair.key.range ||
      !pair.value.range
    )
      return;
    fields.push({
      field,
      startLine: lineCounter.linePos(pair.key.range[0]).line - 1,
      endLine: lineCounter.linePos(pair.value.range[1] - 1).line - 1,
    });
  };

  if (document.get("kind") === "FederatedRayCluster") {
    const primary = spec.get("primaryCluster", true);
    add(
      primary,
      "enableInTreeAutoscaling",
      "primaryCluster.enableInTreeAutoscaling",
    );
    add(primary, "autoscalerOptions", "primaryCluster.autoscalerOptions");
    return { fields, workersOnly: false };
  }
  if (document.get("kind") !== "RayCluster")
    return { fields, workersOnly: false };

  const workersOnly = !spec.has("headGroupSpec");
  const groups = spec.get("workerGroupSpecs", true);
  if (isSeq(groups)) {
    for (const group of groups.items) {
      if (!isMap(group)) continue;
      add(group, "managedBy", "managedBy");
      if (workersOnly)
        add(
          group.get("rayStartParams", true),
          "address",
          "rayStartParams.address",
        );
    }
  }
  if (spec.get("enableInTreeAutoscaling") === false)
    add(spec, "enableInTreeAutoscaling", "enableInTreeAutoscaling");
  return { fields, workersOnly };
}
