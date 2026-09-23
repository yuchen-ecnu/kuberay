import { readFileSync } from "node:fs";
import { parse } from "yaml";
import type { Federation } from "./model";

export function fixture(): Federation {
  const resource = parse(
    readFileSync(
      new URL(
        "../../../ray-operator/config/samples/ray-federation.yaml",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  resource.metadata.namespace = "ray-federation";
  resource.metadata.uid = "frc-uid";
  resource.metadata.resourceVersion = "1";
  resource.metadata.generation = 2;
  return resource;
}
