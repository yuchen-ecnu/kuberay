import assert from "node:assert/strict";
import { test } from "node:test";
import { resourceHighlights } from "./yaml-highlights";

test("highlights worker managedBy by path, ignoring matching keys and YAML text inside templates", () => {
  const source = `apiVersion: ray.io/v1
kind: RayCluster
metadata:
  annotations:
    managedBy: unrelated
spec:
  managedBy: ray.io/kuberay-operator
  headGroupSpec:
    rayStartParams:
      address: auto
  enableInTreeAutoscaling: false
  workerGroupSpecs:
    - groupName: local
      template:
        metadata:
          annotations:
            example: |
              managedBy: ray.io/federated-raycluster-controller
        spec:
          address: not-a-Ray-setting
    - groupName: remote-a
      managedBy: ray.io/federated-raycluster-controller
      replicas: 2
    - groupName: remote-b
      "managedBy": ray.io/federated-raycluster-controller
`;
  const result = resourceHighlights(source);
  assert.equal(result.workersOnly, false);
  assert.deepEqual(
    result.fields.map((field) => ({
      field: field.field,
      text: source
        .split("\n")
        .slice(field.startLine, field.endLine + 1)
        .map((line) => line.trim())
        .join("\n"),
    })),
    [
      {
        field: "managedBy",
        text: "managedBy: ray.io/federated-raycluster-controller",
      },
      {
        field: "managedBy",
        text: '"managedBy": ray.io/federated-raycluster-controller',
      },
      {
        field: "enableInTreeAutoscaling",
        text: "enableInTreeAutoscaling: false",
      },
    ],
  );
});

test("workers-only highlights existing address settings for every group and records the omitted head", () => {
  const source = `apiVersion: ray.io/v1
kind: RayCluster
spec:
  enableInTreeAutoscaling: false
  workerGroupSpecs:
    - groupName: cpu
      rayStartParams:
        "address": head.example:6379
        num-cpus: "1"
    - groupName: gpu
      rayStartParams: {address: "head.example:6379"}
`;
  const result = resourceHighlights(source);
  assert.equal(result.workersOnly, true);
  assert.deepEqual(
    result.fields.map((field) => field.field),
    [
      "rayStartParams.address",
      "rayStartParams.address",
      "enableInTreeAutoscaling",
    ],
  );
  assert.equal(result.fields[0].startLine, result.fields[0].endLine);
  assert.match(
    source.split("\n")[result.fields[0].startLine],
    /"address": head.example:6379/,
  );
});

test("FRC highlights autoscaler settings under primaryCluster only", () => {
  const source = `apiVersion: ray.io/v1
kind: FederatedRayCluster
spec:
  enableInTreeAutoscaling: true
  autoscalerOptions: {version: v1}
  primaryCluster:
    headGroupSpec: {}
    enableInTreeAutoscaling: false
    autoscalerOptions:
      version: v2
`;
  const result = resourceHighlights(source);
  assert.equal(result.workersOnly, false);
  assert.deepEqual(
    result.fields.map((field) => ({
      field: field.field,
      text: source
        .split("\n")
        .slice(field.startLine, field.endLine + 1)
        .map((line) => line.trim())
        .join("\n"),
    })),
    [
      {
        field: "primaryCluster.enableInTreeAutoscaling",
        text: "enableInTreeAutoscaling: false",
      },
      {
        field: "primaryCluster.autoscalerOptions",
        text: "autoscalerOptions:\nversion: v2",
      },
    ],
  );
});

test("invalid YAML, other resources and ordinary head settings do not receive design highlights", () => {
  for (const source of [
    "apiVersion: [broken",
    "# RayCluster not yet created",
    "kind: FederatedRayCluster\nspec: {managedBy: ray.io/federated-raycluster-controller}",
    "apiVersion: example.io/v1\nkind: RayCluster\nspec: {}",
    "apiVersion: ray.io/v1\nkind: RayCluster\nspec: {headGroupSpec: {}, enableInTreeAutoscaling: true}",
  ])
    assert.deepEqual(resourceHighlights(source), {
      fields: [],
      workersOnly: false,
    });
});
