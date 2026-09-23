import assert from "node:assert/strict";
import { test } from "node:test";
import { yamlFolds } from "./yaml-folding";

test("finds nested map and sequence folds without folding scalar values", () => {
  const source = `apiVersion: ray.io/v1
kind: FederatedRayCluster
metadata:
  name: demo
spec:
  memberClusters:
    - name: member-a
      workerGroups:
        - groupName: gpu
          replicas: 2
  networking: {headEndpoint: {address: head.example:6379}}
`;
  const lines = source.split("\n");
  assert.deepEqual(
    yamlFolds(source).map(({ startLine, endLine }) => ({
      start: lines[startLine].trim(),
      end: lines[endLine].trim(),
    })),
    [
      { start: "metadata:", end: "name: demo" },
      {
        start: "spec:",
        end: "networking: {headEndpoint: {address: head.example:6379}}",
      },
      { start: "memberClusters:", end: "replicas: 2" },
      { start: "- name: member-a", end: "replicas: 2" },
      { start: "workerGroups:", end: "replicas: 2" },
      { start: "- groupName: gpu", end: "replicas: 2" },
    ],
  );
});

test("returns no folds for invalid YAML and comments", () => {
  assert.deepEqual(yamlFolds("apiVersion: [broken"), []);
  assert.deepEqual(yamlFolds("# waiting for a resource"), []);
});
