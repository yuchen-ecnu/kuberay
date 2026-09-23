import { parseDocument, stringify } from "yaml";

export interface Condition {
  type: string;
  status: string;
  message?: string;
  reason?: string;
  observedGeneration?: number;
}

export interface ReconciliationIssue {
  scope: string;
  type: string;
  message: string;
}

export interface WorkerGroup {
  groupName: string;
  replicas?: number;
  minReplicas?: number;
  maxReplicas?: number;
  managedBy?:
    | "ray.io/raycluster-controller"
    | "ray.io/federated-raycluster-controller";
  [key: string]: unknown;
}

export interface WorkerTarget {
  groupName: string;
  replicas: number;
  initial: boolean;
}

export interface Member {
  name: string;
  namespace: string;
  kubeconfigSecretRef?: { name: string };
  workerGroups?: WorkerGroup[];
}

export interface FederationSpec {
  primaryCluster: {
    enableInTreeAutoscaling?: boolean;
    autoscalerOptions?: Record<string, unknown>;
    headGroupSpec: Record<string, unknown>;
    workerGroups?: WorkerGroup[];
    rayVersion?: string;
  };
  memberClusters: Member[];
  networking: {
    headEndpoint: { address: string; gcsPort?: number; mode?: string };
  };
  memberCleanupPolicy?: string;
}

export interface Resource {
  apiVersion: string;
  kind: string;
  metadata: {
    name: string;
    namespace: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    [key: string]: unknown;
  };
  // Preserve the complete Kubernetes spec, including user Pod templates.
  spec: Record<string, any>;
  status?: {
    conditions?: Condition[];
    [key: string]: any;
  };
}

export interface Federation extends Resource {
  spec: FederationSpec;
}

export interface PodView {
  name: string;
  uid: string;
  role: "head" | "worker";
  group: string;
  phase: string;
  ready: boolean;
  reason?: string;
  node: string;
  ip: string;
  restarts: number;
  createdAt: string;
  containers: {
    name: string;
    image: string;
    requests: Record<string, string>;
    limits: Record<string, string>;
  }[];
}

export interface ClusterView {
  id: string;
  label: string;
  role: "primary" | "member";
  memberName?: string;
  namespace: string;
  resource: Resource | null;
  revision?: string | null;
  pods: PodView[];
  error?: string;
}

export interface Snapshot {
  updatedAt: string;
  federation: Federation | null;
  revision: string | null;
  clusters: ClusterView[];
  error?: string;
}

export const MAX_YAML_BYTES = 200 * 1024;

export type YamlResource = Record<string, any>;

function isObject(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function manifest(resource: Resource) {
  const metadata = { ...resource.metadata };
  for (const field of [
    "uid",
    "resourceVersion",
    "generation",
    "creationTimestamp",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "managedFields",
    "selfLink",
  ])
    delete metadata[field];
  return {
    apiVersion: resource.apiVersion,
    kind: resource.kind,
    metadata,
    spec: resource.spec,
  };
}

export function toYaml(resource: Resource) {
  return stringify(manifest(resource), { lineWidth: 0 });
}

// Parse only the transport format here. Kubernetes admission (the CRD schema,
// CEL rules and operator webhooks) is the authority for resource semantics.
export function parseResource(source: string): YamlResource {
  if (new TextEncoder().encode(source).length > MAX_YAML_BYTES) {
    throw new Error("YAML must not exceed 200 KiB.");
  }
  const document = parseDocument(source, { uniqueKeys: true });
  if (document.errors.length) throw new Error(document.errors[0].message);
  const value = document.toJS({ maxAliasCount: 50 });
  // Kubernetes resources must be JSON-compatible; YAML aliases may otherwise
  // create a cycle that would crash topology rendering or request serialization.
  try {
    JSON.stringify(value, (_, item) => {
      if (typeof item === "number" && !Number.isFinite(item)) throw new Error();
      return item;
    });
  } catch {
    throw new Error(
      "YAML contains a cycle or a value that JSON cannot represent.",
    );
  }
  if (!isObject(value)) throw new Error("The YAML document must be an object.");
  return value;
}

// A malformed spec must still reach Kubernetes validation. This guard only
// decides whether the draft is structurally safe to use for topology preview.
export function federationPreview(value: unknown): Federation | null {
  if (!isObject(value) || !isObject(value.metadata) || !isObject(value.spec))
    return null;
  const primary = value.spec.primaryCluster;
  const members = value.spec.memberClusters;
  const groupsArePreviewable = (groups: unknown) =>
    groups === undefined ||
    (Array.isArray(groups) &&
      groups.every(
        (group) =>
          isObject(group) &&
          typeof group.groupName === "string" &&
          (group.replicas === undefined ||
            (typeof group.replicas === "number" &&
              Number.isFinite(group.replicas))),
      ));
  if (
    value.apiVersion !== "ray.io/v1" ||
    value.kind !== "FederatedRayCluster" ||
    typeof value.metadata.name !== "string" ||
    !isObject(primary) ||
    !groupsArePreviewable(primary.workerGroups) ||
    !Array.isArray(members) ||
    !members.every(
      (member) =>
        isObject(member) &&
        typeof member.name === "string" &&
        groupsArePreviewable(member.workerGroups),
    )
  )
    return null;
  return value as Federation;
}

export function rayClusterPreview(value: unknown): Resource | null {
  if (!isObject(value) || !isObject(value.metadata) || !isObject(value.spec))
    return null;
  const groups = value.spec.workerGroupSpecs;
  if (
    value.apiVersion !== "ray.io/v1" ||
    value.kind !== "RayCluster" ||
    typeof value.metadata.name !== "string" ||
    (groups !== undefined &&
      (!Array.isArray(groups) ||
        !groups.every(
          (group) =>
            isObject(group) &&
            typeof group.groupName === "string" &&
            (group.replicas === undefined ||
              (typeof group.replicas === "number" &&
                Number.isFinite(group.replicas))),
        )))
  )
    return null;
  return value as Resource;
}

export function groupsForCluster(federation: Federation, cluster: ClusterView) {
  if (cluster.role === "primary") {
    return federation.spec.primaryCluster.workerGroups ?? [];
  }
  return (
    federation.spec.memberClusters.find((m) => m.name === cluster.memberName)
      ?.workerGroups ?? []
  );
}

export function initialWorkers(federation: Federation) {
  return [
    ...(federation.spec.primaryCluster.workerGroups ?? []),
    ...federation.spec.memberClusters.flatMap((m) => m.workerGroups ?? []),
  ].reduce((sum, group) => sum + (group.replicas ?? 0), 0);
}

// Existing managed groups always use PRC runtime targets. FRC replicas seed
// newly introduced groups, and manual members use their own RayCluster spec.
export function targetsForCluster(
  federation: Federation,
  cluster: ClusterView,
  primary: Resource | null = null,
): WorkerTarget[] {
  const live: WorkerGroup[] = cluster.resource?.spec.workerGroupSpecs ?? [];
  const primaryGroups: WorkerGroup[] = primary?.spec.workerGroupSpecs ?? [];
  const manual =
    cluster.role === "member" &&
    federation.spec.memberClusters.some(
      (member) =>
        member.name === cluster.memberName && !member.kubeconfigSecretRef,
    );
  const groups = manual ? live : groupsForCluster(federation, cluster);
  return groups.map((initial) => {
    const delegated = cluster.role === "member";
    const current = manual
      ? live.find((group) => group.groupName === initial.groupName)
      : primaryGroups.find(
          (group) =>
            group.groupName === initial.groupName &&
            (group.managedBy === "ray.io/federated-raycluster-controller") ===
              delegated,
        ) ||
        (cluster.role === "primary"
          ? live.find((group) => group.groupName === initial.groupName)
          : undefined);
    const group = current || initial;
    const replicas = group.suspend
      ? 0
      : Math.min(
          group.maxReplicas ?? 2147483647,
          Math.max(group.minReplicas ?? 0, group.replicas ?? 0),
        );
    return {
      groupName: group.groupName,
      replicas,
      initial: !current,
    };
  });
}

export function currentCondition(resource: Resource, type: string) {
  const condition = resource.status?.conditions?.find((c) => c.type === type);
  if (
    condition?.observedGeneration !== undefined &&
    condition.observedGeneration !== resource.metadata.generation
  ) {
    return "Unknown";
  }
  return condition?.status ?? "Unknown";
}

// Conditions such as `Ready=False, message=Ready` summarize progress without
// explaining it. Prefer current, specific controller messages for the demo.
export function reconciliationIssues(
  resource: Federation | null | undefined,
): ReconciliationIssue[] {
  if (!resource) return [];
  const generation = resource.metadata.generation;
  const issues: ReconciliationIssue[] = [];
  const add = (scope: string, conditions: unknown) => {
    if (!Array.isArray(conditions)) return;
    for (const condition of conditions as Condition[]) {
      if (
        condition.status !== "False" ||
        condition.reason === "ReconcilingTopology" ||
        !condition.message ||
        condition.message === condition.type ||
        (condition.observedGeneration !== undefined &&
          generation !== undefined &&
          condition.observedGeneration !== generation)
      )
        continue;
      issues.push({ scope, type: condition.type, message: condition.message });
      break;
    }
  };
  const members = resource.status?.memberClusterStatuses;
  if (Array.isArray(members)) {
    for (const member of members)
      add(String(member.name ?? "member"), member.conditions);
  }
  add("FederatedRayCluster", resource.status?.conditions);
  return issues.filter(
    (issue, index) =>
      issues.findIndex(
        (candidate) =>
          candidate.scope === issue.scope &&
          candidate.message === issue.message,
      ) === index,
  );
}

export function summarizePod(pod: Resource): PodView {
  const labels = pod.metadata.labels ?? {};
  const statuses = pod.status?.containerStatuses ?? [];
  const waiting = statuses.find((s: any) => s.state?.waiting)?.state.waiting;
  return {
    name: pod.metadata.name,
    uid: pod.metadata.uid ?? pod.metadata.name,
    role: labels["ray.io/node-type"] === "head" ? "head" : "worker",
    group: labels["ray.io/group"] ?? "",
    phase: pod.metadata.deletionTimestamp
      ? "Terminating"
      : (waiting?.reason ?? pod.status?.phase ?? "Unknown"),
    ready:
      !pod.metadata.deletionTimestamp &&
      pod.status?.conditions?.some(
        (c) => c.type === "Ready" && c.status === "True",
      ) === true,
    reason: waiting?.message ?? pod.status?.message,
    node: pod.spec.nodeName ?? "Unscheduled",
    ip: pod.status?.podIP ?? "—",
    restarts: statuses.reduce((sum: number, s: any) => sum + s.restartCount, 0),
    createdAt: String(pod.metadata.creationTimestamp ?? ""),
    containers: (pod.spec.containers ?? []).map((c: any) => ({
      name: c.name,
      image: c.image,
      requests: c.resources?.requests ?? {},
      limits: c.resources?.limits ?? {},
    })),
  };
}
