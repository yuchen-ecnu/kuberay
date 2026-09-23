import { MarkerType, type Edge, type Node } from "@xyflow/react";
import { translate, type Locale, type MessageKey } from "./i18n";
import {
  currentCondition,
  targetsForCluster,
  reconciliationIssues,
  type ClusterView,
  type Federation,
  type PodView,
  type Snapshot,
  type WorkerTarget,
} from "./model";

export type GraphData = {
  kind: "cluster" | "frc" | "controller" | "raycluster" | "pod" | "planned";
  title: string;
  subtitle?: string;
  clusterId?: string;
  role?: string;
  status?: string;
  pod?: PodView;
  targets?: WorkerTarget[];
  error?: string;
  unmapped?: boolean;
  layoutPosition: { x: number; y: number };
  podColumnX?: number;
};
export type GraphNode = Node<GraphData>;

// Shared columns keep every RayCluster and its Pod branches aligned across kind clusters.
const layout = {
  width: 940,
  memberX: 430,
  resourceX: 450,
  podX: 690,
  bodyY: 112,
  resourceHeight: 108,
  podHeight: 84,
  podGap: 12,
  clusterGap: 48,
};
const stackHeight = (count: number) =>
  count ? count * layout.podHeight + (count - 1) * layout.podGap : 0;

export function buildGraph(
  snapshot: Snapshot,
  draft: Federation,
  locale: Locale = "en",
) {
  const t = (key: MessageKey) => translate(locale, key);
  const nodes: GraphNode[] = [];
  const edges: Edge[] = [];
  const issues = reconciliationIssues(snapshot.federation);
  const trackedMembers = new Set([
    ...draft.spec.memberClusters.map((member) => member.name),
    ...(snapshot.federation?.spec.memberClusters ?? []).map(
      (member) => member.name,
    ),
    ...(Array.isArray(snapshot.federation?.status?.memberClusterStatuses)
      ? snapshot.federation.status.memberClusterStatuses.map(
          (member: { name: string }) => member.name,
        )
      : []),
  ]);
  const observedClusters = snapshot.clusters.filter(
    (cluster) =>
      cluster.role === "primary" ||
      (!!cluster.memberName && trackedMembers.has(cluster.memberName)),
  );
  const observedMembers = new Set(
    observedClusters.map((cluster) => cluster.memberName).filter(Boolean),
  );
  const declaredOnly: ClusterView[] = draft.spec.memberClusters
    .filter((member) => !observedMembers.has(member.name))
    .map((member) => ({
      id: `declared:${member.name}`,
      label: member.name,
      role: "member",
      memberName: member.name,
      namespace: member.namespace,
      resource: null,
      pods: [],
    }));
  const clusters = [...observedClusters, ...declaredOnly].sort(
    (a, b) => Number(b.role === "primary") - Number(a.role === "primary"),
  );
  const dirty =
    JSON.stringify(snapshot.federation?.spec) !== JSON.stringify(draft.spec);
  const primary =
    snapshot.clusters.find((cluster) => cluster.role === "primary")?.resource ??
    null;
  const edge = (source: string, target: string, description: string) => {
    const color = "#908da0";
    edges.push({
      id: `control:${source}:${target}`,
      source,
      target,
      sourceHandle: "out",
      targetHandle: "in",
      type: "smoothstep",
      data: { description },
      ariaLabel: description,
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 14, height: 14 },
      style: {
        stroke: color,
        strokeWidth: 1.4,
      },
      interactionWidth: 18,
      zIndex: 1,
    });
  };
  let clusterY = 0;
  for (const cluster of clusters) {
    const unmapped = cluster.id.startsWith("declared:");
    const parentId = `cluster:${cluster.id}`;
    const resourceId = `rc:${cluster.id}`;
    const groups = targetsForCluster(draft, cluster, primary);
    const member = draft.spec.memberClusters.find(
      (m) => m.name === cluster.memberName,
    );
    const pods = [...cluster.pods].sort(
      (a, b) =>
        a.role.localeCompare(b.role) ||
        a.group.localeCompare(b.group) ||
        a.name.localeCompare(b.name),
    );
    const pending = groups.flatMap((group) => {
      const actual = cluster.pods.filter(
        (pod) =>
          pod.role === "worker" &&
          pod.group === group.groupName &&
          pod.phase !== "Terminating",
      ).length;
      return Array.from(
        { length: Math.min(8, Math.max(0, (group.replicas ?? 0) - actual)) },
        (_, i) => ({
          group: group.groupName,
          index: i,
          initial: group.initial,
        }),
      );
    });
    const clusterX = cluster.role === "primary" ? 0 : layout.memberX;
    const bodyHeight = Math.max(
      layout.resourceHeight,
      stackHeight(pods.length + pending.length),
    );
    const resourceY = layout.bodyY + (bodyHeight - layout.resourceHeight) / 2;
    const height = layout.bodyY + bodyHeight + 40;
    const clusterPosition = { x: clusterX, y: clusterY };
    nodes.push({
      id: parentId,
      type: "cluster",
      position: clusterPosition,
      style: { width: layout.width - clusterX, height },
      dragHandle: ".cluster-grip",
      selectable: false,
      data: {
        kind: "cluster",
        layoutPosition: { ...clusterPosition },
        podColumnX: layout.podX - clusterX,
        title: cluster.label,
        subtitle: unmapped
          ? `${cluster.namespace} · ${t("Not mapped to a demo kind cluster")}`
          : cluster.namespace,
        role: cluster.role,
        clusterId: cluster.id,
        targets: groups,
        unmapped,
        status:
          member && !member.kubeconfigSecretRef
            ? t("Manual member")
            : t("Managed"),
        error:
          cluster.error ??
          issues.find((issue) => issue.scope === cluster.memberName)?.message,
      },
    });
    const add = (
      id: string,
      x: number,
      y: number,
      data: Omit<GraphData, "layoutPosition">,
      width = 180,
      height = layout.resourceHeight,
    ) => {
      nodes.push({
        id,
        type: "entity",
        parentId,
        extent: "parent",
        position: { x, y },
        style: { width, height },
        data: { ...data, layoutPosition: { x, y } },
        zIndex: 2,
      });
    };
    if (cluster.role === "primary") {
      add(
        "frc",
        20,
        resourceY,
        {
          kind: "frc",
          title: draft.metadata.name,
          subtitle: "FederatedRayCluster",
          status: snapshot.federation
            ? currentCondition(snapshot.federation, "Ready")
            : "Unknown",
          error: issues[0]?.message,
        },
        150,
      );
      add("controller", 215, resourceY, {
        kind: "controller",
        title: "Federation controller",
        subtitle: t("Distribute desired state by groupName"),
      });
      edge("frc", "controller", "reconcile");
    }
    add(resourceId, layout.resourceX - clusterX, resourceY, {
      kind: "raycluster",
      title:
        cluster.role === "primary" ? "Primary RayCluster" : "Member RayCluster",
      subtitle: cluster.resource
        ? cluster.resource.spec.headGroupSpec
          ? "head + workers"
          : t("Workers only · No head")
        : t("Pending creation"),
      clusterId: cluster.id,
      role: cluster.role,
    });
    if (cluster.role === "primary" || member?.kubeconfigSecretRef)
      edge(
        "controller",
        resourceId,
        cluster.role === "primary"
          ? t("Create PRC")
          : t("Member API · Create MRC"),
      );
    const addPod = (pod: PodView, y: number) => {
      const id = `pod:${cluster.id}:${pod.uid}`;
      add(
        id,
        layout.podX - clusterX,
        y,
        {
          kind: "pod",
          title: pod.role === "head" ? "Ray head" : "Ray worker",
          subtitle: pod.group,
          pod,
          clusterId: cluster.id,
          role: pod.role,
        },
        225,
        layout.podHeight,
      );
      edge(resourceId, id, t("Local operator"));
    };
    for (const [i, pod] of pods.entries()) {
      addPod(pod, layout.bodyY + i * (layout.podHeight + layout.podGap));
    }
    for (const [i, item] of pending.entries()) {
      const position = pods.length + i;
      const id = `planned:${cluster.id}:${item.group}:${item.index}`;
      add(
        id,
        layout.podX - clusterX,
        layout.bodyY + position * (layout.podHeight + layout.podGap),
        {
          kind: "planned",
          title:
            dirty && item.initial
              ? t("New worker in draft")
              : t("Pending worker"),
          subtitle: item.group,
          clusterId: cluster.id,
        },
        225,
        layout.podHeight,
      );
      edge(resourceId, id, t("Local operator"));
    }
    clusterY += height + layout.clusterGap;
  }
  return { nodes, edges };
}

// Untouched nodes follow layout changes; only presenter-dragged nodes retain their position.
export function mergeNodePositions(next: GraphNode[], previous: GraphNode[]) {
  const old = new Map(previous.map((node) => [node.id, node]));
  return next.map((node) => {
    const prior = old.get(node.id);
    if (!prior || prior.parentId !== node.parentId) return node;
    const moved =
      prior.position.x !== prior.data.layoutPosition.x ||
      prior.position.y !== prior.data.layoutPosition.y;
    return {
      ...node,
      position: moved ? prior.position : node.position,
      selected: prior.selected,
    };
  });
}
