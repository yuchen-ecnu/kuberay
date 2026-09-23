import { useEffect, useMemo, useState } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  Panel,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useNodesState,
  useReactFlow,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type {
  ClusterView,
  Federation,
  PodView,
  Snapshot,
} from "@/federation/model";
import {
  buildGraph,
  mergeNodePositions,
  type GraphNode,
} from "@/federation/graph";
import styles from "./federation.module.css";
import { useLocale } from "./Locale";
import { translateDiagnostic } from "@/federation/i18n";

export function StatusDot({
  ready,
  warning = false,
}: {
  ready: boolean;
  warning?: boolean;
}) {
  return (
    <span
      className={`${styles.dot} ${ready ? styles.green : warning ? styles.red : styles.amber}`}
    />
  );
}

function ClusterNode({ data }: NodeProps<GraphNode>) {
  const { locale, t } = useLocale();
  return (
    <div
      className={`${styles.graphCluster} ${data.role === "member" ? styles.memberCluster : ""} ${data.unmapped ? styles.declaredCluster : ""}`}
      data-testid={`cluster-${data.clusterId}`}
    >
      <div className={`${styles.clusterGrip} cluster-grip`}>
        <span className={styles.kubernetesIcon}>⎈</span>
        <div>
          <strong>{data.title}</strong>
          <span>{data.subtitle}</span>
        </div>
        <span className={styles.clusterRole}>
          {data.role === "primary" ? "PRIMARY" : "MEMBER"}
        </span>
      </div>
      {data.error && (
        <span
          className={styles.clusterErrorText}
          title={translateDiagnostic(data.error, locale)}
        >
          {t("API unavailable · Partial observation")}
        </span>
      )}
      <div className={styles.clusterTargets}>
        {data.targets?.map((group) => (
          <span key={group.groupName} data-testid={`target-${group.groupName}`}>
            <span>{group.groupName} </span>
            <b>{group.replicas ?? 0}</b>
            <small>
              {group.initial ? t("Initial replicas") : t("Current target")}
            </small>
          </span>
        ))}
      </div>
    </div>
  );
}

function EntityNode({ data, selected }: NodeProps<GraphNode>) {
  const { locale, t } = useLocale();
  const pod = data.pod;
  const isPod = data.kind === "pod" || data.kind === "planned";
  return (
    <div
      className={`${styles.graphEntity} ${isPod ? styles.graphPod : ""} ${styles[data.kind]} ${data.role === "head" ? styles.graphHead : ""} ${selected ? styles.graphSelected : ""}`}
      data-testid={
        pod
          ? "pod-node"
          : data.kind === "planned"
            ? "planned-node"
            : `graph-${data.kind}`
      }
    >
      {data.kind !== "frc" && (
        <Handle type="target" position={Position.Left} id="in" />
      )}
      {!isPod && <Handle type="source" position={Position.Right} id="out" />}
      <div className={styles.entityTitle}>
        <span className={styles.entityGlyph}>
          {data.kind === "frc"
            ? "F"
            : data.kind === "controller"
              ? "⟳"
              : data.kind === "raycluster"
                ? "R"
                : data.kind === "planned"
                  ? "+"
                  : pod?.role === "head"
                    ? "H"
                    : "W"}
        </span>
        <div>
          <strong title={data.title}>{data.title}</strong>
          <small title={data.subtitle}>{data.subtitle}</small>
        </div>
      </div>
      {pod && (
        <>
          <code className={styles.graphPodName} title={pod.name}>
            {pod.name}
          </code>
          <div className={styles.graphPodStatus}>
            <StatusDot
              ready={pod.ready}
              warning={pod.phase.includes("Error")}
            />
            <span>{pod.phase}</span>
            <code>{pod.ip}</code>
          </div>
        </>
      )}
      {data.kind === "frc" && (
        <div
          className={`${styles.entityStatus} ${data.error ? styles.entityStatusError : ""}`}
          title={
            data.error ? translateDiagnostic(data.error, locale) : undefined
          }
        >
          <StatusDot ready={data.status === "True"} warning={!!data.error} />
          {data.error
            ? t("Reconciliation blocked")
            : data.status === "True"
              ? "Ready"
              : data.status === "False"
                ? "Reconciling"
                : "Unknown"}
        </div>
      )}
      {data.kind === "planned" && (
        <small className={styles.plannedCaption}>
          {t("Pod not yet observed")}
        </small>
      )}
    </div>
  );
}

const nodeTypes = { cluster: ClusterNode, entity: EntityNode };

interface Props {
  snapshot: Snapshot;
  draft: Federation;
  editorOpen: boolean;
  onPod: (cluster: ClusterView, pod: PodView) => void;
  onResource: (id: string) => void;
}

function Canvas({ snapshot, draft, editorOpen, onPod, onResource }: Props) {
  const { locale, t } = useLocale();
  const graph = useMemo(
    () => buildGraph(snapshot, draft, locale),
    [snapshot, draft, locale],
  );
  const [nodes, setNodes, onNodesChange] = useNodesState<GraphNode>(
    graph.nodes,
  );
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const selectedEdge = graph.edges.find((edge) => edge.id === selectedEdgeId);
  const flow = useReactFlow<GraphNode>();
  useEffect(
    () => setNodes((previous) => mergeNodePositions(graph.nodes, previous)),
    [graph.nodes, setNodes],
  );
  useEffect(() => {
    const timer = setTimeout(() => {
      void flow.fitView({ padding: 0.04, duration: 250 });
    }, 80);
    return () => clearTimeout(timer);
  }, [editorOpen, flow]);
  const edges = graph.edges.map((edge) => ({
    ...edge,
    selected: selectedEdge?.id === edge.id,
  }));
  return (
    <div className={styles.graphCanvas} data-testid="topology-canvas">
      <ReactFlow<GraphNode>
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        colorMode="light"
        onNodesChange={onNodesChange}
        nodesConnectable={false}
        edgesReconnectable={false}
        deleteKeyCode={null}
        minZoom={0.2}
        maxZoom={2}
        fitView
        fitViewOptions={{ padding: 0.04 }}
        onNodeClick={(_, node) => {
          const cluster = snapshot.clusters.find(
            (c) => c.id === node.data.clusterId,
          );
          if (node.data.pod && cluster) onPod(cluster, node.data.pod);
          if (node.data.kind === "frc") onResource("frc");
          if (node.data.kind === "raycluster" && cluster?.resource)
            onResource(cluster.id);
        }}
        onEdgeClick={(_, edge) => setSelectedEdgeId(edge.id)}
        onPaneClick={() => setSelectedEdgeId(null)}
        ariaLabelConfig={{
          "controls.zoomIn.ariaLabel": t("Zoom in"),
          "controls.zoomOut.ariaLabel": t("Zoom out"),
          "controls.fitView.ariaLabel": t("Fit view"),
        }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={26}
          size={1}
          color="#dedbd5"
        />
        <Controls showInteractive={false} position="bottom-left" />
        <Panel position="top-left" className={styles.graphTools}>
          <button
            className={styles.resetGraph}
            onClick={() => {
              setNodes(graph.nodes);
              requestAnimationFrame(
                () => void flow.fitView({ padding: 0.04, duration: 300 }),
              );
            }}
          >
            {t("Reset layout")}
          </button>
        </Panel>
        {selectedEdge && (
          <Panel position="bottom-center" className={styles.edgeInfo}>
            <strong>{t("Kubernetes control plane")}</strong>
            <span>
              {String(
                selectedEdge.data?.description ??
                  t("Controllers create and maintain resources from the spec"),
              )}
            </span>
            <button
              onClick={() => setSelectedEdgeId(null)}
              aria-label={t("Close edge details")}
            >
              ×
            </button>
          </Panel>
        )}
      </ReactFlow>
    </div>
  );
}

export default function Topology(props: Props) {
  return (
    <ReactFlowProvider>
      <Canvas {...props} />
    </ReactFlowProvider>
  );
}
