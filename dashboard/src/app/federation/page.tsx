"use client";

import { useEffect, useMemo, useState } from "react";
import useSWR from "swr";
import { parseDocument } from "yaml";
import Topology, { StatusDot } from "@/components/Federation/Topology";
import YamlEditor from "@/components/Federation/YamlEditor";
import KubectlTerminal from "@/components/Federation/KubectlTerminal";
import { LocaleProvider, useLocale } from "@/components/Federation/Locale";
import { translateDiagnostic } from "@/federation/i18n";
import PodDetails, {
  type PodSelection,
} from "@/components/Federation/PodDetails";
import {
  currentCondition,
  federationPreview,
  parseResource,
  rayClusterPreview,
  reconciliationIssues,
  toYaml,
  type Resource,
  type Snapshot,
} from "@/federation/model";
import styles from "@/components/Federation/federation.module.css";

async function fetchSnapshot(url: string): Promise<Snapshot> {
  const response = await fetch(url, { cache: "no-store" });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error);
  return data;
}

export default function FederationPage() {
  return (
    <LocaleProvider>
      <FederationDemo />
    </LocaleProvider>
  );
}

function FederationDemo() {
  const { locale, t } = useLocale();
  const [basePath, setBasePath] = useState<string | null>(null);
  const apiPath = basePath === null ? null : `${basePath}/api/federation`;
  useEffect(() => {
    setBasePath(window.location.pathname.replace(/\/federation\/?$/, ""));
  }, []);
  const { data, error, mutate } = useSWR<Snapshot>(apiPath, fetchSnapshot, {
    refreshInterval: 5000,
    revalidateOnFocus: true,
  });
  const [source, setSource] = useState<string | null>(null);
  const [base, setBase] = useState({ yaml: "", revision: "" });
  const [primarySource, setPrimarySource] = useState<string | null>(null);
  const [primaryBase, setPrimaryBase] = useState({
    yaml: "",
    revision: "",
  });
  const [tab, setTab] = useState("frc");
  const [preview, setPreview] = useState(false);
  const [terminalOpen, setTerminalOpen] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<{
    text: string;
    error?: boolean;
  } | null>(null);
  const [selected, setSelected] = useState<PodSelection | null>(null);
  const primaryCluster = data?.clusters.find(
    (cluster) => cluster.role === "primary",
  );
  const editingFrc = tab === "frc";
  const editingPrimary = tab === primaryCluster?.id;
  const editable = editingFrc || editingPrimary;
  const activeSource = editingFrc
    ? source
    : editingPrimary
      ? primarySource
      : null;
  const activeBase = editingFrc ? base : primaryBase;
  const dirty =
    editable && activeSource !== null && activeSource !== activeBase.yaml;
  const anyDirty =
    (source !== null && source !== base.yaml) ||
    (primarySource !== null && primarySource !== primaryBase.yaml);

  useEffect(() => {
    if (source === null && data?.federation && data.revision) {
      const yaml = toYaml(data.federation);
      setSource(yaml);
      setBase({ yaml, revision: data.revision });
    }
  }, [source, data]);
  useEffect(() => {
    if (
      primarySource === null &&
      primaryCluster?.resource &&
      primaryCluster.revision
    ) {
      const yaml = toYaml(primaryCluster.resource);
      setPrimarySource(yaml);
      setPrimaryBase({ yaml, revision: primaryCluster.revision });
    }
  }, [primarySource, primaryCluster]);
  useEffect(() => {
    const warn = (event: BeforeUnloadEvent) => {
      if (anyDirty) {
        event.preventDefault();
        event.returnValue = "";
      }
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [anyDirty]);

  const parsed = useMemo(() => {
    if (source === null) return { value: null, error: null };
    try {
      return {
        value: federationPreview(parseResource(source)),
        error: null,
      };
    } catch (issue) {
      return { value: null, error: (issue as Error).message };
    }
  }, [source]);
  const primaryParsed = useMemo(() => {
    if (primarySource === null) return { value: null, error: null };
    try {
      return {
        value: rayClusterPreview(parseResource(primarySource)),
        error: null,
      };
    } catch (issue) {
      return { value: null, error: (issue as Error).message };
    }
  }, [primarySource]);
  const live = data?.federation;
  const draft = parsed.value ?? live;
  const activeParsed = editingPrimary ? primaryParsed : parsed;
  const previewUnavailable =
    editable &&
    activeSource !== null &&
    activeParsed.error === null &&
    activeParsed.value === null;
  const stale = !!error || !!data?.error || data?.clusters.some((c) => c.error);
  const changedElsewhere = editingFrc
    ? !!(data?.revision && base.revision && data.revision !== base.revision)
    : editingPrimary
      ? !!(
          primaryCluster?.revision &&
          primaryBase.revision &&
          primaryCluster.revision !== primaryBase.revision
        )
      : false;
  const pods = data?.clusters.flatMap((cluster) => cluster.pods) ?? [];
  const ready = live ? currentCondition(live, "Ready") : "Unknown";
  const controllerIssues = reconciliationIssues(live);
  const viewed = data?.clusters.find((cluster) => cluster.id === tab)?.resource;
  const topologyData = useMemo<Snapshot | undefined>(() => {
    if (!data || !primaryParsed.value) return data;
    return {
      ...data,
      clusters: data.clusters.map((cluster) =>
        cluster.role === "primary"
          ? { ...cluster, resource: primaryParsed.value as Resource }
          : cluster,
      ),
    };
  }, [data, primaryParsed.value]);
  const displayedSource = editingFrc
    ? (source ?? "")
    : editingPrimary
      ? (primarySource ?? `# ${t("RayCluster has not been created")}`)
      : viewed
        ? toYaml(viewed)
        : `# ${t("RayCluster has not been created")}`;

  function loadLatest() {
    if (editingFrc && live && data?.revision) {
      const yaml = toYaml(live);
      setSource(yaml);
      setBase({ yaml, revision: data.revision });
    } else if (
      editingPrimary &&
      primaryCluster?.resource &&
      primaryCluster.revision
    ) {
      const yaml = toYaml(primaryCluster.resource);
      setPrimarySource(yaml);
      setPrimaryBase({ yaml, revision: primaryCluster.revision });
    } else {
      return;
    }
    setNotice(null);
  }
  async function submit(action: "validate" | "apply") {
    if (!activeSource || !editable) return;
    const submitted = activeSource;
    const target = editingPrimary ? "primary" : "frc";
    setBusy(action);
    setNotice(null);
    try {
      const response = await fetch(apiPath!, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action,
          target,
          yaml: submitted,
          revision: activeBase.revision,
        }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error);
      setNotice({ text: result.message });
      if (action === "apply") {
        const yaml = toYaml(result.resource);
        if (target === "primary") {
          setPrimarySource((current) =>
            current === submitted ? yaml : current,
          );
          setPrimaryBase({ yaml, revision: result.revision });
        } else {
          setSource((current) => (current === submitted ? yaml : current));
          setBase({ yaml, revision: result.revision });
        }
        await mutate();
      }
    } catch (issue) {
      setNotice({ text: (issue as Error).message, error: true });
    } finally {
      setBusy(null);
    }
  }
  function selectTab(next: string) {
    setTab(next);
    setPreview(false);
    setNotice(null);
  }
  function download() {
    const url = URL.createObjectURL(
      new Blob([displayedSource], { type: "application/yaml" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = `${tab === "frc" ? "federated-raycluster" : tab}.yaml`;
    link.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className={styles.app} lang="en">
      <header className={styles.header}>
        <div className={styles.demoTitle}>
          <span className={styles.brandMark}>R</span>
          <strong>Federation Lab</strong>
        </div>
        <div className={styles.headerActions}>
          {live && (
            <span className={styles.connection}>
              <StatusDot ready={ready === "True" && !stale} warning={!!stale} />
              {stale
                ? t("Observation error")
                : ready === "True"
                  ? "Ready"
                  : "Reconciling"}
              <span data-testid="ready-pods">
                {pods.filter((p) => p.ready).length}/{pods.length}
              </span>
            </span>
          )}
          <button
            className={styles.textButton}
            onClick={() => void mutate()}
            aria-label={t("Refresh")}
            title={t("Refresh cluster observations")}
          >
            ↻
          </button>
          <button
            className={styles.terminalButton}
            aria-pressed={terminalOpen}
            onClick={() => setTerminalOpen(!terminalOpen)}
          >
            <span aria-hidden="true">›_</span>
            {t("Terminal")}
          </button>
          <a
            className={styles.dashboardLink}
            href={`${basePath ?? "."}/ray-dashboard/`}
            target="_blank"
            rel="noreferrer"
          >
            Dashboard <span aria-hidden="true">↗</span>
          </a>
        </div>
      </header>
      {(error || data?.error) && (
        <div className={styles.connectionError} role="alert">
          {translateDiagnostic(error?.message ?? data?.error ?? "", locale)} ·{" "}
          {t("Showing the last observed topology")}
        </div>
      )}
      <main className={styles.demoBody}>
        <section className={styles.editorPanel}>
          <div className={styles.editorHeading}>
            <div>
              <h2>YAML</h2>
              <span className={dirty ? styles.draftBadge : styles.liveBadge}>
                {dirty ? t("Draft · Not applied") : t("Cluster configuration")}
              </span>
            </div>
            <button
              className={styles.iconButton}
              onClick={download}
              aria-label={t("Download YAML")}
              title={t("Download YAML")}
            >
              ↓
            </button>
          </div>
          <div
            className={styles.tabs}
            role="tablist"
            aria-label={t("Resource YAML")}
          >
            <button
              role="tab"
              aria-selected={tab === "frc"}
              onClick={() => selectTab("frc")}
            >
              FRC <span aria-hidden="true">{t("Edit")}</span>
            </button>
            {data?.clusters.map((cluster) => (
              <button
                key={cluster.id}
                role="tab"
                aria-selected={tab === cluster.id}
                onClick={() => selectTab(cluster.id)}
              >
                {cluster.role === "primary"
                  ? "PRC"
                  : `MRC · ${cluster.memberName}`}
                {cluster.role === "primary" && (
                  <span aria-hidden="true">{t("Edit")}</span>
                )}
              </button>
            ))}
          </div>
          <div className={styles.editorTools}>
            <span>
              <StatusDot
                ready={!activeParsed.error && !previewUnavailable}
                warning={!!activeParsed.error || previewUnavailable}
              />
              {editingFrc
                ? t("FederatedRayCluster YAML editor")
                : editingPrimary
                  ? t("Primary RayCluster YAML editor")
                  : t("Controller-generated · Read only")}
            </span>
            <div>
              {editable && (
                <>
                  <button
                    onClick={() => {
                      try {
                        const formatted = parseDocument(
                          activeSource ?? "",
                        ).toString({ lineWidth: 0 });
                        if (editingPrimary) setPrimarySource(formatted);
                        else setSource(formatted);
                      } catch {
                        /* Inline diagnostic. */
                      }
                    }}
                    disabled={!!activeParsed.error}
                  >
                    {t("Format")}
                  </button>
                  <button onClick={() => setPreview(!preview)}>
                    {preview ? t("Edit") : t("Preview")}
                  </button>
                </>
              )}
            </div>
          </div>
          <YamlEditor
            source={displayedSource}
            readOnly={!editable || preview}
            label={
              editingFrc
                ? t("FederatedRayCluster YAML editor")
                : editingPrimary
                  ? t("Primary RayCluster YAML editor")
                  : t("RayCluster YAML preview")
            }
            onChange={(value) => {
              if (editingPrimary) setPrimarySource(value);
              else if (editingFrc) setSource(value);
              setNotice(null);
            }}
          />
          <div className={styles.editorFooter}>
            {tab === "frc" && (
              <small>
                {draft?.spec.primaryCluster.enableInTreeAutoscaling
                  ? t(
                      "FRC replicas seed new groups. Change FRC bounds to guide autoscaling; Ray writes runtime targets to the PRC.",
                    )
                  : t(
                      "FRC replicas seed new groups. Edit the PRC to scale existing groups.",
                    )}
              </small>
            )}
            {editingPrimary && (
              <small>
                {draft?.spec.primaryCluster.enableInTreeAutoscaling
                  ? t(
                      "Ray autoscaler writes runtime targets to the PRC. Manual edits may be replaced by its next decision.",
                    )
                  : t(
                      "Edit PRC replicas or scaleStrategy to scale existing groups. The federation controller syncs managed members.",
                    )}
              </small>
            )}
            {editable && activeParsed.error && (
              <div className={styles.error} role="alert">
                {translateDiagnostic(activeParsed.error, locale)}
                <small>
                  {t("The topology shows the current cluster configuration.")}
                </small>
              </div>
            )}
            {previewUnavailable && (
              <div className={styles.warning} role="status">
                {t(
                  "This draft cannot produce a topology preview, but it can still be submitted to Kubernetes / Operator validation.",
                )}
              </div>
            )}
            {changedElsewhere && (
              <div className={styles.warning}>
                {t(
                  "The spec changed. Your draft is preserved; merge the latest configuration.",
                )}
              </div>
            )}
            {notice && (
              <div
                className={notice.error ? styles.error : styles.success}
                role="status"
              >
                {translateDiagnostic(notice.text, locale)}
              </div>
            )}
            <div className={styles.editorActions}>
              <button
                className={styles.textButton}
                onClick={loadLatest}
                disabled={!editable || !!busy}
              >
                {t("Reload latest")}
              </button>
              <div>
                <button
                  className={styles.secondaryButton}
                  onClick={() => void submit("validate")}
                  disabled={
                    !live ||
                    !editable ||
                    !activeSource ||
                    !!busy ||
                    !!activeParsed.error
                  }
                >
                  {busy === "validate"
                    ? t("Validating…")
                    : t("Kubernetes / Operator validation")}
                </button>
                <button
                  className={styles.primaryButton}
                  onClick={() => void submit("apply")}
                  disabled={!dirty || !!busy || !!activeParsed.error}
                >
                  {busy === "apply" ? t("Applying…") : t("Apply to kind")}
                </button>
              </div>
            </div>
          </div>
        </section>
        <section className={styles.canvasArea}>
          <div className={styles.graphHeader}>
            <div>
              <h2>{t("Topology")}</h2>
              <span>{data?.clusters.length ?? 0} kind clusters</span>
            </div>
          </div>
          {controllerIssues[0] && (
            <div
              className={styles.reconcileError}
              role="alert"
              data-testid="reconciliation-error"
            >
              <strong>
                {t("Reconciliation blocked")} · {controllerIssues[0].scope}
              </strong>
              <span>
                {translateDiagnostic(controllerIssues[0].message, locale)}
              </span>
            </div>
          )}
          <div className={styles.graphBody}>
            {topologyData && draft ? (
              <Topology
                snapshot={topologyData}
                draft={draft}
                editorOpen
                onPod={(cluster, pod) => setSelected({ cluster, pod })}
                onResource={selectTab}
              />
            ) : (
              <div className={styles.emptyState}>
                {data
                  ? t("Waiting for the FRC. Run the demo setup script first.")
                  : t("Connecting to local kind clusters…")}
              </div>
            )}
          </div>
        </section>
      </main>
      <KubectlTerminal
        apiPath={apiPath}
        clusters={data?.clusters ?? []}
        open={terminalOpen}
        onClose={() => setTerminalOpen(false)}
      />
      <PodDetails selected={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
