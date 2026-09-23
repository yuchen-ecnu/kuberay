import { useEffect, useRef } from "react";
import type { ClusterView, PodView } from "@/federation/model";
import styles from "./federation.module.css";
import { useLocale } from "./Locale";

export type PodSelection = { cluster: ClusterView; pod: PodView };

export default function PodDetails({
  selected,
  onClose,
}: {
  selected: PodSelection | null;
  onClose: () => void;
}) {
  const { t } = useLocale();
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    if (selected) dialog.current?.showModal();
    else dialog.current?.close();
  }, [selected]);
  return (
    <dialog
      className={styles.podDialog}
      ref={dialog}
      onClose={onClose}
      aria-labelledby="pod-title"
    >
      {selected && (
        <>
          <div className={styles.dialogHeading}>
            <div>
              <span className={styles.eyebrow}>LIVE POD</span>
              <h2 id="pod-title">
                {selected.pod.role === "head" ? "Ray Head" : "Ray Worker"}
              </h2>
            </div>
            <button
              className={styles.iconButton}
              onClick={onClose}
              aria-label={t("Close Pod details")}
            >
              ×
            </button>
          </div>
          <code className={styles.podName}>{selected.pod.name}</code>
          <dl>
            {[
              ["Kubernetes", selected.cluster.label],
              [t("Namespace"), selected.cluster.namespace],
              [t("Node"), selected.pod.node],
              ["Pod IP", selected.pod.ip],
              [t("Worker group"), selected.pod.group || "—"],
              [t("Phase"), selected.pod.phase],
              [t("Ready"), String(selected.pod.ready)],
              [t("Restarts"), String(selected.pod.restarts)],
              [t("Created"), selected.pod.createdAt],
            ].map(([key, value]) => (
              <div key={key}>
                <dt>{key}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
          {selected.pod.reason && (
            <div className={styles.warning}>{selected.pod.reason}</div>
          )}
          {selected.pod.containers.map((container) => (
            <div className={styles.containerDetails} key={container.name}>
              <strong>{container.name}</strong>
              <code>{container.image}</code>
              <div>
                <span>{t("Requests")}</span>
                <code>
                  {Object.entries(container.requests)
                    .map(([key, value]) => `${key}: ${value}`)
                    .join(" · ") || "—"}
                </code>
              </div>
              <div>
                <span>{t("Limits")}</span>
                <code>
                  {Object.entries(container.limits)
                    .map(([key, value]) => `${key}: ${value}`)
                    .join(" · ") || "—"}
                </code>
              </div>
            </div>
          ))}
          <small className={styles.muted}>
            {t("Kubernetes snapshot captured when opened.")}
          </small>
        </>
      )}
    </dialog>
  );
}
