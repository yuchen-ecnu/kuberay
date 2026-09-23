import { useEffect, useMemo, useRef, useState } from "react";
import { yaml } from "@codemirror/lang-yaml";
import CodeMirror from "@uiw/react-codemirror";
import {
  resourceHighlights,
  type DesignField,
  type YamlHighlight,
} from "@/federation/yaml-highlights";
import type { MessageKey } from "@/federation/i18n";
import { yamlFolds, type YamlFold } from "@/federation/yaml-folding";
import { useLocale } from "./Locale";
import styles from "./federation.module.css";

const descriptions: Record<DesignField, MessageKey> = {
  managedBy:
    "New group-level managedBy: ray.io/federated-raycluster-controller delegates the group to the federation controller, without local Pods.",
  "rayStartParams.address":
    "Existing rayStartParams.address: workers connect to the primary head.",
  enableInTreeAutoscaling:
    "Existing enableInTreeAutoscaling: false disables the built-in autoscaler.",
  "primaryCluster.enableInTreeAutoscaling":
    "FRC primaryCluster configures the global autoscaler; the default demo scales through the PRC.",
  "primaryCluster.autoscalerOptions":
    "FRC autoscalerOptions belongs to primaryCluster and is projected to the PRC spec.",
};

function Highlight({
  source,
  fields,
  hiddenLines,
  collapsedFolds,
}: {
  source: string;
  fields: YamlHighlight[];
  hiddenLines: Set<number>;
  collapsedFolds: Map<number, YamlFold>;
}) {
  const { t } = useLocale();
  return (
    <>
      {source.split("\n").map((line, index) => {
        if (hiddenLines.has(index)) return null;
        const match = line.match(/^(\s*(?:- )?)([^:#]+:)(.*)$/);
        const highlight = fields.find(
          (field) => index >= field.startLine && index <= field.endLine,
        );
        const collapsed = collapsedFolds.get(index);
        return (
          <div
            key={index}
            className={`${styles.yamlLine} ${highlight ? (highlight.field === "managedBy" ? styles.yamlAddedLine : styles.yamlReusedLine) : ""}`}
            data-line={index}
            data-design-field={highlight?.field}
            title={highlight ? t(descriptions[highlight.field]) : undefined}
          >
            {match ? (
              <>
                {match[1]}
                <span className={styles.yamlKey}>{match[2]}</span>
                <span className={styles.yamlValue}>{match[3]}</span>
              </>
            ) : (
              line || " "
            )}
            {collapsed && (
              <span className={styles.yamlFoldSummary}>
                {" "}
                …{" "}
                {t("{count} lines folded", {
                  count: collapsed.endLine - collapsed.startLine,
                })}
              </span>
            )}
          </div>
        );
      })}
    </>
  );
}

export default function YamlEditor({
  source,
  onChange,
  readOnly,
  label,
}: {
  source: string;
  onChange: (value: string) => void;
  readOnly: boolean;
  label?: string;
}) {
  const { t } = useLocale();
  const editorLabel = label ?? t("FederatedRayCluster YAML editor");
  const editorContent = useRef<HTMLElement | null>(null);
  const lines = useRef<HTMLDivElement>(null);
  const preview = useRef<HTMLPreElement>(null);
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());
  const highlights = useMemo(
    () =>
      readOnly
        ? resourceHighlights(source)
        : { fields: [], workersOnly: false },
    [source, readOnly],
  );
  const folds = useMemo(
    () => (readOnly ? yamlFolds(source) : []),
    [source, readOnly],
  );
  const editorExtensions = useMemo(() => [yaml()], []);
  useEffect(() => {
    editorContent.current?.setAttribute("aria-label", editorLabel);
  }, [editorLabel]);
  const sourceKey = useMemo(() => {
    let hash = 2166136261;
    for (let index = 0; index < source.length; index += 1) {
      hash ^= source.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(36);
  }, [source]);
  const foldKey = (fold: YamlFold) =>
    `${sourceKey}:${fold.startLine}:${fold.endLine}`;
  const activeFolds = useMemo(
    () => folds.filter((fold) => collapsed.has(foldKey(fold))),
    // foldKey is derived entirely from sourceKey.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [folds, collapsed, sourceKey],
  );
  const hiddenLines = useMemo(() => {
    const result = new Set<number>();
    for (const fold of activeFolds)
      for (let line = fold.startLine + 1; line <= fold.endLine; line += 1)
        result.add(line);
    return result;
  }, [activeFolds]);
  const collapsedFolds = useMemo(
    () => new Map(activeFolds.map((fold) => [fold.startLine, fold])),
    [activeFolds],
  );
  const foldByStart = useMemo(
    () => new Map(folds.map((fold) => [fold.startLine, fold])),
    [folds],
  );
  const visibleLines = source
    .split("\n")
    .map((_, index) => index)
    .filter((index) => !hiddenLines.has(index));

  function toggleFold(fold: YamlFold) {
    const key = foldKey(fold);
    setCollapsed((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }
  function jumpTo(field: YamlHighlight) {
    setCollapsed((current) => {
      const next = new Set(current);
      for (const fold of folds)
        if (field.startLine > fold.startLine && field.startLine <= fold.endLine)
          next.delete(foldKey(fold));
      return next;
    });
    requestAnimationFrame(() => {
      const element = preview.current;
      const row = element?.querySelector<HTMLElement>(
        `[data-line="${field.startLine}"]`,
      );
      if (!element || !row) return;
      element.scrollTo({
        top: Math.max(0, row.offsetTop - element.clientHeight / 3),
        left: 0,
      });
      element.focus({ preventScroll: true });
    });
  }
  return (
    <>
      {highlights.fields.length > 0 && (
        <aside
          className={styles.yamlDesign}
          aria-label={t("Fields involved in this design")}
        >
          <div className={styles.yamlDesignLegend}>
            <strong>{t("Design changes")}</strong>
            <span className={styles.yamlAddedLegend}>{t("New field")}</span>
            <span className={styles.yamlReusedLegend}>
              {t("Existing field")}
            </span>
          </div>
          <div className={styles.yamlDesignLinks}>
            {highlights.fields.map((field) => (
              <button
                key={`${field.field}:${field.startLine}`}
                type="button"
                className={
                  field.field === "managedBy"
                    ? styles.yamlAddedLink
                    : styles.yamlReusedLink
                }
                title={t(descriptions[field.field])}
                onClick={() => jumpTo(field)}
              >
                <code>{field.field}</code>
                <span aria-hidden="true">↗</span>
              </button>
            ))}
          </div>
          {highlights.workersOnly && (
            <p>{t("headGroupSpec is omitted; workers only.")}</p>
          )}
        </aside>
      )}
      <div className={styles.codeEditor}>
        {readOnly && (
          <div
            className={`${styles.lineNumbers} ${styles.foldLineNumbers}`}
            ref={lines}
          >
            {visibleLines.map((index) => {
              const highlight = highlights.fields.find(
                (field) => index >= field.startLine && index <= field.endLine,
              );
              const fold = foldByStart.get(index);
              const isCollapsed = !!fold && collapsed.has(foldKey(fold));
              return (
                <div
                  key={index}
                  className={`${styles.yamlLineNumber} ${
                    highlight
                      ? highlight.field === "managedBy"
                        ? styles.yamlAddedNumber
                        : styles.yamlReusedNumber
                      : ""
                  }`}
                  data-fold-line={fold ? index : undefined}
                >
                  {fold ? (
                    <button
                      type="button"
                      className={styles.yamlFoldToggle}
                      aria-label={t(
                        isCollapsed ? "Expand line {line}" : "Fold line {line}",
                        { line: index + 1 },
                      )}
                      title={t(
                        isCollapsed ? "Expand line {line}" : "Fold line {line}",
                        { line: index + 1 },
                      )}
                      aria-expanded={!isCollapsed}
                      onClick={() => toggleFold(fold)}
                    >
                      <span aria-hidden="true">{isCollapsed ? "▸" : "⌄"}</span>
                    </button>
                  ) : (
                    <span className={styles.yamlFoldSpacer} />
                  )}
                  <span
                    className={
                      highlight
                        ? highlight.field === "managedBy"
                          ? styles.yamlAddedNumber
                          : styles.yamlReusedNumber
                        : undefined
                    }
                  >
                    {index + 1}
                  </span>
                </div>
              );
            })}
          </div>
        )}
        {readOnly ? (
          <pre
            ref={preview}
            aria-label={t("YAML preview")}
            tabIndex={0}
            onScroll={(event) => {
              if (lines.current)
                lines.current.scrollTop = event.currentTarget.scrollTop;
            }}
          >
            <Highlight
              source={source}
              fields={highlights.fields}
              hiddenLines={hiddenLines}
              collapsedFolds={collapsedFolds}
            />
          </pre>
        ) : (
          <CodeMirror
            className={styles.codeMirror}
            value={source}
            height="100%"
            extensions={editorExtensions}
            indentWithTab
            basicSetup={{
              foldGutter: true,
              lineNumbers: true,
              highlightActiveLine: true,
              highlightActiveLineGutter: true,
            }}
            onChange={onChange}
            onCreateEditor={(view) => {
              editorContent.current = view.contentDOM;
              view.contentDOM.setAttribute("aria-label", editorLabel);
              view.contentDOM.setAttribute("aria-multiline", "true");
              view.contentDOM.setAttribute("autocapitalize", "off");
              view.contentDOM.setAttribute("autocomplete", "off");
              view.contentDOM.setAttribute("spellcheck", "false");
            }}
          />
        )}
      </div>
    </>
  );
}
