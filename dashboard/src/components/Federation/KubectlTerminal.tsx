"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { FitAddon } from "@xterm/addon-fit";
import type { Terminal } from "@xterm/xterm";
import type { ClusterView } from "@/federation/model";
import { useLocale } from "./Locale";
import styles from "./federation.module.css";

interface Props {
  apiPath: string | null;
  clusters: ClusterView[];
  open: boolean;
  onClose: () => void;
  standalone?: boolean;
}

type Connection = "idle" | "connecting" | "connected" | "closed" | "error";

function fitWithBottomGutter(instance: Terminal, fitter: FitAddon) {
  const dimensions = fitter.proposeDimensions();
  if (!dimensions) return;
  instance.resize(dimensions.cols, Math.max(2, dimensions.rows - 1));
}

function terminalUrl(apiPath: string, cluster: string) {
  const url = new URL(`${apiPath}/terminal`, window.location.href);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("cluster", cluster);
  return url.toString();
}

interface TerminalSessionProps {
  active: boolean;
  apiPath: string;
  clusterId: string;
  enabled: boolean;
  fullscreen: boolean;
  reconnect: number;
  standalone: boolean;
  onConnection: (clusterId: string, connection: Connection) => void;
  onExitFullscreen: () => void;
  onTerminal: (clusterId: string, terminal: Terminal | null) => void;
}

function TerminalSession({
  active,
  apiPath,
  clusterId,
  enabled,
  fullscreen,
  reconnect,
  standalone,
  onConnection,
  onExitFullscreen,
  onTerminal,
}: TerminalSessionProps) {
  const host = useRef<HTMLDivElement>(null);
  const terminal = useRef<Terminal | null>(null);
  const fit = useRef<FitAddon | null>(null);
  const activeState = useRef(active);
  const fullscreenState = useRef(fullscreen);
  activeState.current = active;
  fullscreenState.current = fullscreen;

  useEffect(() => {
    if (!enabled || !host.current) return;
    let disposed = false;
    let socket: WebSocket | undefined;
    let observer: ResizeObserver | undefined;
    let input: { dispose(): void } | undefined;
    let resized: { dispose(): void } | undefined;
    let frame = 0;
    const element = host.current;
    onConnection(clusterId, "connecting");

    void Promise.all([import("@xterm/xterm"), import("@xterm/addon-fit")])
      .then(([xterm, addon]) => {
        if (disposed) return;
        element.replaceChildren();
        const instance = new xterm.Terminal({
          allowProposedApi: false,
          convertEol: false,
          cursorBlink: true,
          cursorStyle: "bar",
          fontFamily:
            '"SFMono-Regular", "Cascadia Code", "Roboto Mono", Menlo, Consolas, monospace',
          fontSize: standalone ? 14 : 12,
          lineHeight: 1.28,
          scrollback: 10000,
          theme: {
            background: "#fffdf9",
            foreground: "#302f36",
            cursor: "#655ee8",
            cursorAccent: "#fffdf9",
            selectionBackground: "#dcd9ff",
            black: "#3c3b43",
            red: "#b94843",
            green: "#48795c",
            yellow: "#96701d",
            blue: "#536ac4",
            magenta: "#8460ae",
            cyan: "#337d82",
            white: "#e7e4df",
            brightBlack: "#77747e",
            brightRed: "#cc5b55",
            brightGreen: "#5a936f",
            brightYellow: "#ab842c",
            brightBlue: "#687ed5",
            brightMagenta: "#9872c0",
            brightCyan: "#438e93",
            brightWhite: "#fffdf9",
          },
        });
        const fitter = new addon.FitAddon();
        instance.loadAddon(fitter);
        instance.open(element);
        instance.attachCustomKeyEventHandler((event) => {
          if (
            event.type === "keydown" &&
            event.key === "Escape" &&
            fullscreenState.current
          ) {
            onExitFullscreen();
            return false;
          }
          return true;
        });
        terminal.current = instance;
        fit.current = fitter;
        onTerminal(clusterId, instance);
        const resize = () => {
          cancelAnimationFrame(frame);
          frame = requestAnimationFrame(() => {
            try {
              fitWithBottomGutter(instance, fitter);
            } catch {
              // The panel can be between hidden and visible layout states.
            }
          });
        };
        observer = new ResizeObserver(resize);
        observer.observe(element);
        resize();

        socket = new WebSocket(terminalUrl(apiPath, clusterId));
        socket.binaryType = "arraybuffer";
        socket.onopen = () => {
          if (disposed) return;
          onConnection(clusterId, "connected");
          socket?.send(
            JSON.stringify({
              type: "resize",
              cols: instance.cols,
              rows: instance.rows,
            }),
          );
          if (activeState.current) instance.focus();
        };
        socket.onmessage = (event) => {
          if (typeof event.data === "string") instance.write(event.data);
          else if (event.data instanceof ArrayBuffer)
            instance.write(new Uint8Array(event.data));
        };
        socket.onerror = () => {
          if (!disposed) onConnection(clusterId, "error");
        };
        socket.onclose = () => {
          if (!disposed) onConnection(clusterId, "closed");
        };
        input = instance.onData((data) => {
          if (socket?.readyState === WebSocket.OPEN)
            socket.send(JSON.stringify({ type: "input", data }));
        });
        resized = instance.onResize(({ cols, rows }) => {
          if (socket?.readyState === WebSocket.OPEN)
            socket.send(JSON.stringify({ type: "resize", cols, rows }));
        });
      })
      .catch(() => {
        if (!disposed) onConnection(clusterId, "error");
      });

    return () => {
      disposed = true;
      cancelAnimationFrame(frame);
      observer?.disconnect();
      input?.dispose();
      resized?.dispose();
      socket?.close();
      terminal.current?.dispose();
      terminal.current = null;
      fit.current = null;
      onTerminal(clusterId, null);
      element.replaceChildren();
    };
  }, [
    apiPath,
    clusterId,
    enabled,
    onConnection,
    onExitFullscreen,
    onTerminal,
    reconnect,
    standalone,
  ]);

  useEffect(() => {
    if (!active || !terminal.current || !fit.current) return;
    terminal.current.options.fontSize = standalone || fullscreen ? 14 : 12;
    requestAnimationFrame(() => {
      if (terminal.current && fit.current) {
        fitWithBottomGutter(terminal.current, fit.current);
        terminal.current.focus();
      }
    });
  }, [active, fullscreen, standalone]);

  return (
    <div
      ref={host}
      className={styles.terminalXterm}
      data-testid={active ? "terminal-xterm" : undefined}
      hidden={!active}
      onClick={() => terminal.current?.focus()}
    />
  );
}

export default function KubectlTerminal({
  apiPath,
  clusters,
  open,
  onClose,
  standalone = false,
}: Props) {
  const { t } = useLocale();
  const [clusterId, setClusterId] = useState("");
  const [connections, setConnections] = useState<Record<string, Connection>>(
    {},
  );
  const [reconnects, setReconnects] = useState<Record<string, number>>({});
  const [activatedClusters, setActivatedClusters] = useState<string[]>([]);
  const [fullscreen, setFullscreen] = useState(false);
  const terminals = useRef(new Map<string, Terminal>());
  const cluster =
    clusters.find((candidate) => candidate.id === clusterId) ?? clusters[0];
  const selectedClusterId = cluster?.id;
  const connection = selectedClusterId
    ? (connections[selectedClusterId] ?? "idle")
    : "idle";

  const handleConnection = useCallback(
    (id: string, nextConnection: Connection) => {
      setConnections((current) =>
        current[id] === nextConnection
          ? current
          : { ...current, [id]: nextConnection },
      );
    },
    [],
  );
  const handleTerminal = useCallback(
    (id: string, instance: Terminal | null) => {
      if (instance) terminals.current.set(id, instance);
      else terminals.current.delete(id);
    },
    [],
  );
  const exitFullscreen = useCallback(() => setFullscreen(false), []);

  useEffect(() => {
    if (!clusters.some((candidate) => candidate.id === clusterId))
      setClusterId(clusters[0]?.id ?? "");
  }, [clusterId, clusters]);

  useEffect(() => {
    if (!open) {
      setActivatedClusters([]);
      return;
    }
    if (!selectedClusterId) return;
    setActivatedClusters((current) =>
      current.includes(selectedClusterId)
        ? current
        : [...current, selectedClusterId],
    );
  }, [open, selectedClusterId]);

  useEffect(() => {
    if (!open) setFullscreen(false);
  }, [open]);

  const stateLabel =
    connection === "connected"
      ? t("Connected")
      : connection === "connecting"
        ? t("Connecting…")
        : connection === "error"
          ? t("Connection failed")
          : connection === "closed"
            ? t("Session ended")
            : "";

  return (
    <section
      className={`${styles.terminalPanel} ${open ? "" : styles.terminalClosed} ${fullscreen ? styles.terminalFullscreen : ""} ${standalone ? styles.terminalStandalone : ""}`}
      aria-label={t("Cluster terminal")}
      aria-hidden={!open}
      onKeyDown={(event) => {
        if (event.key === "Escape" && fullscreen) {
          event.preventDefault();
          event.stopPropagation();
          setFullscreen(false);
        }
      }}
    >
      <header className={styles.terminalHeader}>
        <div className={styles.terminalTitle}>
          <span aria-hidden="true">›_</span>
          <strong>{t("Interactive shell")}</strong>
        </div>
        <div
          className={styles.terminalClusters}
          role="tablist"
          aria-label={t("Cluster")}
        >
          {clusters.map((candidate) => (
            <button
              key={candidate.id}
              type="button"
              role="tab"
              aria-selected={candidate.id === cluster?.id}
              onClick={() => setClusterId(candidate.id)}
            >
              <span className={styles.terminalClusterDot} />
              {candidate.label}
            </button>
          ))}
        </div>
        <span className={styles.terminalScope}>
          {cluster?.namespace ?? "—"}
        </span>
        <span className={`${styles.terminalStatus} ${styles[connection]}`}>
          {stateLabel}
        </span>
        <button
          type="button"
          className={styles.terminalTextButton}
          onClick={() => {
            if (selectedClusterId)
              terminals.current.get(selectedClusterId)?.clear();
          }}
        >
          {t("Clear")}
        </button>
        {(connection === "closed" || connection === "error") && (
          <button
            type="button"
            className={styles.terminalTextButton}
            onClick={() => {
              if (!selectedClusterId) return;
              setReconnects((current) => ({
                ...current,
                [selectedClusterId]: (current[selectedClusterId] ?? 0) + 1,
              }));
            }}
          >
            {t("Reconnect")}
          </button>
        )}
        {!standalone && (
          <>
            <a
              className={styles.terminalRouteLink}
              href={(apiPath ?? "./api/federation").replace(
                /\/api\/federation$/,
                "/terminal",
              )}
              target="_blank"
              rel="noreferrer"
              aria-label={t("Open terminal in a new tab")}
              title={t("Open terminal in a new tab")}
            >
              ↗
            </a>
            <button
              type="button"
              className={styles.terminalFullscreenToggle}
              aria-label={t(
                fullscreen ? "Exit fullscreen" : "Fullscreen terminal",
              )}
              aria-pressed={fullscreen}
              title={t(fullscreen ? "Exit fullscreen" : "Fullscreen terminal")}
              onClick={() => setFullscreen((value) => !value)}
            >
              <svg viewBox="0 0 20 20" aria-hidden="true">
                <path
                  d={
                    fullscreen
                      ? "M3 7h4V3M17 7h-4V3M3 13h4v4M17 13h-4v4"
                      : "M7 3H3v4M13 3h4v4M3 13v4h4M17 13v4h-4"
                  }
                />
              </svg>
            </button>
          </>
        )}
        {standalone ? (
          <a className={styles.terminalBack} href="./federation">
            Federation Lab
          </a>
        ) : (
          <button
            type="button"
            className={styles.terminalClose}
            onClick={onClose}
            aria-label={t("Close terminal")}
          >
            ×
          </button>
        )}
      </header>
      {apiPath &&
        clusters.map((candidate) => (
          <TerminalSession
            key={candidate.id}
            active={candidate.id === selectedClusterId}
            apiPath={apiPath}
            clusterId={candidate.id}
            enabled={open && activatedClusters.includes(candidate.id)}
            fullscreen={fullscreen}
            reconnect={reconnects[candidate.id] ?? 0}
            standalone={standalone}
            onConnection={handleConnection}
            onExitFullscreen={exitFullscreen}
            onTerminal={handleTerminal}
          />
        ))}
    </section>
  );
}
