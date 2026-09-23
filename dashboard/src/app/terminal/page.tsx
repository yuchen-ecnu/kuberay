"use client";

import { useEffect, useState } from "react";
import useSWR from "swr";
import KubectlTerminal from "@/components/Federation/KubectlTerminal";
import { LocaleProvider } from "@/components/Federation/Locale";
import styles from "@/components/Federation/federation.module.css";
import type { Snapshot } from "@/federation/model";

async function fetchSnapshot(url: string): Promise<Snapshot> {
  const response = await fetch(url, { cache: "no-store" });
  const value = await response.json();
  if (!response.ok) throw new Error(value.error);
  return value;
}

export default function TerminalPage() {
  return (
    <LocaleProvider>
      <TerminalRoute />
    </LocaleProvider>
  );
}

function TerminalRoute() {
  const [basePath, setBasePath] = useState<string | null>(null);
  useEffect(() => {
    setBasePath(window.location.pathname.replace(/\/terminal\/?$/, ""));
  }, []);
  const apiPath = basePath === null ? null : `${basePath}/api/federation`;
  const { data, error } = useSWR<Snapshot>(apiPath, fetchSnapshot, {
    refreshInterval: 5000,
  });

  return (
    <main className={styles.terminalRoute}>
      <KubectlTerminal
        apiPath={apiPath}
        clusters={data?.clusters ?? []}
        open
        standalone
        onClose={() => {}}
      />
      {(error || (!data && apiPath)) && (
        <div className={styles.terminalRouteNotice} role="status">
          {error?.message ?? "Connecting to the demo clusters…"}
        </div>
      )}
    </main>
  );
}
