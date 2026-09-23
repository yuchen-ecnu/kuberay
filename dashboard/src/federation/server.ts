import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";
import {
  manifest,
  parseResource,
  summarizePod,
  type ClusterView,
  type Federation,
  type Resource,
  type Snapshot,
} from "./model";

export interface ClusterConfig {
  id: string;
  label: string;
  role: "primary" | "member";
  memberName?: string;
  namespace: string;
  kubeconfig: string;
  context?: string;
  terminalTarget?: string;
  terminalContainer?: string;
}

export interface DemoConfig {
  name: string;
  namespace: string;
  clusters: ClusterConfig[];
}

export class DemoError extends Error {
  constructor(
    message: string,
    public status = 400,
  ) {
    super(message);
  }
}

export function validateConfig(value: DemoConfig): DemoConfig {
  const name = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/;
  if (
    !name.test(value?.name ?? "") ||
    !name.test(value?.namespace ?? "") ||
    !Array.isArray(value.clusters) ||
    !value.clusters.length ||
    value.clusters.length > 17
  ) {
    throw new DemoError(
      "Demo configuration requires name, namespace and clusters.",
      503,
    );
  }
  const ids = new Set<string>();
  const members = new Set<string>();
  for (const cluster of value.clusters) {
    if (
      !name.test(cluster.id ?? "") ||
      ids.has(cluster.id) ||
      !cluster.label ||
      !name.test(cluster.namespace ?? "") ||
      !isAbsolute(cluster.kubeconfig ?? "") ||
      (cluster.terminalTarget !== undefined &&
        !/^(?:pod|deployment)\/[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(
          cluster.terminalTarget,
        )) ||
      (cluster.terminalContainer !== undefined &&
        !name.test(cluster.terminalContainer)) ||
      !["primary", "member"].includes(cluster.role)
    ) {
      throw new DemoError(
        "Invalid cluster configuration: provide a unique id, namespace and absolute kubeconfig path.",
        503,
      );
    }
    ids.add(cluster.id);
    if (cluster.role === "member") {
      if (!cluster.memberName || members.has(cluster.memberName)) {
        throw new DemoError(
          "Each member cluster needs a unique memberName.",
          503,
        );
      }
      members.add(cluster.memberName);
    } else if (cluster.namespace !== value.namespace) {
      throw new DemoError(
        "The primary namespace must match the FRC namespace.",
        503,
      );
    }
  }
  if (value.clusters.filter((c) => c.role === "primary").length !== 1) {
    throw new DemoError(
      "Demo configuration must contain exactly one primary cluster.",
      503,
    );
  }
  return value;
}

export type Kubectl = (
  cluster: ClusterConfig,
  args: string[],
  input?: unknown,
) => Promise<string>;

export const kubectl: Kubectl = (cluster, args, input) =>
  new Promise((resolve, reject) => {
    const child = spawn(
      process.env.FEDERATION_KUBECTL ?? "kubectl",
      [
        "--kubeconfig",
        cluster.kubeconfig,
        ...(cluster.context ? ["--context", cluster.context] : []),
        "--namespace",
        cluster.namespace,
        "--request-timeout=8s",
        ...args,
      ],
      { stdio: ["pipe", "pipe", "pipe"], shell: false },
    );
    let stdout = "";
    let stderr = "";
    let failure: DemoError | undefined;
    const timeout = setTimeout(() => {
      failure = new DemoError("The Kubernetes API request timed out.", 504);
      child.kill("SIGKILL");
    }, 12000);
    const collect = (target: "out" | "err", data: Buffer) => {
      if (target === "out") stdout += data.toString();
      else stderr += data.toString();
      if (stdout.length + stderr.length > 4 * 1024 * 1024) {
        failure = new DemoError(
          "The cluster response exceeds the demo size limit.",
          502,
        );
        child.kill("SIGKILL");
      }
    };
    child.stdout.on("data", (data) => collect("out", data));
    child.stderr.on("data", (data) => collect("err", data));
    child.stdin.on("error", () => {
      /* Process exit is handled below. */
    });
    child.on("error", () => {
      clearTimeout(timeout);
      reject(
        new DemoError(
          "Could not start kubectl. Check FEDERATION_KUBECTL.",
          503,
        ),
      );
    });
    child.on("close", (code) => {
      clearTimeout(timeout);
      if (failure) return reject(failure);
      if (code !== 0) {
        const message = stderr
          .replaceAll(cluster.kubeconfig, "[kubeconfig]")
          .slice(0, 3000);
        return reject(
          new DemoError(
            message || "The Kubernetes API request failed.",
            /Conflict|modified/.test(stderr) ? 409 : 422,
          ),
        );
      }
      resolve(stdout);
    });
    child.stdin.end(input ? JSON.stringify(input) : undefined);
  });

// Status-only updates do not invalidate an editor's base spec. The API server's
// resourceVersion still protects the final replace against concurrent writes.
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, item]) => [key, canonical(item)]),
    );
  }
  return value;
}

export function revision(resource: Resource) {
  return createHash("sha256")
    .update(
      JSON.stringify(
        canonical({ uid: resource.metadata.uid, spec: resource.spec }),
      ),
    )
    .digest("hex");
}

export function createFederationService(
  config: DemoConfig,
  run: Kubectl = kubectl,
) {
  validateConfig(config);
  const primary = config.clusters.find((c) => c.role === "primary")!;
  let cached: Promise<Snapshot> | undefined;
  let cachedAt = 0;
  let writing = false;
  const read = async (
    cluster: ClusterConfig,
    kind: string,
    name = config.name,
  ): Promise<Resource | null> => {
    const output = await run(cluster, [
      "get",
      kind,
      name,
      "--ignore-not-found",
      "-o",
      "json",
    ]);
    return output.trim() ? JSON.parse(output) : null;
  };

  async function snapshot(): Promise<Snapshot> {
    if (cached && Date.now() - cachedAt < 2500) return cached;
    cachedAt = Date.now();
    cached = (async () => {
      const federationResult = read(primary, "federatedrayclusters.ray.io");
      const clusterResults = config.clusters.map(
        async (cluster): Promise<ClusterView> => {
          const view: ClusterView = {
            id: cluster.id,
            label: cluster.label,
            role: cluster.role,
            memberName: cluster.memberName,
            namespace: cluster.namespace,
            resource: null,
            revision: null,
            pods: [],
          };
          const federation =
            cluster.role === "member"
              ? await federationResult.catch(() => null)
              : null;
          const memberFederation = federation as Federation | null;
          const members = memberFederation?.status?.memberClusterStatuses;
          const declaredMember = memberFederation?.spec.memberClusters.find(
            (member) =>
              member.name === cluster.memberName &&
              member.namespace === cluster.namespace,
          );
          const member = Array.isArray(members)
            ? members.find(
                (m) =>
                  m.name === cluster.memberName &&
                  m.namespace === cluster.namespace,
              )
            : undefined;
          // A configured connection may outlive a logical FRC member. Once it
          // is absent from both spec and status, falling back would alias an
          // existing legacy member that still uses the FRC name.
          if (
            cluster.role === "member" &&
            memberFederation &&
            !declaredMember &&
            !member
          )
            return view;
          // Persisted names cover both new members and existing members that
          // retain their old names. Manual/older deployments use the demo name.
          const clusterName = member?.rayClusterName || config.name;
          const results = await Promise.allSettled([
            read(cluster, "rayclusters.ray.io", clusterName),
            run(cluster, [
              "get",
              "pods",
              "-l",
              `ray.io/cluster=${clusterName},ray.io/federation-probe!=true`,
              "-o",
              "json",
            ]),
          ]);
          const [resource, pods] = results;
          if (resource.status === "fulfilled") {
            view.resource = resource.value;
            view.revision = resource.value ? revision(resource.value) : null;
          }
          if (pods.status === "fulfilled")
            view.pods = JSON.parse(pods.value).items.map(summarizePod);
          view.error =
            results
              .filter((r) => r.status === "rejected")
              .map((r) => String((r as PromiseRejectedResult).reason.message))
              .join("; ") || undefined;
          return view;
        },
      );
      const [frc, ...clusters] = await Promise.allSettled([
        federationResult,
        ...clusterResults,
      ]);
      const federation =
        frc.status === "fulfilled" ? (frc.value as Federation | null) : null;
      const activeMembers = federation
        ? new Set([
            ...federation.spec.memberClusters.map((member) => member.name),
            ...(Array.isArray(federation.status?.memberClusterStatuses)
              ? federation.status.memberClusterStatuses.map(
                  (member: { name: string }) => member.name,
                )
              : []),
          ])
        : null;
      return {
        updatedAt: new Date().toISOString(),
        federation,
        revision: federation ? revision(federation) : null,
        error: frc.status === "rejected" ? frc.reason.message : undefined,
        clusters: clusters
          .map((result, index) =>
            result.status === "fulfilled"
              ? (result.value as ClusterView)
              : {
                  id: config.clusters[index].id,
                  label: config.clusters[index].label,
                  role: config.clusters[index].role,
                  memberName: config.clusters[index].memberName,
                  namespace: config.clusters[index].namespace,
                  resource: null,
                  pods: [],
                  error: "Could not read cluster resources.",
                },
          )
          .filter(
            (cluster) =>
              cluster.role === "primary" ||
              !activeMembers ||
              activeMembers.has(cluster.memberName!),
          ),
      };
    })();
    return cached;
  }

  async function update(
    source: string,
    baseRevision: string,
    dryRun: boolean,
    target: "frc" | "primary" = "frc",
  ) {
    if (writing)
      throw new DemoError(
        "An update is still in progress. Try again shortly.",
        409,
      );
    const draft = parseResource(source);
    writing = true;
    try {
      const kind =
        target === "primary"
          ? "rayclusters.ray.io"
          : "federatedrayclusters.ray.io";
      const current = await read(primary, kind);
      if (!current)
        throw new DemoError(
          target === "primary"
            ? "The PRC has not been created. Wait for the federation controller to initialize it."
            : "The FRC does not exist. Run the demo setup script first.",
          404,
        );
      if (!baseRevision || revision(current) !== baseRevision) {
        throw new DemoError(
          "The cluster spec changed. Reload the latest YAML and merge your changes.",
          409,
        );
      }
      // Forward the edited resource without interpreting its API semantics.
      // replace still needs the live identity fields for optimistic concurrency.
      const next = structuredClone(draft);
      if (
        next.metadata === undefined ||
        (next.metadata !== null &&
          typeof next.metadata === "object" &&
          !Array.isArray(next.metadata))
      ) {
        next.metadata = {
          ...(next.metadata ?? {}),
          uid: current.metadata.uid,
          resourceVersion: current.metadata.resourceVersion,
        };
      }
      const args = ["replace", "--validate=strict", "-f", "-", "-o", "json"];
      const validated = JSON.parse(
        await run(primary, [...args, "--dry-run=server"], next),
      );
      if (dryRun)
        return {
          message:
            "Kubernetes admission passed. Changes have not been applied.",
          resource: manifest(validated),
        };
      const applied = JSON.parse(await run(primary, args, next));
      cached = undefined;
      return {
        message:
          target === "primary"
            ? "Applied to PRC. Controllers are reconciling local and managed workers."
            : "Applied to FRC. The federation controller is reconciling topology and policy.",
        resource: manifest(applied),
        revision: revision(applied),
      };
    } finally {
      writing = false;
    }
  }
  async function dashboardTarget() {
    const observed = (await snapshot()).clusters.find(
      (c) => c.role === "primary",
    );
    const head = observed?.pods.find((pod) => pod.role === "head" && pod.ready);
    // A port-forward targets the Pod's listener, not the Service's NodePort.
    const port = Number(
      observed?.resource?.spec.headGroupSpec?.rayStartParams?.[
        "dashboard-port"
      ] ?? 8265,
    );
    if (
      observed?.error ||
      !head ||
      !Number.isInteger(port) ||
      port < 1 ||
      port > 65535
    ) {
      throw new DemoError(
        "Ray head is not ready. The Dashboard is temporarily unavailable.",
        503,
      );
    }
    return { cluster: primary, podName: head.name, podUID: head.uid, port };
  }
  return { snapshot, update, dashboardTarget };
}

let service: ReturnType<typeof createFederationService> | undefined;

export async function getFederationService() {
  if (service) return service;
  const filename = process.env.FEDERATION_DEMO_CONFIG;
  if (!filename)
    throw new DemoError(
      "Set FEDERATION_DEMO_CONFIG and follow dashboard/demo/federation/README.md to start the demo.",
      503,
    );
  let config: DemoConfig;
  try {
    config = JSON.parse(await readFile(filename, "utf8"));
  } catch {
    throw new DemoError(
      "Could not read the demo configuration JSON file.",
      503,
    );
  }
  service = createFederationService(config);
  return service;
}
