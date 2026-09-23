#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PRIMARY_KUBECONFIG=""
MEMBER_KUBECONFIG=""
STATE_DIR="/tmp/kuberay-federation-demo"
PROXY_PATH="${FEDERATION_DEMO_PROXY_PATH:-}"
BIND_ADDRESS="127.0.0.1"
DEMO_PORT="3000"
RAY_IMAGE="${RAY_IMAGE:-rayproject/ray:2.56.0-py311-cpu}"
MEMBER_NODE="frc-member-control-plane"
HEAD_ADDRESS="10.96.0.110"

usage() {
  cat <<'EOF'
Usage: dashboard/demo/federation/run.sh \
  --primary-kubeconfig /absolute/path/primary.kubeconfig \
  --member-kubeconfig /absolute/path/member.kubeconfig [options]

Prepare or refresh the demo, build the dashboard, and run its custom server.
The two kind clusters, federation CRDs, operators, and cross-cluster routes must
already exist. See dashboard/demo/federation/README.md for cluster setup.

Options:
  --state-dir PATH        Local demo state (default: /tmp/kuberay-federation-demo)
  --proxy-path PATH       Browser proxy prefix, for example /workspace/proxy/3000
  --bind ADDRESS          Server bind address (default: 127.0.0.1)
  --port PORT             Server port (default: 3000)
  --image IMAGE           Ray image already loaded in both kind clusters
  --member-node NAME      Member kind control-plane container name
  --head-address IP       Head Service IP in the primary Service CIDR
  --help                  Show this help
EOF
}

while (($#)); do
  case "$1" in
    --primary-kubeconfig|--member-kubeconfig|--state-dir|--proxy-path|--bind|--port|--image|--member-node|--head-address)
      if (($# < 2)); then echo "Missing value for $1" >&2; exit 2; fi
      case "$1" in
        --primary-kubeconfig) PRIMARY_KUBECONFIG="$2" ;;
        --member-kubeconfig) MEMBER_KUBECONFIG="$2" ;;
        --state-dir) STATE_DIR="$2" ;;
        --proxy-path) PROXY_PATH="$2" ;;
        --bind) BIND_ADDRESS="$2" ;;
        --port) DEMO_PORT="$2" ;;
        --image) RAY_IMAGE="$2" ;;
        --member-node) MEMBER_NODE="$2" ;;
        --head-address) HEAD_ADDRESS="$2" ;;
      esac
      shift 2
      ;;
    --help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for config in "$PRIMARY_KUBECONFIG" "$MEMBER_KUBECONFIG"; do
  if [[ "$config" != /* || ! -f "$config" ]]; then
    echo "Both kubeconfig paths must be absolute paths to existing files." >&2
    exit 2
  fi
done
if [[ -n "$PROXY_PATH" && "$PROXY_PATH" != /* ]]; then
  echo "--proxy-path must start with /." >&2
  exit 2
fi
if [[ ! "$DEMO_PORT" =~ ^[0-9]+$ ]] || ((DEMO_PORT < 1 || DEMO_PORT > 65535)); then
  echo "--port must be an integer from 1 to 65535." >&2
  exit 2
fi
for executable in node python3 kubectl docker; do
  command -v "$executable" >/dev/null || { echo "Missing executable: $executable" >&2; exit 1; }
done
if [[ "$(node --version)" != v22.* ]]; then
  echo "Node.js 22 is required." >&2
  exit 1
fi

STATE_DIR="$(python3 -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$STATE_DIR")"
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"

python3 "$SCRIPT_DIR/setup.py" \
  --primary-kubeconfig "$PRIMARY_KUBECONFIG" \
  --member-kubeconfig "$MEMBER_KUBECONFIG" \
  --state-dir "$STATE_DIR" \
  --member-node "$MEMBER_NODE" \
  --head-address "$HEAD_ADDRESS" \
  --image "$RAY_IMAGE"

cd "$DASHBOARD_DIR"
export FEDERATION_DEMO_PROXY_PATH="$PROXY_PATH"
node .yarn/releases/yarn-4.9.2.cjs install --immutable
node .yarn/releases/yarn-4.9.2.cjs build:federation

echo "Direct URL: http://$BIND_ADDRESS:$DEMO_PORT/federation"
if [[ -n "$PROXY_PATH" ]]; then
  echo "Open the workspace proxy URL ending in ${PROXY_PATH}/federation."
fi
NODE_ENV=production HOSTNAME="$BIND_ADDRESS" PORT="$DEMO_PORT" \
  FEDERATION_DEMO_CONFIG="$STATE_DIR/config.json" \
  exec node .yarn/releases/yarn-4.9.2.cjs start:federation
