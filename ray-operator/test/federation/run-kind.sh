#!/usr/bin/env bash
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1

OPERATOR_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STATE_DIR="${STATE_DIR:-$(mktemp -d)}"
KIND_PREFIX="${KIND_PREFIX:-frc}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.35.0}"
RAY_IMAGE="${RAY_IMAGE:-rayproject/ray:2.56.0-py311-cpu}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-kuberay-federation:test}"
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"
for executable in go kind kubectl docker openssl python3 rg; do command -v "$executable" >/dev/null; done

for side in primary member; do
  cluster="$KIND_PREFIX-$side"
  if kind get clusters 2>/dev/null | rg -Fx -- "$cluster" >/dev/null; then
    if [[ "${REUSE_KIND_CLUSTERS:-0}" != 1 ]]; then
      echo "Cluster $cluster already exists. Set REUSE_KIND_CLUSTERS=1 to test it explicitly." >&2
      exit 1
    fi
    kind export kubeconfig --name "$cluster" --kubeconfig "$STATE_DIR/$side.kubeconfig"
  else
    pod_cidr=10.241.0.0/16
    service_cidr=10.96.0.0/16
    if [[ "$side" == member ]]; then pod_cidr=10.242.0.0/16; service_cidr=10.97.0.0/16; fi
    cat > "$STATE_DIR/$side-kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  podSubnet: $pod_cidr
  serviceSubnet: $service_cidr
nodes:
- role: control-plane
EOF
    if [[ "$(docker info --format '{{.CgroupVersion}}')" == 1 ]]; then
      cat >> "$STATE_DIR/$side-kind.yaml" <<'EOF'
kubeadmConfigPatches:
- |
  kind: KubeletConfiguration
  failCgroupV1: false
  cgroupDriver: cgroupfs
containerdConfigPatches:
- |
  [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc.options]
    SystemdCgroup = false
EOF
    fi
    kind create cluster --name "$cluster" --image "$KIND_NODE_IMAGE" --config "$STATE_DIR/$side-kind.yaml" --kubeconfig "$STATE_DIR/$side.kubeconfig" --wait 120s
  fi
done

primary_ip="$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$KIND_PREFIX-primary-control-plane")"
member_ip="$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$KIND_PREFIX-member-control-plane")"
docker exec "$KIND_PREFIX-primary-control-plane" ip route replace 10.242.0.0/16 via "$member_ip"
docker exec "$KIND_PREFIX-primary-control-plane" ip route replace 10.97.0.0/16 via "$member_ip"
docker exec "$KIND_PREFIX-member-control-plane" ip route replace 10.241.0.0/16 via "$primary_ip"
docker exec "$KIND_PREFIX-member-control-plane" ip route replace 10.96.0.0/16 via "$primary_ip"

docker image inspect "$RAY_IMAGE" >/dev/null 2>&1 || docker pull "$RAY_IMAGE"
(cd "$OPERATOR_ROOT" && CGO_ENABLED=0 go build -o bin/manager .)
cat > "$STATE_DIR/Dockerfile" <<'EOF'
FROM scratch
COPY bin/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
EOF
docker build -f "$STATE_DIR/Dockerfile" -t "$OPERATOR_IMAGE" "$OPERATOR_ROOT"

mkdir -p "$STATE_DIR/webhook"
cat > "$STATE_DIR/webhook/openssl.cnf" <<'EOF'
[req]
distinguished_name = subject
x509_extensions = extensions
prompt = no
[subject]
CN = kuberay-webhook-service.kuberay-system.svc
[extensions]
basicConstraints = critical,CA:TRUE
keyUsage = critical,digitalSignature,keyEncipherment,keyCertSign
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = kuberay-webhook-service.kuberay-system.svc
DNS.2 = kuberay-webhook-service.kuberay-system.svc.cluster.local
EOF
openssl req -x509 -nodes -newkey rsa:2048 -days 30 \
  -keyout "$STATE_DIR/webhook/tls.key" -out "$STATE_DIR/webhook/tls.crt" \
  -config "$STATE_DIR/webhook/openssl.cnf" >/dev/null 2>&1
webhook_cert="$(base64 < "$STATE_DIR/webhook/tls.crt" | tr -d '\n')"
webhook_key="$(base64 < "$STATE_DIR/webhook/tls.key" | tr -d '\n')"

cat > "$STATE_DIR/operator.yaml" <<EOF
apiVersion: v1
kind: Namespace
metadata: {name: kuberay-system}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: kuberay-operator, namespace: kuberay-system}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: kuberay-federation-test}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: kuberay-operator}
subjects:
- {kind: ServiceAccount, name: kuberay-operator, namespace: kuberay-system}
---
apiVersion: v1
kind: Secret
metadata: {name: kuberay-webhook-cert, namespace: kuberay-system}
type: kubernetes.io/tls
data:
  tls.crt: $webhook_cert
  tls.key: $webhook_key
---
apiVersion: v1
kind: Service
metadata: {name: kuberay-webhook-service, namespace: kuberay-system}
spec:
  selector: {app: kuberay-operator}
  ports:
  - {name: https, port: 443, targetPort: 9443}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: kuberay-operator, namespace: kuberay-system}
spec:
  replicas: 1
  selector:
    matchLabels: {app: kuberay-operator}
  template:
    metadata:
      labels: {app: kuberay-operator}
    spec:
      serviceAccountName: kuberay-operator
      containers:
      - name: manager
        image: $OPERATOR_IMAGE
        imagePullPolicy: Never
        args: ["--feature-gates=RayFederation=true,RayServiceIncrementalUpgrade=false", "--enable-leader-election=false"]
        env:
        - {name: ENABLE_WEBHOOKS, value: "true"}
        ports:
        - {name: webhook, containerPort: 9443}
        volumeMounts:
        - {name: webhook-cert, mountPath: /tmp/k8s-webhook-server/serving-certs, readOnly: true}
        resources:
          requests: {cpu: 200m, memory: 256Mi}
          limits: {memory: 1Gi}
        readinessProbe:
          httpGet: {path: /readyz, port: 8082}
          initialDelaySeconds: 3
          periodSeconds: 3
      volumes:
      - name: webhook-cert
        secret: {secretName: kuberay-webhook-cert}
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata: {name: kuberay-federation-validating-webhook}
webhooks:
- name: vfederatedraycluster.kb.io
  admissionReviewVersions: [v1]
  sideEffects: None
  failurePolicy: Fail
  timeoutSeconds: 5
  clientConfig:
    caBundle: $webhook_cert
    service:
      name: kuberay-webhook-service
      namespace: kuberay-system
      path: /validate-ray-io-v1-federatedraycluster
      port: 443
  rules:
  - apiGroups: [ray.io]
    apiVersions: [v1]
    operations: [CREATE, UPDATE]
    resources: [federatedrayclusters]
    scope: Namespaced
EOF
for side in primary member; do
  kind load docker-image "$RAY_IMAGE" "$OPERATOR_IMAGE" --name "$KIND_PREFIX-$side"
  kubectl --kubeconfig "$STATE_DIR/$side.kubeconfig" apply --server-side -f "$OPERATOR_ROOT/config/crd/bases"
  kubectl --kubeconfig "$STATE_DIR/$side.kubeconfig" apply -f "$OPERATOR_ROOT/config/rbac/role.yaml"
  kubectl --kubeconfig "$STATE_DIR/$side.kubeconfig" apply -f "$STATE_DIR/operator.yaml"
  kubectl --kubeconfig "$STATE_DIR/$side.kubeconfig" -n kuberay-system rollout restart deployment/kuberay-operator
  kubectl --kubeconfig "$STATE_DIR/$side.kubeconfig" -n kuberay-system rollout status deployment/kuberay-operator --timeout=90s
done
python3 "$OPERATOR_ROOT/test/federation/placement_kind_e2e.py" \
  --kubeconfig "$STATE_DIR/primary.kubeconfig" --image "$RAY_IMAGE" --artifacts "$STATE_DIR/placement-artifacts" |
  tee "$STATE_DIR/placement-kind-e2e.log"
python3 "$OPERATOR_ROOT/test/federation/kind_e2e.py" \
  --primary-kubeconfig "$STATE_DIR/primary.kubeconfig" \
  --member-kubeconfig "$STATE_DIR/member.kubeconfig" \
  --member-node "$KIND_PREFIX-member-control-plane" --image "$RAY_IMAGE" --artifacts "$STATE_DIR/artifacts" |
  tee "$STATE_DIR/kind-e2e.log"
python3 "$OPERATOR_ROOT/test/federation/standalone_kind_e2e.py" \
  --primary-kubeconfig "$STATE_DIR/primary.kubeconfig" \
  --member-kubeconfig "$STATE_DIR/member.kubeconfig" \
  --image "$RAY_IMAGE" --artifacts "$STATE_DIR/standalone-artifacts" |
  tee "$STATE_DIR/standalone-kind-e2e.log"
echo "Artifacts: $STATE_DIR/artifacts. Test clusters are retained for inspection."
