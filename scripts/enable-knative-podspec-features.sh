#!/usr/bin/env bash
set -euo pipefail

# --- Knative settings ---
NAMESPACE="knative-serving"
CONFIGMAP="config-features"

# --- Operator RBAC settings ---
CLUSTERROLE="operator-manager-role"

echo "🔍 Checking that kubectl is available..."
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found in PATH. Please install kubectl and try again."
  exit 1
fi

echo "🔍 Checking that namespace '${NAMESPACE}' exists..."
if ! kubectl get ns "${NAMESPACE}" >/dev/null 2>&1; then
  echo "Namespace '${NAMESPACE}' not found. Is Knative Serving installed?"
  exit 1
fi

echo "🔍 Checking that ConfigMap '${CONFIGMAP}' exists in namespace '${NAMESPACE}'..."
if ! kubectl -n "${NAMESPACE}" get configmap "${CONFIGMAP}" >/dev/null 2>&1; then
  echo "ConfigMap '${CONFIGMAP}' not found in namespace '${NAMESPACE}'."
  echo "   This is usually created by Knative Serving as 'config-features'."
  exit 1
fi

echo "🔧 Patching ${CONFIGMAP} in namespace ${NAMESPACE} to enable podspec/container features..."
kubectl -n "${NAMESPACE}" patch configmap "${CONFIGMAP}" \
  --type merge \
  -p '{
    "data": {
      "kubernetes.podspec-affinity": "enabled",
      "kubernetes.podspec-tolerations": "enabled",
      "kubernetes.podspec-nodeselector": "enabled",
      "kubernetes.podspec-topologyspreadconstraints": "enabled",

      "kubernetes.podspec-hostnetwork": "enabled",
      "kubernetes.podspec-dnspolicy": "enabled",
      "kubernetes.podspec-volumes-emptydir": "enabled",
      "kubernetes.podspec-fieldref": "enabled",
      "kubernetes.podspec-securitycontext": "enabled",
      "kubernetes.containerspec-addcapabilities": "enabled"
    }
  }'

echo "🔍 Checking that ClusterRole '${CLUSTERROLE}' exists..."
if ! kubectl get clusterrole "${CLUSTERROLE}" >/dev/null 2>&1; then
  echo "ClusterRole '${CLUSTERROLE}' not found."
  echo "Available roles that look relevant:"
  kubectl get clusterrole | grep -E 'manager-role|operator' || true
  exit 1
fi

echo "🔧 Ensuring ClusterRole '${CLUSTERROLE}' can list/watch nodes..."
# Idempotent check: does a rule already exist granting get/list/watch on nodes?
if kubectl get clusterrole "${CLUSTERROLE}" -o jsonpath='{range .rules[*]}{.resources}{"|"}{.verbs}{"\n"}{end}' \
  | grep -q '\[nodes\].*\[get list watch\]'; then
  echo "✅ Node permissions already present on '${CLUSTERROLE}'."
else
  echo "➕ Adding node get/list/watch permissions to '${CLUSTERROLE}'..."
  kubectl patch clusterrole "${CLUSTERROLE}" --type='json' -p='[
    {"op":"add","path":"/rules/-","value":{"apiGroups":[""],"resources":["nodes"],"verbs":["get","list","watch"]}}
  ]'
  echo "✅ Patched '${CLUSTERROLE}'."
fi

echo
echo "Done! Knative should now accept the needed PodSpec features (hostNetwork, dnsPolicy, emptyDir, fieldRef, securityContext, addcapabilities, etc)."
echo "and your operator should be able to list/watch nodes."
echo
echo "If things still look off, try restarting the Knative webhook pod:"
echo "  kubectl -n ${NAMESPACE} rollout restart deploy/webhook"
echo
echo "And restart your operator controller manager:"
echo "  kubectl -n operator-system rollout restart deploy/operator-controller-manager"
