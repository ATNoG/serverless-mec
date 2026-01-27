#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="knative-serving"
CONFIGMAP="config-features"

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

echo "Patching ${CONFIGMAP} in namespace ${NAMESPACE} to enable podspec features..."
kubectl -n "${NAMESPACE}" patch configmap "${CONFIGMAP}" \
  --type merge \
  -p '{
    "data": {
      "kubernetes.podspec-affinity": "enabled",
      "kubernetes.podspec-tolerations": "enabled",
      "kubernetes.podspec-nodeselector": "enabled",
      "kubernetes.podspec-topologyspreadconstraints": "enabled"
    }
  }'

echo "Done! Knative should now accept affinity, tolerations, and nodeSelector in Pod specs."
echo "   If things still look off, try restarting the Knative webhook pod:"
echo "     kubectl -n ${NAMESPACE} rollout restart deploy webhook"
