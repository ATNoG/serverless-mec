#!/bin/bash
set -e

REGISTRY=ghcr.io/pmacoutinho
TAG=${1:-latest}

kubectl patch configmap config-deployment -n knative-serving \
  --type merge \
  -p "{\"data\":{\"queue-sidecar-image\":\"${REGISTRY}/freezer-queue-proxy:${TAG}\"}}"

kubectl patch configmap config-features -n knative-serving \
  --type merge \
  -p '{"data":{"queueproxy.mount-podinfo":"enabled"}}'

echo "Done. Restart existing serving pods to pick up the new queue-proxy:"
echo "  kubectl rollout restart deployment -n <your-namespace>"
