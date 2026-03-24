#!/bin/bash
set -e

REGISTRY=ghcr.io/pmacoutinho
TAG=${1:-latest}

docker buildx build \
  --platform linux/arm64 \
  --builder mec-builder \
  -t ${REGISTRY}/freezer-queue-proxy:${TAG} \
  --push \
  .

echo "Pushed ${REGISTRY}/freezer-queue-proxy:${TAG}"
