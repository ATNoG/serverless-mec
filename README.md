# serverless-mec

A Kubernetes-native MEC (Multi-access Edge Computing) orchestrator that uses **Knative** and **CloudEvents** to deploy and manage edge applications on K3s clusters. Developed as part of a Master's dissertation exploring serverless alternatives to traditional NFV-based MEC platforms (OSM/MANO).

## Overview

This project implements the application lifecycle management layer of an ETSI MEC platform using Kubernetes-native primitives instead of heavyweight NFV orchestrators. It covers:

- An **EdgeApplication CRD** aligned with ETSI MEC 010-2, with a vendor-specific extension that maps to Knative Services and Triggers
- A **Kubernetes operator** that reconciles EdgeApplications into Knative Services, Triggers, and per-node replicas
- An **ITS packet capture pipeline** that sniffs ETSI ITS CAM/DENM frames from RSU network interfaces, converts them to CloudEvents, and routes them through a Kafka-backed Knative Broker
- **CRIU-based container freezing** to checkpoint idle serverless functions and restore them on demand, freeing RAM on resource-constrained edge nodes
- **Application handoff** between edge nodes via the EdgeApplicationHandoff CRD

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  RSU / Edge Node                                                    │
│                                                                     │
│  ┌─────────────┐    CloudEvents     ┌──────────────────────┐       │
│  │ ITS Sniffer  │──────────────────►│  Kafka-backed Broker  │       │
│  │ (DaemonSet)  │   type=its.cam    │  (Knative Eventing)   │       │
│  │ hostNetwork  │   type=its.denm   └──────────┬───────────┘       │
│  └─────────────┘                               │                    │
│                                          Knative Triggers           │
│                                       (attribute filtering)         │
│                                                │                    │
│                        ┌───────────────────────┼──────────┐        │
│                        ▼                       ▼          ▼        │
│                 ┌─────────────┐    ┌──────────────┐  ┌────────┐   │
│                 │Retransmitter│    │  CAM Logger   │  │  MQTT  │   │
│                 │  (KService) │    │  (KService)   │  │Forwarder│  │
│                 └──────┬──────┘    └──────────────┘  └────────┘   │
│                        │                                            │
│                        ▼                                            │
│             ┌─────────────────────┐                                 │
│             │ Queue-Proxy Plugin  │  idle timeout → freeze          │
│             │ (freezer plugin)    │  new request  → thaw            │
│             └─────────┬───────────┘                                 │
│                       │ HTTP                                        │
│                       ▼                                             │
│             ┌─────────────────────┐                                 │
│             │ Freeze Daemon       │  CRIU checkpoint/restore        │
│             │ (DaemonSet)         │  via containerd                 │
│             └─────────────────────┘                                 │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  MEC Operator (runs in cluster)                                     │
│                                                                     │
│  EdgeApplication CR ──► Knative Service(s) + Trigger(s)            │
│  AutoReplicas      ──► one KService per matching node              │
│  Handoff CR        ──► migrate app instance between nodes          │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

### EdgeApplication Operator (`operator/`)

A Go operator (Kubebuilder) that reconciles `EdgeApplication` custom resources into Knative primitives.

**EdgeApplication CRD fields:**
- **ETSI MEC 010-2 descriptor fields:** `dId`, `name`, `provider`, `softVersion`, `dVersion`, `infoName`, `description`
- **Vendor-specific service block:** container image, env, resources, securityContext, volumes, nodeSelector, affinity, tolerations, hostNetwork
- **`triggerFilters`:** list of CloudEvent attribute maps. Each entry creates a Knative Trigger pointing to the service
- **`replicas`:** additional named instances on specific nodes
- **`autoReplicas`:** daemon mode. Automatically creates one KService per node matching a label selector (e.g., `road-rsu: "true"`)

**EdgeApplicationHandoff CRD:** manages migration of application instances between edge nodes.

Example EdgeApplication:
```yaml
apiVersion: mec.atnog.org/v1alpha1
kind: EdgeApplication
metadata:
  name: its-sniffer
spec:
  dId: "its-sniffer-appd-v1"
  name: "its-sniffer"
  provider: "atnog.mec"
  softVersion: "1.0.0"
  dVersion: "1.0.0"
  service:
    container:
      image: ghcr.io/pmacoutinho/its-sniffer:latest
      securityContext:
        capabilities:
          add: ["NET_ADMIN", "NET_RAW"]
        runAsUser: 0
    hostNetwork: true
    dnsPolicy: ClusterFirstWithHostNet
    triggerFilters:
      - type: its.cam
      - type: its.denm
    autoReplicas:
      - matchNodes:
          road-rsu: "true"
```

### ITS Packet Capture (`sniffer/`)

A C-based ITS packet sniffer and producer that processes all packet layers in sequence (GeoNetworking, BTP, CAM/DENM). Uses raw sockets for capture with no external dependencies (no tshark/pyshark needed), making it suitable for resource-constrained RSU nodes.

Components:
- **`its_sniffer`** — captures GeoNetworking frames (`ether proto 0x8947`) from the network interface, decodes the full ITS stack, and posts CloudEvents to the Knative Broker
- **`its_producer`** — generates synthetic CAM/DENM packets for testing and benchmarking
- **`cloudevent_sender`** — shared CloudEvent HTTP posting library
- **`logger`** — shared logging utilities

The sniffer runs as a DaemonSet with `hostNetwork: true` and `NET_ADMIN`/`NET_RAW` capabilities. Events are posted to a Kafka-backed Knative Broker via `K_SINK` (injected by SinkBinding).

### CRIU Container Freezing

Two companion repositories handle checkpoint/restore of idle containers:

- **[container-freezer-criu](https://github.com/pmacoutinho/container-freezer-criu)** — DaemonSet that performs CRIU checkpoint (freeze) and restore (thaw) via containerd. Dumps full process state to disk, frees RAM, restores in ~641ms vs ~5258ms cold start (8.2x speedup).
- **[knative-freezer-plugin](https://github.com/pmacoutinho/knative-freezer-plugin)** — Custom queue-proxy that detects idle containers (30s default) and triggers freeze/thaw automatically. Thaws transparently on incoming requests.

Both are included as **git submodules** in this repo.

Additionally, `kyverno/inject-restart-policy-never.yaml` provides a Kyverno ClusterPolicy that injects `restartPolicy: Never` on Knative user containers to prevent kubelet from restarting containers after CRIU checkpoint kills them.

### Knative Services (`knativeServices/`)

- **Event Display** — simple CloudEvent logger for debugging
- **MQTT Forwarder** — receives CloudEvents via HTTP and republishes to MQTT topics (e.g., `its/{type}/st{stationtype}`)
- **Retransmitter** — forwards events between brokers/hops for multi-tier edge architectures and benchmarking

### Event Routing

- **Brokers** (`brokers/`) — Kafka-backed Knative Broker (Strimzi) and optional Mosquitto MQTT broker
- **Triggers** (`triggers/`) — attribute-based routing examples (e.g., `type=its.cam`, `stationtype=5`)
- **SinkBinding** (`sinkBinding/`) — injects `K_SINK` into sniffer DaemonSets

## Prerequisites

- **K3s** (or Kubernetes) cluster with `kubectl` access
- **Knative Serving + Eventing** installed ([install guide](https://knative.dev/docs/install/yaml-install/))
- **Strimzi** (Kafka) for the Knative Kafka Broker
- **Kyverno** (if using CRIU container freezing)
- **CRIU** installed on worker nodes (if using container freezing, amd64 only)

## Quick Start

### 1. Enable Knative features for edge workloads

The operator needs PodSpec features (hostNetwork, capabilities, nodeSelector, etc.) enabled in Knative:

```bash
./scripts/enable-knative-features.sh
```

### 2. Deploy the Kafka-backed Broker

```bash
kubectl apply -f configMaps/kafka-broker-config.yaml
kubectl apply -f brokers/kafkaBroker.yaml
```

### 3. Deploy the operator

```bash
kubectl apply -f configMaps/mec-operator-config.yaml
cd operator && make deploy
```

### 4. Deploy an EdgeApplication

```bash
kubectl apply -f edgeApplications/its-sniffer.yaml
```

The operator will create:
- One Knative Service per matching node (autoReplicas mode)
- Knative Triggers for `its.cam` and `its.denm` events
- Pods with hostNetwork and NET_ADMIN/NET_RAW capabilities

### 5. Verify events are flowing

```bash
# Deploy a logger
kubectl apply -f edgeApplications/cam-logger-operator.yaml

# Watch logs
kubectl logs -f -l serving.knative.dev/service=cam-logger-operator -c user-container
```

### 6. (Optional) Enable CRIU container freezing

```bash
# Initialize submodules
git submodule update --init --recursive

# Deploy freeze daemon (see container-freezer-criu README)
cd container-freezer && kubectl apply -f config/common/ && kubectl apply -f config/containerd/300-daemon-containerd.yaml

# Deploy Kyverno policy
kubectl apply -f kyverno/inject-restart-policy-never.yaml

# Deploy custom queue-proxy (see knative-freezer-plugin README)
cd knative-freezer-plugin && ./build.sh && ./patch.sh
```

### 7. (Optional) MQTT integration

```bash
kubectl apply -f brokers/mosquitto-broker.yaml
kubectl apply -f knativeServices/forwarder/forwarder.yaml
kubectl apply -f triggers/forward-all-trigger.yaml
```

## Testing without Live ITS Traffic

If you don't have live ITS traffic, you can generate synthetic packets using the **producer** included in the `sniffer/` folder. It runs natively on ARM64 RSU nodes and produces CAM/DENM packets directly on the network interface:

```bash
# Deploy the producer as an EdgeApplication
kubectl apply -f edgeApplications/its-producer.yaml
```

Alternatively, replay captured PCAP files:

```bash
# Deploy a replayer pod on a node with hostNetwork
kubectl apply -f helpers/pcap-replayer.yaml

# If captures are 802.11+radiotap, normalize to Ethernet 0x8947 first
python3 helpers/normalize_to_eth8947.py input.pcap output.pcap

# Replay
kubectl exec -it pcap-replayer -- tcpreplay --intf1=eth0 /tmp/output.pcap
```

## Benchmarking

Latency benchmarks correlate sniffer and retransmitter NDJSON logs:

```bash
python3 benchmarks/analyze_bench.py
```

CRIU benchmark (cold start vs checkpoint/restore, 50 iterations):
```bash
cd container-freezer && ./benchmark-integration.sh 50
```

## Project Structure

```
operator/                  # EdgeApplication + Handoff operator (Kubebuilder)
  api/v1alpha1/            #   CRD type definitions
  internal/controller/     #   Reconciliation logic
  config/                  #   Kustomize manifests (RBAC, CRD, manager)
sniffer/                   # ITS packet sniffer and producer (C, raw sockets)
  src/                     #   Source code (sniffer, producer, CloudEvent sender, logger)
knativeServices/           # Knative Service definitions
  forwarder/               #   MQTT forwarder
  retransmitter/           #   Event retransmitter (benchmarking)
edgeApplications/          # EdgeApplication CR examples
triggers/                  # Knative Trigger examples
brokers/                   # Kafka + Mosquitto broker manifests
configMaps/                # Operator + broker configuration
sinkBinding/               # SinkBinding for DaemonSet → Broker
handoffs/                  # EdgeApplicationHandoff CR examples
helpers/                   # PCAP normalization, replay pods
scripts/                   # Cluster setup scripts
kyverno/                   # Kyverno policies (restartPolicy injection)
benchmarks/                # Latency analysis scripts
container-freezer/         # [submodule] CRIU checkpoint/restore daemon
knative-freezer-plugin/    # [submodule] Queue-proxy freezer plugin
```

## Related Repositories

- [container-freezer-criu](https://github.com/pmacoutinho/container-freezer-criu) — CRIU checkpoint/restore daemon for Knative containers
- [knative-freezer-plugin](https://github.com/pmacoutinho/knative-freezer-plugin) — Queue-proxy plugin for automatic idle freeze/thaw

## License

This project is licensed under the **GNU General Public License v3.0**.
