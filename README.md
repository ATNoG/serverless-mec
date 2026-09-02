# serverless-mec

A Kubernetes-native MEC (Multi-access Edge Computing) orchestrator that uses **Knative** and **CloudEvents** to deploy and manage edge applications on K3s clusters. Developed as part of a Master's dissertation exploring serverless alternatives to traditional NFV-based MEC platforms (OSM/MANO).

## Overview

This project implements the application lifecycle management layer of an ETSI MEC platform using Kubernetes-native primitives instead of heavyweight NFV orchestrators. It covers:

- An **EdgeApplication CRD** aligned with ETSI MEC 010-2, with a vendor-specific extension that maps to Knative Services and Triggers
- A **Kubernetes operator** that reconciles EdgeApplications into Knative Services, Triggers, and per-node replicas
- An **ITS packet capture pipeline** that sniffs ETSI ITS CAM/DENM frames from RSU network interfaces, converts them to CloudEvents, and routes them through a Kafka-backed Knative Broker
- **CRIU-based container checkpoint/restore** to freeze idle serverless functions and restore them on demand, freeing RAM on both x86 and ARM64 edge nodes, with per-node capability detection so freeze is enabled only where CRIU is available (degrading gracefully to cold starts elsewhere)
- **Application handoff** (make-before-break) between edge nodes via the EdgeApplicationHandoff CRD

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  RSU / Edge Node                                                    │
│                                                                     │
│  ┌─────────────┐    CloudEvents      ┌─────────────────────┐        │
│  │ ITS Sniffer │────────────────────►│ Kafka-backed Broker │        │
│  │ (DaemonSet) │    type=its.cam     │ (Knative Eventing)  │        │
│  │ hostNetwork │    type=its.denm    └──────────┬──────────┘        │
│  └─────────────┘                                │                   │
│                                         Knative Triggers            │
│                                      (attribute filtering)          │
│                                                 │                   │
│                ┌──────────────────┬─────────────┴────┐              │
│                ▼                  ▼                  ▼              │
│        ┌───────────────┐   ┌─────────────┐   ┌───────────────┐      │
│        │ Retransmitter │   │ CAM Logger  │   │MQTT Forwarder │      │
│        │  (KService)   │   │ (KService)  │   │  (KService)   │      │
│        └───────┬───────┘   └─────────────┘   └───────────────┘      │
│                │                                                    │
│                ▼                                                    │
│        ┌───────────────────────┐                                    │
│        │  Queue-Proxy Plugin   │  idle timeout → freeze             │
│        │   (freezer plugin)    │  new request  → thaw               │
│        └───────────┬───────────┘                                    │
│                    │ HTTP                                           │
│                    ▼                                                │
│        ┌───────────────────────┐                                    │
│        │     Freeze Daemon     │  CRIU checkpoint/restore           │
│        │      (DaemonSet)      │  via containerd                    │
│        └───────────────────────┘                                    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  MEC Operator (runs in cluster)                                     │
│                                                                     │
│  EdgeApplication CR ──► Knative Service(s) + Trigger(s)             │
│  Zones             ──► one KService per matching node               │
│  Handoff CR        ──► migrate app instance between nodes           │
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
- **`zones`:** daemon mode. Each zone is a node label selector (e.g., `road-rsu: "true"`); the operator creates one KService per node matching the zone's `matchNodes`
- **`freezeEnabled`:** enables CRIU checkpoint/restore idling for the service (via the knative-freezer-plugin). Freeze is only activated on nodes the freeze daemon has labeled checkpoint-capable; otherwise the service falls back to standard scale-to-zero cold starts
- **`freezeIdleTimeout`:** seconds of inactivity before a container is checkpointed (plugin default: 30s), applied as the `qpoption.knative.dev/freezer-idle-timeout` annotation

**EdgeApplicationHandoff CRD:** manages make-before-break migration of application instances between edge nodes. The target is deployed and checkpointed in place; on handoff the target is restored while the source is retained until the target is Ready.

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
  zones:
    - matchNodes:
        road-rsu: "true"
```

### ITS Packet Capture (`sniffer/`)

A C-based ITS packet sniffer and producer that processes all packet layers in sequence (GeoNetworking, BTP, CAM/DENM). Uses raw sockets for capture with no external dependencies (no tshark/pyshark needed), making it suitable for resource-constrained RSU nodes.

Components:
- **`its_sniffer`**: captures GeoNetworking frames (`ether proto 0x8947`) from the network interface, decodes the full ITS stack, and posts CloudEvents to the Knative Broker
- **`its_producer`**: generates synthetic CAM/DENM packets for testing and benchmarking
- **`cloudevent_sender`**: shared CloudEvent HTTP posting library
- **`logger`**: shared logging utilities

The sniffer runs as a DaemonSet with `hostNetwork: true` and `NET_ADMIN`/`NET_RAW` capabilities. Events are posted to a Kafka-backed Knative Broker via `K_SINK` (injected by SinkBinding).

### CRIU Container Freezing

Two companion repositories handle checkpoint/restore of idle containers:

- **[container-freezer](https://github.com/ATNoG/container-freezer)**: DaemonSet that performs CRIU checkpoint (freeze) and restore (thaw) via containerd on both x86 and ARM64 nodes (on arm64 it checkpoints to a filesystem path to bypass the overlay/xattr limits of older kernels). It also **self-labels each node** with `mec.atnog.org/checkpoint-support` (probing the kernel and the host's CRIU binary) so the operator enables freeze only where it works. On x86 VMs, restore takes ~731ms vs ~5978ms cold start (8.2x); on ARM64 RSUs, ~10.9s vs ~25.6s (2.3x).
- **[knative-freezer-plugin](https://github.com/ATNoG/knative-freezer-plugin)**: Custom queue-proxy that checkpoints idle containers (default 30s, configurable per-app via `freezeIdleTimeout`) and thaws them transparently on the next request.

Both are included as **git submodules** in this repo.

Additionally, `kyverno/inject-restart-policy-never.yaml` provides a Kyverno ClusterPolicy that injects `restartPolicy: Never` on freeze-enabled Knative user containers, preventing kubelet from restarting them after CRIU checkpoint kills them. (The idle timeout is now a first-class CRD field, `freezeIdleTimeout`, rather than a Kyverno policy.)

### Knative Services (`knativeServices/`)

- **Event Display**: simple CloudEvent logger for debugging
- **MQTT Forwarder**: receives CloudEvents via HTTP and republishes to MQTT topics (e.g., `its/{type}/st{stationtype}`)
- **Retransmitter**: forwards events between brokers/hops for multi-tier edge architectures and benchmarking

### Event Routing

- **Brokers** (`brokers/`): Kafka-backed Knative Broker (Strimzi) and optional Mosquitto MQTT broker
- **Triggers** (`triggers/`): attribute-based routing examples (e.g., `type=its.cam`, `stationtype=5`)
- **SinkBinding** (`sinkBinding/`): injects `K_SINK` into sniffer DaemonSets

## Prerequisites

- **K3s** (or Kubernetes) cluster with `kubectl` access
- **Knative Serving + Eventing** installed ([install guide](https://knative.dev/docs/install/yaml-install/))
- **Strimzi** (Kafka) for the Knative Kafka Broker
- **Kyverno** (if using CRIU container freezing, to inject `restartPolicy: Never`)
- **CRIU** installed on worker nodes that should support freezing (x86 and ARM64; the freeze daemon auto-detects per-node support and labels nodes accordingly)

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
- One Knative Service per matching node (zones / daemon mode)
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

The `benchmarks/` folder contains the checkpoint/restore, migration, and pipeline benchmarks:

```bash
# Checkpoint/restore vs cold start
./benchmarks/bench_freeze_vs_coldstart.sh        # x86 VMs
./benchmarks/bench_fvc_rsu.sh                     # ARM64 RSUs

# Migration (handoff) latency, e.g. RSU freeze scenario
./benchmarks/bench_handoff.sh --scenario rsu-freeze --iterations 20

# End-to-end ITS pipeline latency (sniffer -> broker -> retransmitter)
./benchmarks/capture_pipeline_bench.sh
```

Each script writes NDJSON logs to a `*_logs/` folder. The `analyze_*.py` scripts
summarize a run, and `generate_plots.py` renders the evaluation figures. (The
logs and generated figures are gitignored and not tracked.)

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

- [container-freezer](https://github.com/ATNoG/container-freezer): CRIU checkpoint/restore daemon for Knative containers (per-node capability detection, x86 + ARM64)
- [knative-freezer-plugin](https://github.com/ATNoG/knative-freezer-plugin): Queue-proxy plugin for automatic idle freeze/thaw

## License

This project is licensed under the **GNU General Public License v3.0**.
