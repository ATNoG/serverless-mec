# K3s + Knative ITS/CAM Event Pipeline

A Kubernetes-native pipeline to **capture ETSI ITS CAM packets**, turn them into **CloudEvents**, and route them to **Knative** consumers (e.g., loggers, MQTT). Includes optional utilities for normalizing PCAPs and ETSI-compliant CRDs for a MEC orchestrator.

---

## Overview

This repo contains:

- A **live capture** component (containerized) that sniffs CAM frames (L2 GeoNetworking `0x8947` or `udp/2001`), writes **NDJSON** for auditing, and posts **CloudEvents** (`type=its.cam`) to a Knative **Broker**. It can add **CloudEvent extensions** like `stationtype` for fine-grained routing.
- Knative **Triggers** and utility Services (e.g., `event_display`) to filter/inspect traffic, with examples like “only `stationtype=5`”.
- An optional **MQTT forwarder** (HTTP CloudEvent → MQTT topic) and a **Mosquitto** broker Deployment/Service for downstream consumers.
- Optional **ETSI MEC 010-1 CRDs** to model MEC resources (MobileEdgeApplication, TrafficRule, DNSRule).

---

## Architecture (high-level)

1. **Network capture (DaemonSet)** on each node, running with `hostNetwork`, parses CAM and posts CloudEvents to a **Kafka-backed Knative Broker**. A **SinkBinding** injects `K_SINK` so the capture knows where to POST.  
2. **Triggers** route by attributes (e.g., `type=its.cam`, `stationtype=5`) to subscribers (Knative Services), such as a logger or MQTT forwarder.  
3. **Optional MQTT path:** a Knative Service receives the CloudEvent and republishes to MQTT topics derived from event attributes (e.g., `its/{type}/st{stationtype}`).

---

## Prerequisites

- A working **K3s** (or Kubernetes) cluster and `kubectl`.

---

## Quickstart (cluster setup → first events)

### 1) Install Knative (Serving/Eventing) + Kourier + default domain
Follow the [Knative YAML install flow](https://knative.dev/docs/install/yaml-install/).

### 2) Add Kafka (Strimzi) and Knative Kafka components
Deploy Strimzi (namespace `kafka`) and install the Knative **KafkaChannel** and **Kafka Broker** data planes. Again, follow the [Knative Documentation](https://knative.dev/docs/install/yaml-install/eventing/install-eventing-with-yaml/).

### 3) Create a Kafka-backed Broker
`kafkaBroker.yaml`:
```yaml
apiVersion: eventing.knative.dev/v1
kind: Broker
metadata:
  name: default
  namespace: default
  annotations:
    eventing.knative.dev/broker.class: Kafka
spec:
  config:
    apiVersion: v1
    kind: ConfigMap
    name: kafka-broker-config
    namespace: knative-eventing
```
Apply: `kubectl apply -f brokers/kafkaBroker.yaml`.

### 4) Deploy the live capture as a **DaemonSet**
Example manifest `live-capture-ds.yaml` is provided in the repo; apply and ensure pods are Ready.

### 5) Bind the DaemonSet to the Broker (inject `K_SINK`)
Create a **SinkBinding** targeting the DaemonSet (Subject = the DS), Sink = the `default` Broker. **Rollout restart** the DS so new pods get `K_SINK`.

### 6) Add a logger Service + Trigger
A simple `event_display` Knative Service is perfect for verifying delivery; add a Trigger that routes `type=its.cam` to it.

Example Service (`knativeServices/eventDisplayService.yaml`):
```yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: cam-logger
  namespace: default
spec:
  template:
    spec:
      containers:
      - image: gcr.io/knative-releases/knative.dev/eventing/cmd/event_display
        ports:
        - containerPort: 8080
```

Example Trigger (`triggers/camLoggerTrigger.yaml`):
```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: cam-to-logger
  namespace: default
spec:
  broker: default
  filter:
    attributes:
      type: its.cam
  subscriber:
    ref:
      apiVersion: serving.knative.dev/v1
      kind: Service
      name: cam-logger
```

### 7) (Optional) Install MQTT broker + forwarder
- Deploy **Mosquitto** (ConfigMap + Deployment + Service).  
- Deploy the **MQTT forwarder** Knative Service (`/` accepts CloudEvents; republishes to MQTT).  
- Add a Trigger to route `its.cam` to the forwarder.

---

## Testing the pipeline

1) Start a **PCAP replayer** Pod (hostNetwork) and install `tcpreplay`. Copy a test PCAP/PCAPNG into `/tmp`.  
2) If your capture is **802.11+radiotap** with LLC/SNAP, normalize it to **Ethernet 0x8947** using the provided Scapy script, then replay the normalized file:  
   ```
   python3 /tmp/normalize_to_eth8947.py /tmp/cam.pcap /tmp/cam_eth8947.pcap
   tcpreplay --intf1=eth0 /tmp/cam_eth8947.pcap
   ```  
3) Watch the **logger**:  
   ```
   kubectl logs -f -l serving.knative.dev/service=cam-logger -c user-container
   ```
   Or tail the sniffer’s NDJSON:  
   ```
   kubectl exec -it deploy/live-capture -- sh -lc 'tail -f /var/log/cam.ndjson'
   ```  
4) (Optional) Subscribe to **MQTT** topics:
   ```
   mosquitto_sub -h mosquitto.default.svc.cluster.local -p 1883 -t 'its/#' -v
   ```

---

## Configuration (capture container)

Key env/args the capture understands (examples; align with your container code):

- `IFACE` (default: `eth0`)  
- `BPF` – e.g., `ether proto 0x8947` or `udp port 2001`  
- `K_SINK` – injected by SinkBinding, the HTTP endpoint of the Broker  
- `CE_TYPE` (default: `its.cam`)  
- `LOG_EVERY` (progress logging cadence)  
- `INCLUDE_RAW_HEX` (`1/true` to record frame hex into NDJSON)  
- May promote `cam_fields.stationtype` to a CloudEvent **extension** (`stationtype`) to enable attribute-based routing and MQTT topic templating.

---

## Example: route only `stationtype=5`

Create a Trigger that matches the extension attribute and send it to a dedicated logger:
Example (`triggers/cam-stationtype-5-trigger.yaml`):
```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: cam-stationtype-5
  namespace: default
spec:
  broker: default
  filters:
    - exact:
        type: its.cam
        stationtype: "5"
  subscriber:
    ref:
      apiVersion: serving.knative.dev/v1
      kind: Service
      name: cam-stationtype-5-logger
```

Use `event_display` as the subscriber (with minScale=1 while testing).

---

## Troubleshooting

- **Pods ready?** `kubectl get pods -A` and `kubectl describe` to inspect events; check container logs.  
- **No events at subscribers?** Confirm the DaemonSet pods have `K_SINK` (restart DS after changing SinkBinding):  
  ```
  kubectl exec -it $(kubectl get pod -l app=live-capture -o name | head -n1) -- printenv K_SINK
  ```  
- **Wrong BPF filter?** Switch to `ether proto 0x8947`.
- **Accessing the cluster externally?** Use Kourier + default domain as in the guide; ensure **MetalLB** advertises a reachable IP for the Kourier Service.

---

## Local Tips

- Running `kubectl` from your laptop? Copy the K3s kubeconfig and adjust the `server:` endpoint from `127.0.0.1` to your node IP.  

---

## License

This project is licensed under the **GNU General Public License v3.0**

---

## Acknowledgments

This pipeline stands on **Knative**, **Strimzi/Kafka**, **PyShark/TShark**, and **CloudEvents SDK**; the docs and examples above trace the project’s evolution from a single Deployment to a cluster-wide DaemonSet, richer event attributes, and MQTT integrations.
