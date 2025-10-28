# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends tshark tcpdump && \
    rm -rf /var/lib/apt/lists/*

# Note: we run as root in this PoC for simplicity.
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Default args can be overridden by k8s env
ENV IFACE=eth0 \
    BPF="ether proto 0x8947" \
    OUT_PATH=/var/log/cam.ndjson \
    LOG_EVERY=50

ENTRYPOINT ["python", "main.py"]
