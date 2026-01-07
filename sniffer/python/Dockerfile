# syntax=docker/dockerfile:1
FROM python:3.11-alpine

RUN apk add --no-cache \
      libpcap \
      libstdc++ \
      ca-certificates \
      build-base linux-headers libpcap-dev

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
  && apk del build-base linux-headers libpcap-dev

COPY sniffer.py .

ENV IFACE=eth0 \
    BPF="" \
    PROMISCUOUS=0 \
    SEND_QUEUE_MAX=20000 \
    SENDER_WORKERS=4 \
    MEASURE_POST=1 \
    INCLUDE_TIME=0 \
    PCAP_TIMEOUT_MS=1

ENTRYPOINT ["python", "sniffer.py"]
