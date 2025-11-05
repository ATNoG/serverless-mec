# syntax=docker/dockerfile:1
FROM python:3.11-alpine

# tshark (tshark is provided by wireshark-cli on Alpine) + tcpdump
RUN apk add --no-cache wireshark-cli tcpdump

# If running non-root and still capturing:
RUN apk add --no-cache libcap && setcap 'CAP_NET_RAW+eip CAP_NET_ADMIN+eip' /usr/bin/dumpcap

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV IFACE=eth0 \
    BPF="ether proto 0x8947" \
    LOG_EVERY=50

ENTRYPOINT ["python", "main.py"]
