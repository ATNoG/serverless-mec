# syntax=docker/dockerfile:1
FROM python:3.11-alpine

# Install tshark (CLI version of Wireshark) + tcpdump + libcap
RUN apk add --no-cache tshark tcpdump libcap \
 && setcap 'CAP_NET_RAW+eip CAP_NET_ADMIN+eip' /usr/bin/dumpcap

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sniffer.py .

ENV IFACE=eth0 \
    BPF="ether proto 0x8947" \
    LOG_EVERY=50

ENTRYPOINT ["python", "sniffer.py"]
