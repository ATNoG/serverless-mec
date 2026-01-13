# syntax=docker/dockerfile:1

FROM alpine:3.20 AS build
RUN apk add --no-cache build-base libpcap-dev curl-dev linux-headers
WORKDIR /src
COPY sniffer_libpcap.c .
RUN mkdir -p /out && gcc -O2 -Wall -Wextra -pthread sniffer_libpcap.c -lpcap -lcurl -o /out/sniffer_libpcap

FROM alpine:3.20
RUN apk add --no-cache libpcap libcurl ca-certificates
COPY --from=build /out/sniffer_libpcap /usr/local/bin/sniffer_libpcap

ENV IFACE=eth0 \
    BPF="(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001" \
    LOG_EVERY=50 \
    STDOUT_NDJSON=1 \
    PROMISCUOUS=0 \
    SEND_QUEUE_MAX=1000

ENTRYPOINT ["/usr/local/bin/sniffer_libpcap"]
