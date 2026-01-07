# syntax=docker/dockerfile:1

FROM alpine:3.20 AS build
RUN apk add --no-cache build-base libpcap-dev curl-dev linux-headers
WORKDIR /src
COPY sniffer.c .
RUN mkdir -p /out && gcc -O2 -Wall -Wextra -pthread sniffer.c -lpcap -lcurl -o /out/sniffer

FROM alpine:3.20
RUN apk add --no-cache libpcap libcurl ca-certificates
COPY --from=build /out/sniffer /usr/local/bin/sniffer

ENV IFACE=eth0 \
    BPF="(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001" \
    LOG_EVERY=50 \
    STDOUT_NDJSON=1 \
    PROMISCUOUS=0 \
    SEND_QUEUE_MAX=1000

ENTRYPOINT ["/usr/local/bin/sniffer"]
