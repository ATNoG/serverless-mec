# syntax=docker/dockerfile:1

FROM alpine:3.20 AS build
RUN apk add --no-cache build-base curl-dev
WORKDIR /src
COPY sniffer_tshark.c .
RUN mkdir -p /out && gcc -O2 -Wall -Wextra -pthread sniffer_tshark.c -lcurl -o /out/sniffer_tshark

FROM alpine:3.20

# tshark lives in community on Alpine
RUN grep -q '/community' /etc/apk/repositories || \
    echo "https://dl-cdn.alpinelinux.org/alpine/v3.20/community" >> /etc/apk/repositories

RUN apk add --no-cache tshark libcurl ca-certificates

COPY --from=build /out/sniffer_tshark /usr/local/bin/sniffer_tshark

ENV IFACE=eth0 \
    BPF="(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001" \
    LOG_EVERY=50 \
    STDOUT_NDJSON=1 \
    PROMISCUOUS=0 \
    SEND_QUEUE_MAX=1000

ENTRYPOINT ["/usr/local/bin/sniffer_tshark"]
