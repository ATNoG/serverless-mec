#syntax=docker/dockerfile:1
FROM alpine:3.20 AS build

RUN apk add --no-cache build-base linux-headers curl-dev

WORKDIR /src
COPY sniffer_rsock.c .

RUN mkdir -p /out && gcc -O2 -Wall -Wextra -pthread sniffer_rsock.c -lcurl -o /out/sniffer

FROM alpine:3.20
RUN apk add --no-cache libcurl ca-certificates

WORKDIR /app
COPY --from=build /out/sniffer /usr/local/bin/sniffer

ENV IFACE=cw-mon-rxa \
    LOG_EVERY=10 \
    CE_TYPE=its.cam \
    PROMISCUOUS=1 \
    STDOUT_NDJSON=0 \
    SEND_QUEUE_MAX=1000

ENTRYPOINT ["/usr/local/bin/sniffer"]
