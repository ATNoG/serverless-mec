# syntax=docker/dockerfile:1

########################
# Build stage
########################
FROM rust:1.84-alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig

WORKDIR /app

# Copy the real sources (no dummy-main caching trick)
COPY Cargo.toml ./
COPY Cargo.lock* ./
COPY src ./src

RUN cargo build --release

# Sanity check: your real binary must contain this string
RUN grep -a "its_live_capture starting up" /app/target/release/its_live_capture >/dev/null

########################
# Runtime stage
########################
FROM alpine:3.20

RUN apk add --no-cache tshark tcpdump libcap && \
    setcap cap_net_raw,cap_net_admin=eip /usr/bin/dumpcap || true

COPY --from=builder /app/target/release/its_live_capture /usr/local/bin/sniffer

ENV IFACE=eth0 \
    BPF="ether proto 0x8947" \
    LOG_EVERY=50

ENTRYPOINT ["/usr/local/bin/sniffer"]
