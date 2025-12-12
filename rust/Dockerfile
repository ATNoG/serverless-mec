# syntax=docker/dockerfile:1

########################
# Build stage
########################
# Use a Rust version >= 1.83 (1.84-alpine is fine)
FROM rust:1.84-alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig

WORKDIR /app

# Cache dependencies (no Cargo.lock needed)
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Build actual binary
COPY src ./src
RUN cargo build --release

########################
# Runtime stage
########################
FROM alpine:3.20

# Install tshark (Wireshark CLI), tcpdump, and libcap for capabilities
RUN apk add --no-cache tshark tcpdump libcap \
    && setcap 'CAP_NET_RAW+eip CAP_NET_ADMIN+eip' /usr/bin/dumpcap

WORKDIR /app

# Copy the Rust binary from the builder
COPY --from=builder /app/target/release/its_live_capture /usr/local/bin/sniffer

ENV IFACE=eth0 \
    BPF="ether proto 0x8947" \
    LOG_EVERY=50

ENTRYPOINT ["/usr/local/bin/sniffer"]
