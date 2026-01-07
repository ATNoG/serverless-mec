FROM rust:1-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs
RUN cargo fetch

COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wireshark-common \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/its-live-capture-rs /usr/local/bin/its-live-capture-rs
ENTRYPOINT ["/usr/local/bin/its-live-capture-rs"]
