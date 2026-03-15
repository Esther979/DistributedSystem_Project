FROM rust:1.83 as builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    bash \
    curl \
    jq \
    python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/kv-node /app/kv-node
COPY bridge.py /app/bridge.py

RUN chmod +x /app/kv-node /app/bridge.py

CMD ["python3", "/app/bridge.py"]