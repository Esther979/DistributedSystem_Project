FROM rust:1.83 as builder
# WORKDIR /app 就是把容器内的“当前目录”切到 /app，后续相对路径操作都默认在这个目录下进行。
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