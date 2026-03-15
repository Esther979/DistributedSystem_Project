FROM rust:latest as builder
WORKDIR /app
RUN apt-get update && apt-get install -y clang libclang-dev
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin server

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
    openssh-server \
    iproute2 \
    iptables \
    sudo \
    && rm -rf /var/lib/apt/lists/*

RUN echo 'root:root' | chpasswd \
 && sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
 && mkdir -p /var/run/sshd

COPY --from=builder /app/target/release/server /usr/local/bin/server

WORKDIR /app
RUN mkdir -p /app/logs

EXPOSE 22 8000 8080

CMD bash -c "/usr/sbin/sshd && exec /usr/local/bin/server"
