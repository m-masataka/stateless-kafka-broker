FROM rust:1.87 as builder

# install dependencies
RUN apt-get update && apt-get install -y pkg-config libssl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./

# Create a dummy source file to allow cargo fetch to work
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo fetch
RUN cargo build --release || true

COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/serverless_kafka_broker /usr/local/bin/server

EXPOSE 8080
WORKDIR /app

CMD ["server"]
