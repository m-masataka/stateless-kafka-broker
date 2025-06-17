# Stateless Kafka Broker

A minimal, stateless-compatible Kafka broker implementation in Rust.

## 🚀 Overview

`stateless-kafka-broker` is an experimental Kafka-compatible broker built with the following design goals:

- Stateless (no internal storage or metadata persistence)
- Lightweight and easy to deploy
- Implements essential Kafka protocol handling (e.g., `InitProducerId`, etc.)
- Designed for use in ephemeral or serverless environments

This project is ideal for exploring how Kafka protocol works, building custom broker-side logic, or embedding minimal Kafka functionality in controlled environments.

## ✨ Features

- Stateless by design: no Zookeeper or KRaft dependency
- Basic request handling: currently supports `InitProducerIdRequest`
- Extensible message handling framework
- Written in pure async Rust using `tokio`

## 📦 Usage

```bash
# Clone the repo
git clone https://github.com/m-masataka/stateless-kafka-broker
cd stateless-kafka-broker

# Run the broker
cargo run
```
By default, it listens on port 9092 and handles a subset of Kafka protocol requests.

⚠️ Note: This broker is not suitable for production use. It is designed for learning, testing, and minimal simulation purposes.

## 🧩 Architecture

The core idea behind `stateless-kafka-broker` is to provide a lightweight, pluggable Kafka-compatible broker, where state persistence and log storage are **externalized and modularized**.

```
   +-----------------------------+
   |      Kafka Client          |
   +-----------------------------+
             │
     Kafka Protocol (TCP)
             │
   +-----------------------------+
   |  Stateless Kafka Broker     |
   |-----------------------------|
   |  - Request Handlers         |
   |  - Protocol Parser          |
   |  - Response Encoder         |
   |                             |
   |     +-------------------+   |
   |     |    meta_store     |◄─────────────────────────────┐
   |     |-------------------|   |                          |
   |     |  - producer_id    |   |                          |
   |     |  - txn_epoch      |   |       Backend Options:   |
   |     +-------------------+   |     - File               |
   |                             |     - SQLite / SQL       |
   |     +-------------------+   |     - Redis              |
   |     |    log_store      |◄─────────────────────────────┘
   |     |-------------------|         (pluggable)         
   |     |  - Produce Logs   |
   |     |  - Fetch Logs     |
   |     +-------------------+
   +-----------------------------+
```


### 🔧 Pluggable Backends

#### `meta_store`
Handles assignment and tracking of:
- `producer_id`
- `producer_epoch`
- transactional metadata (in the future)

**Backends (planned/pluggable):**
- File-based JSON
- SQLite
- Redis (ephemeral + centralized)

#### `log_store`
Manages message data for `ProduceRequest` / `FetchRequest`.

**Backends (planned/pluggable):**
- Local file storage
- Amazon S3 / GCS (object storage)
- In-memory (for simulation)

---

## 🧱 Modularity Goals

- No hard-coded state logic: everything is injectable
- Component abstraction traits: `MetaStore`, `LogStore`
- Easy to mock and test
- Serverless/federated deployment ready

---

## 📌 Example Use Cases

| Environment        | Meta Store   | Log Store  |
|--------------------|--------------|------------|
| Local dev          | File         | File       |
| Stateless edge app | Redis        | S3         |
| SQLite-backed PoC  | SQLite       | Local FS   |

