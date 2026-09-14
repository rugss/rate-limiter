# Resilient Distributed Event Ingestion & Processing Engine

A production-grade, asynchronous event ingestion and processing pipeline built with **Node.js**, designed to handle bursty, high-throughput traffic while remaining resilient to dependency failures.

The system combines an **atomic Redis Lua sliding-window rate limiter**, a **finite-state-machine circuit breaker**, **BullMQ background workers**, **PostgreSQL transactional persistence**, **end-to-end idempotency**, and a **Dead-Letter Queue (DLQ)** with administrative replay capabilities.

---

## Architecture Overview

```text
                    ┌──────────────────────┐
                    │  HTTP Client /        │
                    │  Webhook              │
                    └──────────┬───────────┘
                               │
                               ▼
             ┌────────────────────────────────────┐
             │       API Gateway (Express.js)     │
             │                                    │
             │  ├── Atomic Lua Rate Limiter       │
             │  └── FSM Circuit Breaker            │
             │      └── In-Memory Fallback         │
             └────────────────┬───────────────────┘
                              │
                              │ Fast ACK
                              │ HTTP 202
                              ▼
             ┌────────────────────────────────────┐
             │       BullMQ Buffer (Redis)        │
             └────────────────┬───────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
          ┌─────────────────┐  ┌─────────────────┐
          │   Worker Node 1 │  │   Worker Node 2 │
          └────────┬────────┘  └────────┬────────┘
                   │                    │
                   │ At-Least-Once      │
                   │ + Idempotent       │
                   └─────────┬──────────┘
                             │
                             ▼
             ┌────────────────────────────────────┐
             │       PostgreSQL Storage            │
             │                                    │
             │  UNIQUE idempotency_key            │
             │  ON CONFLICT DO NOTHING             │
             └────────────────┬───────────────────┘
                              │
                              │ Max Retries Exhausted
                              ▼
             ┌────────────────────────────────────┐
             │     Dead-Letter Queue (DLQ)        │
             │                                    │
             │  ├── Failed Jobs                   │
             │  ├── Stack Traces                  │
             │  └── Admin Replay                  │
             └────────────────────────────────────┘
```

---

## Core Engineering Highlights

### 1. Atomic Sliding-Window Rate Limiter

The API gateway uses **Redis Sorted Sets (ZSETs)** together with a **raw Lua script** to implement a distributed sliding-window rate limiter.

The entire rate-limit operation executes atomically inside Redis:

1. Remove expired requests.
2. Count requests inside the current window.
3. Check whether the request is within the quota.
4. Add the new request timestamp.
5. Return the decision.

Because these operations execute atomically, multiple distributed gateway instances cannot perform conflicting check-then-act operations.

This eliminates race conditions and prevents **quota leaks** under high concurrency.

---

### 2. Resilient FSM Circuit Breaker

Redis is a critical dependency for the distributed rate limiter. To prevent Redis failures from taking down the API gateway, the system implements a **3-state finite-state-machine circuit breaker**:

```text
             Failures
                │
                ▼
        ┌───────────────┐
        │    CLOSED     │
        │ Normal Redis  │
        │    traffic    │
        └───────┬───────┘
                │
                │ Failure threshold reached
                ▼
        ┌───────────────┐
        │     OPEN      │
        │ Redis bypassed│
        │ In-memory mode│
        └───────┬───────┘
                │
                │ Recovery timeout
                ▼
        ┌───────────────┐
        │   HALF-OPEN   │
        │ Test Redis    │
        │   dependency  │
        └───────┬───────┘
                │
        ┌───────┴────────┐
        │                │
      Success          Failure
        │                │
        ▼                ▼
     CLOSED             OPEN
```

When Redis becomes unavailable:

* The circuit breaker transitions from `CLOSED` → `OPEN`.
* Requests are diverted to an **in-memory sliding-window limiter**.
* The gateway remains available instead of failing because of the Redis outage.
* After the recovery timeout, the breaker transitions to `HALF-OPEN`.
* A probe request tests Redis.
* If Redis has recovered, the breaker returns to `CLOSED`.

This provides graceful degradation during cache or network failures.

---

### 3. Fast-ACK Event Ingestion

Incoming events are not processed synchronously by the API server.

Instead:

```text
HTTP Request
     │
     ▼
Validate Event
     │
     ▼
Rate Limit
     │
     ▼
Add Job to BullMQ
     │
     ▼
HTTP 202 Accepted
```

The API gateway quickly places the event into the BullMQ queue and returns:

```http
202 Accepted
```

This decouples incoming traffic bursts from database processing and prevents slow database operations from blocking HTTP requests.

---

### 4. Dual-Layer Idempotency

The system provides idempotency at two different layers.

#### Queue-Level Idempotency

A deterministic `jobId` is generated from the event's idempotency key.

For example:

```text
idempotencyKey = order_tx_98124
```

Results in:

```text
jobId = order_tx_98124
```

BullMQ can therefore reject duplicate submissions before multiple copies enter the processing pipeline.

#### Database-Level Idempotency

The PostgreSQL table uses a `UNIQUE` constraint on the `idempotency_key`.

Database writes use:

```sql
ON CONFLICT DO NOTHING
```

This protects against duplicate writes even when a job is delivered more than once.

This is important because BullMQ workers operate under an **at-least-once delivery model**.

---

### 5. Fault Tolerance & Dead-Letter Queue

Failed jobs are automatically retried using exponential backoff.

Current retry delays:

```text
Attempt 1 → 1 second
Attempt 2 → 2 seconds
Attempt 3 → 4 seconds
```

If a job continues to fail after the maximum number of attempts, it is moved to the **Dead-Letter Queue (DLQ)**.

The DLQ retains useful debugging information such as:

* Job ID
* Event ID
* Event type
* Payload
* Failure reason
* Stack trace
* Number of attempts

Administrators can then inspect, replay, or permanently remove failed jobs.

---

# Empirical Load Benchmarks (Autocannon)

The ingestion gateway was evaluated using **Autocannon** across varying levels of concurrent socket pressure.

The benchmark compares the performance of:

* **Distributed Redis Mode** — rate limiting backed by Redis and executed through the network.
* **In-Memory Fallback Engine** — rate limiting performed locally inside the Node.js process when Redis is unavailable.

The tests demonstrate the performance trade-off between **distributed consistency** and **local execution speed**, while also validating quota enforcement and resilience under increasing socket pressure.

---

## Standard Microservice Concurrency (`-c 100`, 10s duration)

This benchmark represents a more typical microservice workload with **100 concurrent client connections**.

| Metric                  | Distributed Redis Mode                    | In-Memory Fallback Engine                   | Analysis                                                    |
| :---------------------- | :---------------------------------------- | :------------------------------------------ | :---------------------------------------------------------- |
| **Total Requests**      | 89,235 reqs                               | 153,636 reqs                                | +72% request capacity via in-process memory                 |
| **Throughput (Avg)**    | 8,924.8 req/sec                           | 13,967.6 req/sec                            | ~56% throughput boost by skipping network/TCP serialization |
| **Latency (Avg / p99)** | 10.71 ms / 20 ms                          | 6.63 ms / 11 ms                             | Lower latency from local in-process execution               |
| **Quota Integrity**     | 5 allowed (`2xx`), 89,230 blocked (`429`) | 10 allowed (`2xx`), 153,626 blocked (`429`) | **0 race conditions / 0 leaks**                             |
| **Timeouts / Errors**   | 0                                         | 0                                           | Clean execution with zero socket drops                      |

The in-memory engine processes substantially more requests because rate-limit decisions do not require a network round trip to Redis.

The Redis implementation, however, provides the distributed state required when multiple gateway instances need to share a common quota.

---

## High Socket Saturation (`-c 1,000`, 10s duration)

This benchmark increases pressure to **1,000 concurrent client connections**, testing how both implementations behave under substantial socket contention.

| Metric                  | Distributed Redis Mode                    | In-Memory Fallback Engine                   | Analysis                                                                  |
| :---------------------- | :---------------------------------------- | :------------------------------------------ | :------------------------------------------------------------------------ |
| **Total Requests**      | 89,400 reqs                               | 161,103 reqs                                | Local memory absorbs high-density socket load                             |
| **Throughput (Avg)**    | 8,940.4 req/sec                           | 14,646.9 req/sec                            | In-memory maintains peak ~14.6k req/sec                                   |
| **Latency (Avg / p99)** | 111.31 ms / 294 ms                        | 67.87 ms / 88 ms                            | Redis experiences additional socket queue wait; in-memory p99 stays <90ms |
| **Quota Integrity**     | 5 allowed (`2xx`), 89,395 blocked (`429`) | 10 allowed (`2xx`), 161,093 blocked (`429`) | **0 leaks** under 1,000 active client connections                         |
| **Timeouts / Errors**   | 0                                         | 0                                           | 100% availability preserved across both layers                            |

The benchmark shows the increasing latency cost of distributed Redis operations as socket contention rises.

The local fallback avoids the network hop and therefore maintains substantially lower p99 latency.

---

## Extreme Node.js Process Saturation (`-c 10,000`, 10s duration)

The final benchmark pushes the Node.js process to **10,000 concurrent connections**, intentionally creating extreme event-loop and socket pressure.

| Metric                  | Distributed Redis Mode | In-Memory Fallback Engine | Analysis                                                                |
| :---------------------- | :--------------------- | :------------------------ | :---------------------------------------------------------------------- |
| **Total Requests**      | 76,112 reqs            | 127,141 reqs              | Single-threaded V8 event-loop contention                                |
| **Throughput (Avg)**    | 7,611.6 req/sec        | 12,714.8 req/sec          | Fallback engine sustains 12.7k req/sec                                  |
| **Latency (Avg / p99)** | 1,183.62 ms / 3,133 ms | 737.24 ms / 2,172 ms      | Identifies event-loop timer drift under raw socket load                 |
| **Timeouts**            | 1,000 timeouts         | 772 timeouts              | Establishes the threshold for upstream reverse-proxy connection pooling |

At 10,000 concurrent connections, both implementations begin to encounter significant event-loop and socket contention.

This benchmark is intentionally beyond normal microservice operating conditions and is useful for identifying the practical limits of a single Node.js process.

The results also demonstrate that the in-memory fallback continues to outperform the Redis-backed implementation under extreme local process pressure, although neither mode remains completely free of timeouts at this concurrency level.

---

## Benchmark Summary

Across the tested concurrency levels, the results demonstrate three important characteristics:

### Distributed Redis Mode

```text
Advantages
├── Shared state across multiple gateway instances
├── Strong distributed quota enforcement
├── Atomic Lua execution
└── Suitable for horizontally scaled deployments

Trade-off
└── Network/TCP overhead introduces additional latency
```

### In-Memory Fallback Engine

```text
Advantages
├── No network round trip
├── Higher single-process throughput
├── Lower latency
└── Keeps gateway operational during Redis failures

Trade-off
└── State is local to the individual gateway process
```

The fallback engine is therefore not intended to replace Redis during normal distributed operation. Instead, it provides a **resilience mechanism** that allows the gateway to continue accepting traffic when the distributed Redis dependency becomes unavailable.

---

# Getting Started

## Prerequisites

Make sure the following are installed:

* Node.js `18+`
* Docker
* Docker Compose
* npm

---

## 1. Clone the Repository

```bash
git clone <your-repository-url>
cd <project-directory>
```

---

## 2. Start Infrastructure

### Redis

```bash
docker run -d \
  --name redis-rate-limiter \
  -p 6379:6379 \
  redis:alpine
```

### PostgreSQL

```bash
docker run -d \
  --name event-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=events_db \
  -p 5432:5432 \
  postgres:alpine
```

Verify that both containers are running:

```bash
docker ps
```

---

## 3. Environment Configuration

Create a `.env` file in the project root:

```env
PORT=5000

REDIS_HOST=127.0.0.1
REDIS_PORT=6379

PG_HOST=127.0.0.1
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=postgres
PG_DATABASE=events_db
```

> **Note:** Do not commit `.env` to Git. Add it to `.gitignore`.

---

## 4. Install Dependencies

```bash
npm install
```

---

## 5. Run the Application

Start the API gateway and worker in development mode:

```bash
npm run dev
```

If the worker is configured to run separately, start it in another terminal:

```bash
node src/queue/eventWorker.js
```

This allows the worker to operate as an isolated background processing service.

---

# API Reference

## Event Ingestion

### `POST /api/v1/events`

Accepts an event and places it into the asynchronous processing pipeline.

The endpoint is protected by the atomic Redis sliding-window rate limiter.

### Request Headers

```http
Content-Type: application/json
```

### Request Body

```json
{
  "type": "PAYMENT_CAPTURE",
  "idempotencyKey": "order_tx_98124",
  "payload": {
    "userId": "usr_99",
    "amount": 149.50,
    "currency": "USD"
  }
}
```

### Response

```http
HTTP/1.1 202 Accepted
```

```json
{
  "status": "queued",
  "eventId": "order_tx_98124",
  "jobId": "order_tx_98124"
}
```

The `202 Accepted` response indicates that the event has been accepted for asynchronous processing rather than processed synchronously during the HTTP request.

---

# Dead-Letter Queue API

## Inspect Failed Jobs

### `GET /api/v1/dlq`

Returns failed jobs currently stored in the Dead-Letter Queue.

Typical information includes:

* Job ID
* Event ID
* Event type
* Payload
* Failure reason
* Stack trace
* Attempt count

---

## Replay a Failed Job

### `POST /api/v1/dlq/:jobId/replay`

Re-enqueues a failed job back into the active processing lifecycle.

Example:

```bash
curl -X POST http://localhost:5000/api/v1/dlq/<jobId>/replay
```

---

## Permanently Remove a Failed Job

### `DELETE /api/v1/dlq/:jobId`

Permanently removes an irrecoverable job from the DLQ.

Example:

```bash
curl -X DELETE http://localhost:5000/api/v1/dlq/<jobId>
```

---

# Event Processing Lifecycle

A typical event follows this lifecycle:

```text
1. Client sends event
          │
          ▼
2. API validates request
          │
          ▼
3. Rate limiter checks quota
          │
          ▼
4. Idempotency key is evaluated
          │
          ▼
5. Job is added to BullMQ
          │
          ▼
6. API returns HTTP 202
          │
          ▼
7. Worker picks up job
          │
          ▼
8. Event is processed
          │
          ▼
9. PostgreSQL transaction executes
          │
          ▼
10. Event stored using idempotency protection
          │
          ▼
       SUCCESS
```

If processing fails:

```text
Worker
  │
  ▼
Processing Failure
  │
  ▼
Exponential Backoff
  │
  ▼
Retry
  │
  ├── Success ──► Complete
  │
  └── Failure
        │
        ▼
   Retry Exhausted
        │
        ▼
       DLQ
```

---

# Stress Testing

Run the automated Autocannon benchmark:

```bash
npm run benchmark
```

The benchmark can be used to evaluate:

* Request throughput
* Rate-limit enforcement
* Concurrent connection handling
* API latency
* Race conditions
* Redis vs. in-memory performance
* Gateway resilience

---

# Circuit Breaker Failure Simulation

The circuit breaker can be tested by intentionally making Redis unavailable while traffic is active.

### 1. Start the application

```bash
npm run dev
```

### 2. Start the benchmark

In another terminal:

```bash
npm run benchmark
```

### 3. Pause Redis

While traffic is running:

```bash
docker pause redis-rate-limiter
```

The circuit breaker should detect the Redis failure and transition:

```text
CLOSED
   │
   ▼
OPEN
```

Traffic should then be handled by the in-memory fallback rate limiter.

### 4. Restore Redis

```bash
docker unpause redis-rate-limiter
```

The circuit breaker should eventually transition:

```text
OPEN
  │
  ▼
HALF-OPEN
  │
  │ Redis probe succeeds
  ▼
CLOSED
```

This verifies that the system can recover from a Redis outage without requiring a restart of the API gateway.

---

# Reliability Model

The system is designed around several important distributed-systems principles.

### At-Least-Once Processing

A worker may process the same job more than once due to retries or delivery semantics.

Therefore, processing must be idempotent.

### Idempotent Persistence

PostgreSQL protects against duplicate event writes through:

```sql
UNIQUE (idempotency_key)
```

combined with:

```sql
ON CONFLICT DO NOTHING
```

### Asynchronous Decoupling

The API gateway does not wait for database processing to complete.

Instead:

```text
Client → API → Queue → Worker → Database
```

This allows the API layer to absorb traffic bursts without directly coupling request latency to database processing time.

### Graceful Degradation

Redis failures do not immediately make the API unavailable.

The circuit breaker switches the gateway to an in-memory rate limiter until Redis becomes healthy again.

---

# Project Goals

This project demonstrates practical implementation of:

* Distributed rate limiting
* Redis Lua scripting
* Atomic operations
* Circuit breakers
* Finite-state machines
* Asynchronous job processing
* BullMQ workers
* At-least-once delivery
* Idempotent event processing
* PostgreSQL transactions
* Exponential backoff
* Dead-Letter Queues
* Administrative job replay
* Graceful degradation
* High-concurrency benchmarking
* Failure simulation

---

# Key Design Principle

The system separates **request acceptance** from **event processing**:

```text
             SYNCHRONOUS
                 │
                 ▼
        ┌─────────────────┐
        │   API Gateway   │
        └────────┬────────┘
                 │
                 ▼
          Fast 202 ACK
                 │
══════════════════════════════════
                 │
             ASYNCHRONOUS
                 │
                 ▼
        ┌─────────────────┐
        │    BullMQ       │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
        │     Worker      │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
        │   PostgreSQL    │
        └─────────────────┘
```

This architecture allows the ingestion layer to remain responsive even when downstream processing becomes slower or temporarily unavailable.

---

## License

This project is licensed under the MIT License. 