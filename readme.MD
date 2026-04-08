# High-Performance Go Ingestion Pipeline

## Overview
This project is a highly concurrent, resilient, and backpressure-aware event ingestion pipeline written in Go. It is built using **Hexagonal Architecture** (Ports and Adapters) to strictly decouple the core domain logic from external infrastructure (HTTP handlers, PostgreSQL, and 3rd-party APIs).

The pipeline is designed to absorb massive HTTP traffic spikes, asynchronously enrich data via external APIs with exponential backoff, and batch-insert the results into a database—all while guaranteeing **zero data loss** during rolling deployments or unexpected pod terminations.

## Core Architectural Patterns

* **Hexagonal Architecture:** The `domain.Event` remains pure. Infrastructure concerns are isolated to outer adapters.
* **Asynchronous Fire-and-Forget:** HTTP handlers immediately return `202 Accepted` after safely handing events off to the internal pipeline, preventing slow downstream APIs from holding HTTP connections hostage.
* **Fan-Out / Fan-In:** A single dispatcher routes traffic to a pool of concurrent `EnricherWorker` goroutines (Fan-Out), which then push enriched data to a single `batchInsert` worker (Fan-In) for optimized database writes.

## Advanced Go Concurrency Learnings

Building this pipeline required navigating several complex edge cases in Go's concurrency model:

### 1. Graceful Shutdown & The "Dying Breath" Pattern
* **Tiered Shutdowns:** Using a single global context for graceful shutdown causes a "Simultaneous Shutdown Deadlock" where workers try to flush data to channels that have already been closed by other shutting-down workers. **Solution:** Split the shutdown into tiers—stop HTTP ingress first, wait for in-flight requests to drain, and *then* signal the internal workers to exit.
* **The "Last Gasp" Context:** When a worker detects a shutdown (`<-ctx.Done()`), the parent context is already dead. To make one final network or channel call (like saving an event to a Dead Letter Queue), a fresh `context.WithTimeout(context.Background(), 1*time.Second)` must be created to give the operation a strict window to succeed.
* **Pseudo-Random Select:** If multiple channels in a `select` block are ready simultaneously, Go picks one at random. Relying on channel buffers during shutdown without strict context management results in a 50/50 coin toss of losing data.

### 2. Channel Safety & Backpressure
* **Naked Sends are Deadlocks:** Sending data to a channel (`myChan <- data`) without wrapping it in a `select` block that also listens for `ctx.Done()` is a critical risk. If the consumer on the other side crashes or shuts down, the producer will block forever, causing a goroutine leak.
* **True Backpressure:** Simply using `time.After` in a `select` block with a channel send does not create backoff; it creates a constant-pressure loop. True backpressure requires separating the send attempt (with a short timeout to handle micro-bursts) from the backoff sleep (yielding the CPU when the system is genuinely saturated).

### 3. Context Management & Timers
* **Context Immutability:** A `context.Context` cannot be reset. Once it times out, any standard library function (`http.Client`, `sql.DB`) that receives it will instantly fail. Retries must always generate a *fresh* context per attempt.
* **The Undead Context:** Using `context.WithoutCancel` blindly strips all cancellation awareness, making goroutines deaf to OS-level `SIGTERM` signals and preventing the application from shutting down.
* **Memory-Safe Batching:** Modern Go slice reuse (`clear(events)` followed by `events = events[:0]`) prevents the Garbage Collector from trashing memory during high-volume batch processing. 
* **Ticker Leaks:** `time.NewTicker` must always be paired with `defer ticker.Stop()` to prevent background timers from leaking when the goroutine exits. Ticker channels must also be actively drained if work takes longer than the tick interval to prevent stale phantom ticks.

## System Design: In-Memory Pipeline vs. Message Broker

While this in-memory pipeline acts similarly to an external message broker (like Kafka or RabbitMQ), it optimizes for different constraints:

**Why use this Go Pipeline?**
* **Ultra-Low Latency:** Nanosecond handoffs between stages in memory.
* **Cost & Simplicity:** Zero external infrastructure to deploy, monitor, or pay for.
* **Intra-Service Decoupling:** Perfect for isolating slow I/O (API calls) from fast I/O (HTTP ingestion) within a single bounded context.

**When to migrate to a Message Broker:**
* **Fan-Out Ratio > 1:** When multiple distinct microservices need to react to the exact same event.
* **OOM Kills:** If the channel buffers require so much memory during traffic spikes that the Kubernetes pod runs out of RAM.
* **Shutdown Thresholds:** If the "dying breath" pipeline drain consistently takes longer than Kubernetes' graceful termination grace period (e.g., >30 seconds), risking data destruction via `SIGKILL`.

## Project Structure (Hexagonal Architecture)

The codebase is strictly organized following Ports and Adapters to ensure the core domain remains completely agnostic of the transport layer (HTTP) and infrastructure (PostgreSQL, External APIs).

```text
.
├── cmd/
│   └── api/
│       └── main.go           # The Composition Root. Initializes contexts, wires adapters to ports, and orchestrates tiered graceful shutdown.
│
├── internal/
│   ├── config/               # Environment variables and application-wide configurations (e.g., WorkerPoolSize, BatchSize).
│   │
│   ├── core/                 # The absolute center of the hexagon. No external dependencies allowed here.
│   │   ├── domain/           # Core business entities (e.g., `Event` struct) and domain-specific errors.
│   │   ├── port/             # Interfaces defining what the core needs (e.g., `TxManager`, `DataEnricher`, `PipelineRepository`).
│   │   └── service/          # Business logic and use cases. Contains `PipelineService`, `EnricherWorker`, the dispatcher, and batching engines.
│   │
│   └── adapter/              # The outer ring. Implementations of the ports.
│       ├── handler/          # Driving Adapters (Left Side): HTTP REST handlers and routers.
│       ├── extapi/           # Driven Adapters (Right Side): External HTTP clients (e.g., IP Enrichment API).
│       ├── repository/       # Driven Adapters (Right Side): Concrete database logic and Unit of Work (UoW) implementation.
│       └── db/               # Infrastructure setup: PostgreSQL connection pooling and configuration.
│
├── go.mod
└── go.sum
```


## Development & Contributing

Before picking up a ticket or opening a Pull Request, all engineers **must** read and follow our development standards:

* 📖 **[Engineering Standard: Testing Workflow & SDLC](./TESTING.md)**: Learn how to write technical ACs, generate test skeletons, and pass our CI pipeline coverage gates.