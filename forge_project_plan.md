# Forge — Project Plan

## What it does

Forge is a distributed job queue backed by Redis. Producers push jobs into a queue; a pool of concurrent workers pulls jobs off the queue and executes them. If a job fails, it's automatically retried with exponential backoff; if it keeps failing past a max-attempts limit, it's moved to a dead-letter queue instead of being retried forever. If jobs arrive faster than workers can process them, the queue applies backpressure so producers slow down instead of piling up unbounded work in Redis.

This is a backend infra primitive, not an end-user app — think of it as the piece you'd drop into a system that needs to run background work (sending emails, processing uploads, calling flaky third-party APIs) without losing jobs or melting down under load.

**Who would use this.** Any backend system that needs to do work asynchronously and can't afford to lose that work: sending emails/notifications, resizing images or transcoding video after upload, processing payment or webhook callbacks from a third party, running an ETL step. The pattern — accept the request fast, do the real work later in the background, retry on failure — is the same role tools like Sidekiq (Ruby), Celery (Python), BullMQ (Node), or AWS SQS plus a worker fleet play in production systems. Forge is a small, from-scratch version of that same idea.

## Background & Concepts

A quick primer on the vocabulary and technology this project rests on, for anyone reading this plan without a backend/infra background.

**Redis.** An in-memory data store — a very fast key-value database that mostly lives in RAM instead of on disk. Because it skips slow disk access for most operations, it can handle huge numbers of reads/writes per second. It's commonly used as a cache, a session store, or — as here — a message broker connecting different parts of a system.

**Redis data structures.** Unlike a plain key-value store where every value is just a string, Redis gives you several built-in structures per key: strings, lists, sets, sorted sets (ZSETs), and hashes. Forge uses two: a **list** (an ordered sequence supporting push/pop from either end — used for `ready`, `processing`, and `dead`) and a **sorted set / ZSET** (a set where every member has a numeric score and Redis keeps it sorted by that score — used for `delayed`, scored by "when should this job retry").

**Go.** A programming language built by Google — compiled (not interpreted, unlike Python), statically typed, and designed specifically to make writing concurrent, networked backend services easy and fast. It's the language behind a lot of infrastructure software (Docker, Kubernetes, Redis itself) largely because of how well it handles concurrency.

**Concurrency.** Multiple tasks being *in progress* at the same time, even if they're not literally executing at the exact same instant (that's parallelism — a related but distinct idea; concurrency is about structure, parallelism is about simultaneous execution across CPU cores). A worker pool is a concurrency pattern: multiple workers independently pulling and processing jobs so the system makes progress on many jobs at once.

**Goroutines.** Go's built-in unit of concurrent execution — start one by putting `go` in front of a function call, and it runs independently alongside everything else. They're cheap compared to OS threads: a real OS thread typically reserves a megabyte or two of memory and the kernel context-switches between them at real cost; a goroutine starts with a stack of only a couple kilobytes that grows as needed, and the Go runtime (not the OS) schedules many goroutines onto a small pool of OS threads. That's why a Go program can comfortably run thousands of goroutines — in Forge, each of the N workers is one goroutine.

**Background poller goroutine.** Most goroutines in Forge are reactive — a worker blocks waiting for a job, then acts. The poller is different: it loops forever, waking every `POLL_INTERVAL_MS` to check whether any jobs in the `delayed` set are now due, moving them back into `ready` if so. Nothing external triggers it; it's what quietly makes retries fire on schedule.

**Why producers `LPUSH` but workers `BLMOVE`, not a plain pop.** `LPUSH` adds an item to the *left* (head) of a Redis list. A plain `RPOP`/`BRPOP` (blocking pop) would remove from the *right* (tail) — push-left/pop-right gives FIFO order. A worker could just `BRPOP` to grab a job, but the instant it's popped, that job's only copy is gone from Redis and lives only in the worker's memory — if the worker crashes mid-job, the job is lost. `BLMOVE ready processing RIGHT LEFT timeout` avoids this: it atomically pops from the tail of `ready` *and* pushes that same item onto the head of `processing`, in one indivisible step, so the job never disappears from Redis entirely — it just moves from "waiting" to "claimed." If the worker dies mid-job, the job is still sitting safely in `processing` (a fuller system would add a "reaper" to notice stale entries there and requeue them — noted as future work below). To be clear: this push isn't new work being added, and it isn't triggered by failure — it's relocation-for-durability. The failure-driven push (into `delayed` or `dead`) is a separate step that happens afterward, only if the job's handler returns an error. (`BLMOVE` is the modern, non-deprecated form of this — older code/tutorials use `BRPOPLPUSH ready processing timeout`, which does the exact same thing but bakes the direction into the command name instead of taking it as an argument. Both work; `BLMOVE` is what current Redis docs recommend, so that's what Forge uses.)

**OS-level concepts underneath this.** A **process** is a running program with its own isolated memory space. A **thread** is a unit of execution *within* a process; multiple threads in one process share that process's memory, and the OS kernel schedules threads onto physical CPU cores. Goroutines aren't OS threads — they're a lighter abstraction the Go runtime manages itself, then multiplexes onto a handful of real OS threads under the hood, giving thread-like concurrency without the overhead of creating thousands of real OS threads. A **blocking call** pauses the calling goroutine until it completes — `BLMOVE` blocks until either a job shows up or a timeout hits, which is why a worker can just loop calling it without wasting CPU spinning. An **atomic operation** is one other concurrent clients always see as either fully done or not-started, never half-finished — the property that makes `BLMOVE` safe with multiple workers hitting the same Redis instance at once, since two workers can never grab the same job.

**Handlers are supplied by you, not Forge.** Forge never inspects or branches on what's inside a job's payload — it treats it as an opaque blob. What actually runs a job is a handler function (`func(payload []byte) error`) that you pass in when constructing the worker pool; the pool's whole job is to call that function and act on whether it returns an error. This keeps Forge generic (it works the same whether the payload is an email job or an image-resize job) but also means one Forge queue runs one job type per handler — routing multiple job types through a single queue would need an extra dispatch layer on top, which is out of scope here.

## Inputs

There are two kinds of input: what a producer gives the system per job, and how the whole service is configured at startup.

**Per-job input** — a producer calls `Enqueue` with:

| Field | Type | Required | Notes |
|---|---|---|---|
| `payload` | any JSON-serializable value | yes | The actual work data (e.g. `{"to": "a@b.com", "template": "welcome"}`) |
| `maxAttempts` | int | no (default 5) | How many total tries before the job is dead-lettered |

Everything else (ID, timestamps, attempt count) is generated by Forge itself.

**Service configuration** — read from environment variables at startup:

| Env var | Default | Meaning |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Redis connection address |
| `QUEUE_NAME` | `default` | Namespaces the Redis keys, lets multiple queues share one Redis instance |
| `CONCURRENCY` | `10` | Number of worker goroutines running in parallel |
| `MAX_READY` | `1000` | Backpressure ceiling — producers block once this many jobs are waiting |
| `BASE_BACKOFF_MS` | `500` | Starting delay for retry backoff, doubles each attempt |
| `MAX_BACKOFF_MS` | `30000` | Cap on backoff delay so retries don't drift out for hours |
| `POLL_INTERVAL_MS` | `500` | How often the dispatcher checks for delayed jobs that are now due for retry |

## Output / observable behavior

Each job ends in one of three states: acknowledged (succeeded), retried (failed, requeued for a later attempt), or dead-lettered (failed past `maxAttempts`, parked for manual inspection). The demo CLI logs every state transition to stdout; the dead-letter list and queue depth are queryable directly in Redis at any time.

**Delivery guarantee: at-least-once, not exactly-once.** Because of the reliable-queue pattern (a job is only removed from `processing` *after* the handler returns), a job can run more than once — e.g. if a worker crashes after the handler succeeds but before the `Ack` call removes it from `processing`. Forge trades "might run twice" for "never silently lost," which is the right tradeoff for a job queue, but it means **handlers must be idempotent** (safe to run more than once with the same payload).

## Architecture

```
Producer → [ready list] → Worker pool (N goroutines)
                              │
                    success ──┤── Ack (remove from processing)
                              │
                    failure ──┼── attempts < max → [delayed zset] → (poller) → back to [ready list]
                              │
                              └── attempts ≥ max → [dead-letter list]
```

Five Redis structures per queue name, all prefixed `forge:{queue}:`:

- `ready` (LIST) — jobs waiting to be picked up. Producers `LPUSH`, workers `BLMOVE` into `processing`.
- `processing` (LIST) — jobs currently claimed by a worker. This is the "reliable queue" pattern: if a worker crashes mid-job, the job isn't lost, it's just sitting here (a future improvement would be a reaper that requeues stale entries).
- `delayed` (ZSET) — jobs waiting out a retry backoff, scored by the Unix timestamp they become eligible again.
- `dead` (LIST) — jobs that exhausted their retries, kept for inspection/replay. Each entry carries a `LastError` field (the error message from the final failed attempt) so a dead job is actually diagnosable, not just a payload with no explanation.
- job payloads are JSON-encoded and stored as the list/set member itself — no separate hash lookup needed.

## Core mechanics

**Concurrency** is just N worker goroutines, each running its own blocking dequeue loop. N *is* the concurrency limit — no separate semaphore needed since each goroutine only ever holds one job at a time.

**Graceful shutdown**: each worker's loop calls `BLMOVE` with a short timeout (a couple seconds) rather than blocking forever, and checks `ctx.Done()` between calls. This means stopping the pool (e.g. Ctrl+C on the demo) is just cancelling that context — every worker notices within one timeout window and returns instead of hanging on an open blocking call indefinitely.

**Retries with exponential backoff**: on failure, attempt count increments and the job goes into the `delayed` ZSET with score `now + min(base * 2^attempt, cap)`. A single background poller goroutine periodically moves due jobs from `delayed` back into `ready`.

**Dead-letter queue**: once `attempts >= maxAttempts`, the job goes to `dead` instead of `delayed`, with its `LastError` field set to the final failure's error message. It's never silently dropped, and never dropped without a reason attached.

**Backpressure**: `Enqueue` checks `LLEN ready` before pushing. If it's at or above `MAX_READY`, the producer sleeps for `POLL_INTERVAL_MS` and rechecks (respecting context cancellation) instead of pushing — reusing the same interval as the delayed-job poller rather than a busy-loop, so a burst of upstream work can't grow the queue unboundedly or hammer Redis with `LLEN` calls while waiting.

## File layout

```
forge/
  go.mod
  README.md
  cmd/forge/main.go        — demo entrypoint: wires everything, runs a sample producer + workers
  internal/job/job.go      — Job struct, JSON (de)serialization
  internal/queue/queue.go  — Enqueue, Dequeue, Ack, Retry, PromoteDelayed, dead-letter ops
  internal/worker/pool.go  — worker goroutine pool, panic-safe handler execution
```

`go.mod` is Go's dependency manifest — it declares the module's import path and which external packages (like the Redis client) it depends on, similar to `package.json` in Node. `cmd/forge/main.go` is the executable entrypoint; Go convention is that anything meant to run as a program lives under `cmd/<binary-name>/`. `internal/` is a Go-enforced convention: any package under a directory named `internal` can only be imported by code within this same module — how you mark packages as implementation detail rather than a public API, appropriate here since `queue` and `worker` aren't meant to be imported by other projects. Within `internal/`, each subdirectory is a package with one job: `job` defines the data shape, `queue` owns all Redis interaction, `worker` owns orchestrating goroutines around whatever `queue` provides. Splitting it this way means Redis could be swapped for something else later by only touching the `queue` package.

## Is it doable in 3 hours?

Yes, for this scope (single-node Redis, no auth/TLS, no persistence beyond what Redis itself gives you, no metrics/tracing). Rough breakdown:

| Time | Task |
|---|---|
| 0:00–0:20 | Scaffold module, `Job` struct, JSON encode/decode |
| 0:20–1:00 | Queue core: `Enqueue`/`Dequeue`/`Ack` using `BLMOVE` reliable-queue pattern |
| 1:00–1:30 | Worker pool: N goroutines, panic recovery, wiring to a handler function |
| 1:30–2:10 | Retry + exponential backoff via delayed ZSET + poller goroutine |
| 2:10–2:35 | Dead-letter queue path |
| 2:35–2:55 | Backpressure check in `Enqueue` |
| 2:55–3:00 | Demo producer + smoke test against local Redis |

What's realistically **out of scope** for 3 hours and would need more time: crash recovery for jobs stuck in `processing` (a "reaper"), Redis Cluster/sentinel support, per-job priorities, metrics/observability, and tests beyond a manual smoke run.

## How you'll verify it

I can't compile or run Go in this sandbox (no network access to fetch the Go toolchain or Redis here), so the code is written to be correct by inspection, but you'll need to build/run it locally:

```
cd forge
go mod tidy          # fetches go-redis and writes go.sum
redis-server &        # or: docker run -p 6379:6379 redis
go run ./cmd/forge
```
