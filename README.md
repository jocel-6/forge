# Forge

A job queue built from scratch on top of Redis — mainly because I wanted to actually understand how something like Sidekiq or BullMQ works under the hood instead of just importing one and trusting it.

## What it does

You hand it a job — some JSON payload, could be "send this email," could be "resize this image" — and a pool of workers picks it up and runs it. If a job fails, Forge retries it automatically, waiting a little longer each time (exponential backoff). If it keeps failing past a limit, it gets parked in a dead-letter list instead of retried forever. And if jobs show up faster than workers can keep up, producers get slowed down on purpose instead of letting unbounded work pile up in Redis.

This isn't meant to replace a real production queue. It's small, it's from scratch, and it cuts a few corners a real one wouldn't (see below). The point was building something I could actually reason about end to end, not shipping something bulletproof.

## How it's put together

Five Redis keys per queue, doing basically all the work:

- **`ready`** — jobs waiting to be picked up.
- **`processing`** — jobs a worker currently has claimed. Workers move a job here from `ready` atomically (via Redis's `BLMOVE`) instead of just popping it, so if a worker crashes mid-job, the job doesn't vanish — it's still sitting here, waiting.
- **`delayed`** — a sorted set of jobs waiting out their retry backoff, scored by when they're eligible again. A background poller wakes up on a timer and moves due jobs back to `ready`.
- **`dead`** — jobs that ran out of attempts. Never silently dropped, always inspectable, with the last failure's error message attached.

Workers are just goroutines running a loop: grab a job, run it, then ack it or send it to `delayed`/`dead` depending on what happened. "Concurrency" here is literally just how many of those goroutines are running — no separate semaphore or thread pool needed.

If you want the full design writeup, including the parts that were genuinely a judgment call, it's in [forge_project_plan.md](forge_project_plan.md).

## Running it

You need Go and a Redis instance.

```
go mod tidy
docker run -d -p 6379:6379 redis   # or run redis-server directly
go run ./cmd/forge
```

It enqueues a batch of demo jobs (fake emails — point a local [Mailhog](https://github.com/mailhog/MailHog) instance at `localhost:1025` if you want to actually see them land somewhere), and logs every job's outcome as it happens: done, retried, or dead-lettered. Ctrl+C shuts it down cleanly — it waits for whatever's currently in flight to actually finish before exiting, instead of dropping it.

### Config

Everything's an environment variable, all optional:

| Var | Default | What it does |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | where Redis is |
| `QUEUE_NAME` | `default` | namespaces the Redis keys, so multiple queues can share one Redis |
| `CONCURRENCY` | `10` | how many workers run at once |
| `MAX_READY` | `1000` | backpressure ceiling — producers wait once `ready` hits this |
| `BASE_BACKOFF_MS` | `500` | starting retry delay |
| `MAX_BACKOFF_MS` | `30000` | retry delay ceiling, so retries never drift out for hours |
| `POLL_INTERVAL_MS` | `500` | how often the poller checks for due retries |

## What's not here

A job queue that quietly glosses over its own gaps is worse than one that's upfront about them, so:

- **No reaper.** If a worker dies mid-job, that job just sits in `processing` forever — nothing currently notices and requeues it.
- **No clustering.** One Redis instance, no sentinel/cluster support.
- **At-least-once, not exactly-once.** A job can run twice if a worker crashes at exactly the wrong moment (after finishing the work, before acking it). Handlers need to be safe to run more than once.
- **No metrics or tracing.** It logs to stdout and that's the whole observability story.

None of these are hard to add on top of what's here — they just weren't what this exercise was about.
