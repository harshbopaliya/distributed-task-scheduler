# Scaling Notes

The current implementation is single-process with SQLite. Works fine for dev and small deployments but here's what I'd change for serious scale.

## What's limiting us now

**SQLite** - single writer, ~1k writes/sec max. Fine for thousands of tasks/hour but becomes the bottleneck past that.

**Single process** - scheduler runs in one asyncio event loop. Can't distribute across machines.

**Polling** - we check for eligible tasks every 0.5s. At high volume you'd want event-driven instead.

## Getting to 10-50k tasks/hour

Swap SQLite for Postgres. Much better write concurrency with proper connection pooling. The existing code mostly works - just need to change the DB driver from `aiosqlite` to `asyncpg`.

Add a few uvicorn workers behind nginx. API scales horizontally since each request just hits the DB.

```bash
uvicorn api:app --workers 4
```

Scheduler is trickier - you'd need leader election or just run one scheduler instance that's beefy enough.

## Getting to 100k+ tasks/hour

At this point you probably want:

1. **Message queue** (Redis, RabbitMQ) between API and workers. Decouples submission from execution.

2. **Multiple workers** consuming from the queue. Each claims tasks atomically like we already do with `claim_task_for_execution`, but distributed.

3. **Separate service for dependency resolution**. The cycle check (DFS) gets expensive when you have millions of tasks in the graph. Would want to cache or precompute.

## Million+ tasks/hour

Now you're looking at:

- Sharded database (partition by task_id hash)
- Kafka or similar for the queue layer
- Probably separate the scheduler into its own service with hot standby
- Container orchestration (k8s) for worker scaling
- Need actual observability (prometheus, grafana)

The core scheduling logic doesn't change much - it's mostly infrastructure. The atomic claim pattern and dependency checks stay the same, just distributed.

## Rough cost estimate

- Current: free (sqlite, single box)
- 10k/hr: ~$50-100/mo (small postgres instance + 1-2 app servers)
- 100k/hr: ~$500/mo (managed postgres, redis, 3-5 workers)
- 1M/hr: haven't priced this out but probably $2-5k/mo depending on cloud provider

## Things I'd do first if scaling

1. Postgres migration (easy win)
2. Add proper metrics - right now no visibility into queue depth, latency distribution etc
3. Redis for distributed locks if running multiple workers
4. Background the cycle check for large graphs - validate async and mark task as pending-validation

The dependency resolution is probably the hardest part to scale. Everything else is standard distributed systems stuff.
