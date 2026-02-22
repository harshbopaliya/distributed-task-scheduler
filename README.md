# Distributed Task Scheduler

A lightweight task orchestration engine built with Python. Submit tasks via REST API, the system runs them in the right order — respecting dependencies between tasks and a concurrency cap. State is stored in SQLite so everything survives a process restart.

---

## Setup

**Requirements:** Python 3.11+

```bash
git clone https://github.com/harshbopaliya/distributed-task-scheduler.git
cd distributed-task-scheduler
```

With [uv](https://github.com/astral-sh/uv) (recommended):

```bash
uv sync
uv run python main.py
```

Or with pip:

```bash
pip install -r requirements.txt
python main.py
```

Server comes up at `http://localhost:8000`. Swagger UI is at `/docs`.

---

## Configuration

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `MAX_CONCURRENT_TASKS` | `3` | Max tasks running at the same time |
| `DATABASE_PATH` | `tasks.db` | SQLite file path |
| `API_HOST` | `0.0.0.0` | Bind address |
| `API_PORT` | `8000` | Port |
| `SCHEDULER_POLL_INTERVAL` | `0.5` | Seconds between scheduler poll cycles |

---

## API

### Submit a task

```
POST /tasks
```

```json
{
  "id": "task-A",
  "type": "data_processing",
  "duration_ms": 5000,
  "dependencies": []
}
```

- `id` — unique identifier (no leading/trailing whitespace)
- `type` — arbitrary string label, the engine doesn't care what it is
- `duration_ms` — must be > 0; execution is simulated via `asyncio.sleep`
- `dependencies` — list of task IDs that must be `COMPLETED` before this one starts

**Responses:**
- `201` — accepted and queued
- `400` — dependency ID doesn't exist, self-dependency, or would create a cycle
- `409` — a task with this ID already exists

### Get a task

```
GET /tasks/{task_id}
```

Returns the full record: status, `created_at`, `started_at`, `completed_at`, `error_message`.

### List tasks

```
GET /tasks
GET /tasks?status_filter=QUEUED
GET /tasks?status_filter=RUNNING
GET /tasks?status_filter=COMPLETED
GET /tasks?status_filter=FAILED
```

### Utility endpoints

```
GET    /health            — returns db path and concurrency config
GET    /scheduler/status  — running count, max_concurrent, available_slots
DELETE /tasks             — wipes all tasks (useful for testing / clearing the queue)
```

---

## Quick example

```bash
# submit t1, then t2 which depends on t1
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"id":"t1","type":"job","duration_ms":3000,"dependencies":[]}'

curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"id":"t2","type":"job","duration_ms":2000,"dependencies":["t1"]}'

# check t2 — it'll be QUEUED while t1 is still RUNNING
curl http://localhost:8000/tasks/t2
```

---

## How it works

**Task lifecycle:**

```
QUEUED → RUNNING → COMPLETED
                 ↘ FAILED
```

The scheduler runs as a background `asyncio.Task` spawned at startup. On each poll cycle it:

1. Fetches all `QUEUED` tasks from SQLite
2. Fetches the set of all `COMPLETED` task IDs
3. For each queued task, checks `all(dep in completed_ids for dep in task.dependencies)`
4. If satisfied and not already being dispatched, calls `_execute_task`

**Concurrency** is controlled by `asyncio.Semaphore(MAX_CONCURRENT_TASKS)`, but the slot check happens **before** DB dispatch, not inside the fire-and-forget task. The scheduler gates dispatch with `len(self._running_tasks) >= self.max_concurrent` while holding `_running_lock`, so it stops dispatching the moment the slots fill up. The semaphore then acts as a second line of defence.

**Preventing double execution** works in two layers:
1. In-process: `_running_tasks` (a `set`) + `_running_lock` (an `asyncio.Lock`) — the scheduler checks slot availability and already-dispatched IDs together under the same lock, so no two poll cycles can race to dispatch the same task or blow past the concurrency cap
2. Database: `claim_task_for_execution` does `UPDATE ... WHERE id = ? AND status = 'QUEUED'` and checks `rowcount > 0` — the conditional UPDATE is the last safety net if anything slips through the in-process check

**Dependency validation at submit time** — `validate_task_dependencies` runs three checks before writing anything to the DB:
- self-dependency (task ID appears in its own dependencies list)
- missing dependencies (any dep ID not found in the DB)
- cycle detection (DFS over the full in-memory graph including the new node)

Rejected submissions get a `400` with the exact reason.

**Crash recovery:** `start()` calls `reset_running_tasks()` before the loop begins — `UPDATE tasks SET status = 'QUEUED', started_at = NULL WHERE status = 'RUNNING'`. Any task mid-execution when the process died gets re-queued and runs again from scratch. Conservative, but nothing gets permanently stuck.

**Failed dependency cascade:** each poll cycle checks `failed_ids` before checking `completed_ids`. If any of a task's dependencies are in the failed set, that task immediately gets marked `FAILED` with `error_message: "dependency failed"` rather than waiting in `QUEUED` indefinitely. Cascades transitively — one hop per poll cycle.

---

## File layout

```
api.py              FastAPI routes, lifespan (DB init + scheduler start/stop)
scheduler.py        Poll loop, dependency checks, semaphore, cycle detection
database.py         All SQL — create, claim, status updates, crash recovery
models.py           Pydantic request/response schemas
config.py           Env var loading with defaults
main.py             Starts uvicorn
test_scheduler.py   Integration tests (hit a live server)
```

---

## Tests

Needs the server running first:

```bash
# terminal 1
python main.py

# terminal 2
python test_scheduler.py
```

Tests run sequentially and print `PASS` / `FAIL` per test. The runner calls `DELETE /tasks` before starting, so you can re-run against the same server as many times as you like. The suite covers: single task execution, duplicate ID rejection, missing dependency rejection, self-dependency, circular dependency, a 3-step ETL chain with ordering assertions, a fan-in DAG (3 sources → merge → load), concurrency cap enforcement, dependency completion cascade, 404 for unknown IDs, and list endpoint consistency.

---

## Design notes

Single-process async (one uvicorn + asyncio event loop) — no multiprocessing, no separate worker processes. Keeps coordination simple. Everything that would need a cross-process lock with workers is just an in-process asyncio lock instead.

SQLite with an asyncio lock serialising writes — reads don't go through the lock (SQLite handles concurrent reads fine). The lock is just to prevent "database is locked" errors from two coroutines trying to write at exactly the same time. Good enough for this scale.

Polling over event-driven — the 0.5s interval means a task that becomes eligible only starts executing up to 0.5s later. That's fine here. Event-driven (notify on completion) would lower the latency but adds a layer of complexity that isn't justified for moderate workloads.

The crash recovery is intentionally blunt. Re-running a task that was 90% done is acceptable when tasks are just `sleep()`. In a real system you'd think harder about whether tasks are idempotent before deciding to re-execute them.

---

## Scaling

See [SCALING.md](SCALING.md) — covers Postgres migration, horizontal API workers, multiple scheduler instances, and what a million tasks/hour setup would actually need.

---

## License

[MIT](LICENSE)

---

Harsh Bopaliya — bopaliyaharsh7@gmail.com
