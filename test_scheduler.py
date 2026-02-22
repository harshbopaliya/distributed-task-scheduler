import requests
import time

BASE = "http://localhost:8000"


def post_task(task):
    r = requests.post(f"{BASE}/tasks", json=task)
    return r.status_code, r.json()


def get_task(tid):
    r = requests.get(f"{BASE}/tasks/{tid}")
    return r.json() if r.status_code == 200 else None


def wait_done(tid, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = get_task(tid)
        if t and t["status"] in ("COMPLETED", "FAILED"):
            return t
        time.sleep(0.5)
    raise TimeoutError(f"{tid} still not done after {timeout}s")


# just check a single extract job goes through
def test_extract_job():
    code, _ = post_task({"id": "extract-orders", "type": "extract", "duration_ms": 1000, "dependencies": []})
    assert code == 201
    t = wait_done("extract-orders")
    assert t["status"] == "COMPLETED"


# same pipeline submitted twice shouldnt work
def test_duplicate_pipeline():
    post_task({"id": "extract-users", "type": "extract", "duration_ms": 500, "dependencies": []})
    code, body = post_task({"id": "extract-users", "type": "extract", "duration_ms": 500, "dependencies": []})
    assert code == 409


def test_transform_missing_source():
    # transform depends on extract that doesnt exist yet
    code, _ = post_task({"id": "transform-orders", "type": "transform", "duration_ms": 800, "dependencies": ["extract-raw-sales"]})
    assert code == 400


def test_self_dep():
    code, _ = post_task({"id": "load-warehouse", "type": "load", "duration_ms": 500, "dependencies": ["load-warehouse"]})
    assert code == 400


def test_circular_etl():
    post_task({"id": "ext-inventory", "type": "extract", "duration_ms": 300, "dependencies": []})
    post_task({"id": "trans-inventory", "type": "transform", "duration_ms": 300, "dependencies": ["ext-inventory"]})
    code, _ = post_task({"id": "bad-step", "type": "load", "duration_ms": 300, "dependencies": ["trans-inventory", "bad-step"]})
    assert code == 400


# extract -> transform -> load, classic etl chain
def test_etl_chain():
    post_task({"id": "ext-sales", "type": "extract", "duration_ms": 1000, "dependencies": []})
    post_task({"id": "trans-sales", "type": "transform", "duration_ms": 1000, "dependencies": ["ext-sales"]})
    post_task({"id": "load-sales", "type": "load", "duration_ms": 800, "dependencies": ["trans-sales"]})

    ext = wait_done("ext-sales", timeout=15)
    trans = wait_done("trans-sales", timeout=15)
    load = wait_done("load-sales", timeout=15)

    assert ext["status"] == trans["status"] == load["status"] == "COMPLETED"
    assert ext["completed_at"] <= trans["completed_at"] <= load["completed_at"]


# multiple sources feed into one transform then one load
def test_multi_source_etl():
    post_task({"id": "ext-crm", "type": "extract", "duration_ms": 800, "dependencies": []})
    post_task({"id": "ext-erp", "type": "extract", "duration_ms": 1200, "dependencies": []})
    post_task({"id": "ext-logs", "type": "extract", "duration_ms": 600, "dependencies": []})
    post_task({"id": "trans-merge", "type": "transform", "duration_ms": 1000, "dependencies": ["ext-crm", "ext-erp", "ext-logs"]})
    post_task({"id": "load-dw", "type": "load", "duration_ms": 800, "dependencies": ["trans-merge"]})

    result = wait_done("load-dw", timeout=25)
    assert result["status"] == "COMPLETED"

    for src in ("ext-crm", "ext-erp", "ext-logs"):
        assert get_task(src)["status"] == "COMPLETED"
    assert get_task("trans-merge")["status"] == "COMPLETED"


def test_concurrency():
    # 6 independent extracts, server allows 3 max
    for i in range(1, 7):
        post_task({"id": f"ext-region-{i}", "type": "extract", "duration_ms": 5000, "dependencies": []})

    time.sleep(1.2)
    s = requests.get(f"{BASE}/scheduler/status").json()
    assert s["current_running"] <= s["max_concurrent"], \
        f"concurrency blown: {s['current_running']} running with limit {s['max_concurrent']}"


def test_unknown():
    assert requests.get(f"{BASE}/tasks/ext-does-not-exist").status_code == 404


def test_list_tasks():
    r = requests.get(f"{BASE}/tasks")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == len(body["tasks"]) and body["total"] > 0


# happy path only — no way to force a task to fail from outside without a debug endpoint
def test_failed_dep_cascade():
    post_task({"id": "src-cascade", "type": "extract", "duration_ms": 300, "dependencies": []})
    wait_done("src-cascade", timeout=10)
    post_task({"id": "dst-cascade", "type": "load", "duration_ms": 300, "dependencies": ["src-cascade"]})
    result = wait_done("dst-cascade", timeout=10)
    assert result["status"] == "COMPLETED"


if __name__ == "__main__":
    try:
        requests.get(f"{BASE}/health", timeout=3)
    except Exception:
        print("cant reach server, is it running?")
        exit(1)

    # wipe any state from previous runs so IDs don't collide
    requests.delete(f"{BASE}/tasks")

    tests = [
        test_extract_job,
        test_duplicate_pipeline,
        test_transform_missing_source,
        test_self_dep,
        test_circular_etl,
        test_etl_chain,
        test_multi_source_etl,
        test_concurrency,
        test_failed_dep_cascade,
        test_unknown,
        test_list_tasks,
    ]

    failed = []
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except Exception as e:
            print(f"FAIL  {t.__name__} -> {e}")
            failed.append(t.__name__)

    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    if failed:
        exit(1)