"""Offline tests for the split-host noisy agent (U02) and RemoteNoisy (U01).

Run:  PYTHONPATH=. pytest -q tests/test_noisy_agent.py
The agent HTTP server runs on loopback; S3 is a small in-memory fake, so no
ObjectScale host is needed.
"""
import json
import pathlib
import sys
import threading
import time
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from botocore.exceptions import ClientError
from poc import loadgen, noisy_agent, s3util


class FakeBody:
    def read(self):
        return b"x" * 8

    def close(self):
        pass


class FakeS3:
    store = {}
    put_calls = []

    def __init__(self, user="admin", **kw):
        self.user = user

    def put_object(self, Bucket, Key, Body=b"", **kw):
        time.sleep(0.001)
        FakeS3.put_calls.append((self.user, Key))
        FakeS3.store[(Bucket, Key)] = b"x"
        return {"ResponseMetadata": {"HTTPStatusCode": 200, "RetryAttempts": 0}}

    def get_object(self, Bucket, Key, **kw):
        if (Bucket, Key) not in FakeS3.store:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey"},
                 "ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject")
        return {"Body": FakeBody(),
                "ResponseMetadata": {"HTTPStatusCode": 200, "RetryAttempts": 0}}

    def head_bucket(self, Bucket, **kw):
        return {}


@pytest.fixture(autouse=True)
def fake_s3(monkeypatch):
    FakeS3.store, FakeS3.put_calls = {}, []
    monkeypatch.setattr(s3util, "s3", lambda *a, **k: FakeS3(*a, **k))
    yield


@pytest.fixture()
def agent():
    state = noisy_agent.AgentState(token="secret", idle_s=3600)
    srv = noisy_agent.create_server("127.0.0.1", 0, state)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    yield base, state
    srv.shutdown()
    srv.server_close()
    th.join(timeout=2)


def req(base, path, token="secret", body=None, headers=None):
    data = None if body is None else json.dumps(body).encode()
    h = {"Content-Type": "application/json"}
    if token is not None:
        h["X-POC-Token"] = token
        h["X-POC-Time"] = f"{time.time():.6f}"
    h.update(headers or {})
    r = urllib.request.Request(base + path, data=data, headers=h,
                               method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def test_health_requires_token_and_reports_offset(agent):
    base, _ = agent
    code, _ = req(base, "/health", token=None)
    assert code == 401
    code, bad = req(base, "/health", token="wrong")
    assert code == 401
    code, h = req(base, "/health")
    assert code == 200 and h["ok"] and h["service"] == "poc-noisy-agent"
    assert h["offset_ms"] is not None and abs(h["offset_ms"]) < 5000
    assert h["s3"]["ok"] is True


def test_remote_noisy_controls_events_and_stop(agent):
    base, _ = agent
    lg = loadgen.LoadGen(bucket="b", max_keys=0, run_id="demo-test",
                         timeout_s=(1, 2))
    remote = loadgen.RemoteNoisy(lg, base, token="secret", run_id="demo-test",
                                 poll_s=0.05)
    try:
        assert remote.add_noisy(threads=2) == 2
        deadline = time.time() + 5
        while time.time() < deadline:
            if remote.running() and any(u == "noisy" for u, _ in FakeS3.put_calls):
                break
            time.sleep(0.05)
        assert remote.running() is True
        assert any(u == "noisy" for u, _ in FakeS3.put_calls)

        remote.pause_noisy()
        time.sleep(0.05)          # let the 2 in-flight PUTs finish
        n = sum(1 for u, _ in FakeS3.put_calls if u == "noisy")
        time.sleep(0.25)
        assert sum(1 for u, _ in FakeS3.put_calls if u == "noisy") == n
        remote.resume_noisy()
        deadline = time.time() + 5
        while time.time() < deadline:
            if sum(1 for u, _ in FakeS3.put_calls if u == "noisy") > n:
                break
            time.sleep(0.05)
        assert sum(1 for u, _ in FakeS3.put_calls if u == "noisy") > n

        remote.poll()
        assert remote.ingested > 0
        assert any(e.tenant == "tenant-noisy" and e.op == "PUT"
                   for e in lg.events)
        assert remote.stop_noisy() == 0
        deadline = time.time() + 5
        while remote.running() and time.time() < deadline:
            time.sleep(0.05)
        assert remote.running() is False
    finally:
        remote.close()
        lg.stop()


def test_remote_events_attribute_to_request_start_phase():
    lg = loadgen.LoadGen(bucket="b", max_keys=0, run_id="demo-test")
    lg.start_phase("p1")
    ts1 = time.time()
    time.sleep(0.05)
    lg.start_phase("p2")
    ts2 = time.time()
    time.sleep(0.05)
    lg.stop()
    lg.ingest_event("tenant-noisy", "PUT", ts1, 5.0, 200)
    lg.ingest_event("tenant-noisy", "PUT", ts2, 5.0, 200)
    ps = lg.phase_stats()
    assert ps["p1"]["tenant-noisy PUT"]["n"] == 1
    assert ps["p2"]["tenant-noisy PUT"]["n"] == 1


def test_check_clock_rejects_large_offset():
    remote = loadgen.RemoteNoisy.__new__(loadgen.RemoteNoisy)
    remote.health = lambda timeout=10: {"offset_ms": 120.0}
    with pytest.raises(loadgen.RemoteNoisyError):
        remote.check_clock()
    remote.health = lambda timeout=10: {"offset_ms": -20.0}
    off, _ = remote.check_clock()
    assert off == -20.0


def test_agent_reuses_generator_and_requires_run_id(agent):
    base, state = agent
    code, _ = req(base, "/noisy/start", body={"threads": 1})
    assert code == 500
    code, res = req(base, "/noisy/start",
                    body={"run_id": "demo-test", "threads": 1})
    assert code == 200 and res["launched"] == 1
    lg1 = state.lg
    code, res = req(base, "/noisy/start",
                    body={"run_id": "demo-test", "threads": 1})
    assert code == 200 and res["launched"] == 0   # already running: no dup
    assert state.lg is lg1
    code, _ = req(base, "/noisy/stop", body={})
    assert code == 200
    deadline = time.time() + 5
    while state.lg.running("tenant-noisy") and time.time() < deadline:
        time.sleep(0.05)
    assert not state.lg.running("tenant-noisy")
