"""Unit tests for poc.loadgen + poc.throttle_eval with a mocked S3 client.

Run:  PYTHONPATH=/root/OBS pytest -q tests/test_loadgen.py
The FakeS3 client replaces poc.s3util.s3 — no ObjectScale needed. It can be
scripted to return ClientError(429/503) responses, inject RetryAttempts on
successful responses, or hang, so throttling, boundary, and lifecycle paths
are exercised deterministically.
"""
import sys, time, types, pathlib, threading, collections
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from botocore.exceptions import ClientError
from poc import loadgen, s3util, throttle_eval


class FakeBody:
    closed = 0
    def __init__(self, n=10):
        self.n = n
        self.closed_flag = False
    def read(self):
        return b"x" * self.n
    def close(self):
        FakeBody.closed += 1
        self.closed_flag = True


def throttle_error(code="SlowDown", status=429, retries=0):
    return ClientError(
        {"Error": {"Code": code, "Message": "slow down"},
         "ResponseMetadata": {"HTTPStatusCode": status,
                              "RetryAttempts": retries}},
        "PutObject")


class FakeS3:
    """In-memory S3 with scriptable failure/hang behavior."""
    store = {}
    put_calls = []
    throttle_after = None        # PUT index after which noisy PUTs get 429
    status_503 = None            # "SlowDown" | "ServiceUnavailable" injected
    ok_retries = 0               # RetryAttempts reported on successful GET
    block_get = None             # threading.Event: GET blocks until set
    latency_s = 0.001

    def __init__(self, user="admin", **kw):
        self.user = user

    def put_object(self, Bucket, Key, Body=b"", ContentLength=0, **kw):
        time.sleep(FakeS3.latency_s)
        FakeS3.put_calls.append((self.user, Key))
        if self.user == "noisy":
            if (FakeS3.throttle_after is not None
                    and len(FakeS3.put_calls) > FakeS3.throttle_after):
                raise throttle_error()
            if FakeS3.status_503:
                raise throttle_error(code=FakeS3.status_503, status=503)
        FakeS3.store[(Bucket, Key)] = bytes(Body)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_object(self, Bucket, Key, **kw):
        if FakeS3.block_get is not None:
            FakeS3.block_get.wait(timeout=5)
        time.sleep(FakeS3.latency_s)
        if (Bucket, Key) not in FakeS3.store:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey"},
                 "ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject")
        return {"Body": FakeBody(),
                "ResponseMetadata": {"HTTPStatusCode": 200,
                                     "RetryAttempts": FakeS3.ok_retries}}


@pytest.fixture(autouse=True)
def fake_s3(monkeypatch):
    FakeS3.store, FakeS3.put_calls = {}, []
    FakeS3.throttle_after, FakeS3.status_503 = None, None
    FakeS3.ok_retries, FakeS3.block_get = 0, None
    FakeBody.closed = 0
    monkeypatch.setattr(s3util, "s3", lambda *a, **k: FakeS3(*a, **k))
    yield


def run_lg(seconds=0.4, **kw):
    lg = loadgen.LoadGen(bucket="b", max_keys=0, **kw)
    lg.start(noisy=True, critical=True)
    time.sleep(seconds)
    lg.stop()
    return lg


def test_workers_generate_events_with_telemetry():
    lg = run_lg()
    tot = lg.totals()
    assert tot["ok"] > 0 and tot["events"] == tot["ok"]
    ev = lg.events[0]
    assert ev.status == 200 and ev.code == "" and ev.retries == 0
    assert ev.op in ("PUT", "GET")


def test_unique_object_keys():
    run_lg(seconds=0.6)
    keys = [k for _, k in FakeS3.put_calls]
    assert len(keys) == len(set(keys)), "duplicate object keys generated"


def test_key_ring_keeps_load_sustained_past_max_keys():
    lg = loadgen.LoadGen(bucket="b", max_keys=20)
    lg.start(noisy=True, critical=False, noisy_threads=8)
    time.sleep(1.0)
    lg.stop()
    noisy_puts = [k for u, k in FakeS3.put_calls if u == "noisy"]
    distinct = {k.split("/")[-1] for k in noisy_puts}
    # load keeps flowing after the cap; object count bounded by the ring
    assert len(noisy_puts) > 20, "load must not stall at max_keys"
    assert len(distinct) <= 20
    assert lg.totals()["keys_reserved"] > 20


def test_get_body_consumed_and_closed():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start(noisy=False, critical=True)
    time.sleep(0.3)
    n_gets = sum(1 for e in lg.events if e.op == "GET" and e.status == 200)
    lg.stop()
    assert n_gets > 0 and FakeBody.closed >= n_gets


def test_throttled_status_and_code_captured():
    FakeS3.throttle_after = 5
    lg = run_lg(seconds=0.8)
    tot = lg.totals()
    assert tot["throttled"] > 0
    t = [e for e in lg.events if e.status == 429][0]
    assert t.code == "SlowDown" and t.tenant == "tenant-noisy"


def test_503_classification_slowdown_vs_other():
    # 503+SlowDown counts as throttled; other 503 counts as error
    st = loadgen.classify(503, "SlowDown")
    assert st == "r503s" and st in loadgen.THROTTLED
    assert loadgen.classify(503, "ServiceUnavailable") == "r503o"
    assert loadgen.classify(503, "InternalError") == "r503o"
    assert loadgen.classify(429, "") == "r429"
    assert loadgen.classify(500, "InternalError") == "error"
    FakeS3.status_503 = "ServiceUnavailable"
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start(); lg.start_phase("p")
    time.sleep(0.5)
    lg.stop()
    grp = lg.phase_stats()["p"].get("tenant-noisy PUT", {})
    assert grp.get("r503_other", 0) > 0 and grp.get("throttled", 0) == 0


def test_retries_captured_on_success_and_failure():
    FakeS3.ok_retries = 2
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start(noisy=False, critical=True)
    time.sleep(0.3)
    lg.start_phase("p")
    time.sleep(0.3)
    lg.stop()
    g = lg.phase_stats()["p"].get("tenant-critical GET", {})
    assert g.get("retries_ok", 0) >= 2 and g.get("retries_fail", 0) == 0


def test_noisy_stop_and_restart():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start()
    lg.stop_noisy()
    assert not lg.running("tenant-noisy") and lg.running("tenant-critical")
    before = len([k for u, k in FakeS3.put_calls if u == "noisy"])
    n = lg.add_noisy(2)
    assert n == 2 and lg.running("tenant-noisy")
    time.sleep(0.3)
    assert len([k for u, k in FakeS3.put_calls if u == "noisy"]) > before
    assert lg.stop_noisy() == 0 and lg.lingering("tenant-noisy") == 0
    lg.stop()


def test_add_noisy_no_duplicates_while_running():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start(noisy=True, critical=False, noisy_threads=3)
    assert lg.add_noisy(3) == 0
    assert len(lg.threads["tenant-noisy"]) == 3
    lg.stop()


def test_per_tenant_pause_resume():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start()
    lg.pause_noisy()
    time.sleep(0.4)
    n_before = len([k for u, k in FakeS3.put_calls if u == "noisy"])
    time.sleep(0.4)
    n_paused = len([k for u, k in FakeS3.put_calls if u == "noisy"])
    assert n_paused - n_before <= 6     # in-flight only
    assert lg.running("tenant-critical")
    lg.resume_noisy()
    time.sleep(0.4)
    assert len([k for u, k in FakeS3.put_calls if u == "noisy"]) > n_paused
    lg.pause_critical(); lg.resume_critical()
    assert lg.running("tenant-critical")
    lg.stop()


def test_stop_critical_independent():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start()
    lg.stop_critical()
    assert not lg.running("tenant-critical") and lg.running("tenant-noisy")
    lg.stop()


def test_phase_stats_and_timestamps():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start(noisy=False, critical=True)
    lg.start_phase("baseline")
    time.sleep(0.4)
    lg.add_noisy(2); lg.start_phase("contention")
    time.sleep(0.4)
    lg.stop_noisy(); lg.start_phase("recovery")
    time.sleep(0.4)
    lg.stop()
    ps = lg.phase_stats()
    assert set(ps) >= {"baseline", "contention", "recovery"}
    for name in ("baseline", "contention", "recovery"):
        w = ps[name]["_window"]
        assert 0 < w["seconds"] and w["start"] < w["end"]
    assert "tenant-critical GET" in ps["baseline"]
    assert "tenant-noisy PUT" in ps["contention"]
    assert ps["recovery"].get("tenant-noisy PUT", {}).get("n", 0) <= 2


def test_phase_name_reuse_not_overwritten():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start(noisy=False, critical=True)
    lg.start_phase("x"); time.sleep(0.2)
    lg.start_phase("x"); time.sleep(0.2)
    lg.stop()
    ps = lg.phase_stats()
    assert "x" in ps and "x#2" in ps


def test_phase_attribution_at_request_start_and_boundary():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.start(noisy=False, critical=True, critical_threads=1)
    lg.start_phase("p1")
    time.sleep(0.3)                    # normal GETs complete inside p1
    FakeS3.block_get = threading.Event()
    time.sleep(0.15)                   # next GET enters blocked, tagged p1
    lg.start_phase("p2")               # phase flips while request runs
    FakeS3.block_get.set()             # completes inside p2's window
    time.sleep(0.3)
    lg.stop()
    ps = lg.phase_stats()
    g1 = ps["p1"].get("tenant-critical GET", {})
    # blocked request is attributed to p1 (its start phase) and flagged boundary
    assert g1.get("n", 0) >= 1 and g1.get("boundary", 0) >= 1


def test_final_phase_survives_deque_overflow():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    lg.events = collections.deque(maxlen=50)   # tiny buffer, overflow certain
    lg.start(noisy=True, critical=True)
    lg.start_phase("first")
    time.sleep(0.5)
    lg.start_phase("last")
    time.sleep(0.5)
    lg.stop()
    ps = lg.phase_stats()
    total = sum(g["n"] for ph in ps.values() for k, g in ph.items()
                if k != "_window")
    assert total > 50, "events beyond deque capacity must be retained"
    assert "last" in ps and "first" in ps


def test_worker_shutdown_timeout_keeps_reference():
    FakeS3.block_get = threading.Event()   # GET hangs forever
    lg = loadgen.LoadGen(bucket="b", max_keys=0, timeout_s=(0.1, -1.9))
    lg.start(noisy=False, critical=True, critical_threads=1)
    time.sleep(0.3)
    leftover = lg.stop_critical()          # join ~0.1s, worker still hung
    assert leftover >= 1 and lg.lingering("tenant-critical") >= 1
    assert lg.threads["tenant-critical"], "live worker ref must be kept"
    FakeS3.block_get.set()
    for t in lg.threads["tenant-critical"]:
        t.join(timeout=5)
    lg.stop()


def test_percentile_math():
    assert loadgen.pctile([10, 20, 30, 40], 50) == 25
    assert loadgen.pctile([10, 20, 30, 40], 95) > 30
    assert loadgen.pctile([], 95) == 0
    # split ok vs failed percentiles
    evs = [(0, 10.0, 200, "", 0)] * 10 + [(0, 100.0, 429, "SlowDown", 1)] * 10
    a = loadgen.LoadGen._agg(evs, 1.0)
    assert a["ok_p95"] <= 10 and a["fail_p95"] >= 100
    assert a["retries_ok"] == 0 and a["retries_fail"] == 10


def test_start_is_idempotent():
    lg = loadgen.LoadGen(bucket="b", max_keys=0)
    s1 = lg.start()
    n_threads = sum(len(v) for v in lg.threads.values())
    assert lg.start() == s1
    assert sum(len(v) for v in lg.threads.values()) == n_threads
    lg.stop()


# ---------- throttle_eval ----------

class FakeThr:
    def __init__(self):
        self.rules, self.mode, self.code = [], "disabled", 503
    def get_rules(self):       return self.rules
    def get_mode(self):        return self.mode
    def get_response_code(self): return self.code
    def list_actions(self):    return {"actions": ["s3:PutObject", "s3:GetObject"]}
    class api:
        @staticmethod
        def get(path):
            return types.SimpleNamespace(status_code=404, text="")
        @staticmethod
        def post(path, body=None):
            return types.SimpleNamespace(status_code=503, text="")


RULE = {"id": "r1", "rateUnit": "TPS",
        "filters": {"principals": ["urn:ecs:iam::poc:user/n"],
                    "actions": ["s3:PutObject"]},
        "limit": {"value": 50}, "scoping": {"per": ["principal"]}}


def test_verify_rule_accepts_valid():
    chk = throttle_eval.verify_rule(FakeThr(), RULE)
    assert chk["ok"], chk["problems"]


@pytest.mark.parametrize("mutate", [
    lambda r: r.update(bogus=1),
    lambda r: r.update(rateUnit="PPS"),
    lambda r: r["filters"].pop("principals"),
    lambda r: r.update(limit={"value": 0}),
    lambda r: r["filters"].update(paths={"matcher": "fuzzy", "values": []}),
])
def test_verify_rule_rejects_bad(mutate):
    r = {**RULE, "filters": dict(RULE["filters"]), "limit": dict(RULE["limit"])}
    mutate(r)
    chk = throttle_eval.verify_rule(FakeThr(), r)
    assert not chk["ok"]


def _g(tps, thr=0, p95=0, ok=None, n=100, p99=0, r429=None, r503s=None,
       r503o=0, ret_ok=0, ret_fail=0):
    r429 = thr if r429 is None else r429
    r503s = 0 if r503s is None else r503s
    return {"n": n, "tps": tps, "p50": p95 / 2, "p95": p95, "p99": p99,
            "throttled": thr, "errors": 0,
            "ok": ok if ok is not None else n - thr,
            "r429": r429, "r503_slowdown": r503s, "r503_other": r503o,
            "retries_ok": ret_ok, "retries_fail": ret_fail,
            "retries": ret_ok + ret_fail, "boundary": 0, "codes": {}}


def _stats(noisy_ok, noisy_thr, crit_p95, rec_p95, base_p95,
           enforced_s=120, contention_n=10000, sim_thr=0,
           rec_noisy_n=12000, rec_crit_thr=0,
           enf_noisy_p95=0, cont_noisy_p95=15):
    return {
        "baseline": {"tenant-critical GET": _g(500, p95=base_p95),
                     "_window": {"seconds": 60}},
        "contention": {"tenant-noisy PUT": _g(250, n=contention_n,
                                               p95=cont_noisy_p95),
                       "tenant-critical GET": _g(450, p95=30),
                       "_window": {"seconds": 60}},
        "simulation": {"tenant-noisy PUT": _g(250, n=15000, thr=sim_thr),
                       "tenant-critical GET": _g(450, p95=28),
                       "_window": {"seconds": 60}},
        "enforced": {"tenant-noisy PUT": _g(300, thr=noisy_thr, ok=noisy_ok,
                                            n=noisy_ok + noisy_thr,
                                            p95=enf_noisy_p95),
                     "tenant-critical GET": _g(450, p95=crit_p95),
                     "_window": {"seconds": enforced_s}},
        # recovery: noisy STILL RUNNING under enforcement — elevated
        # throttle events on the noisy side are expected, not a failure
        "recovery": {"tenant-noisy PUT": _g(rec_noisy_n / 60, n=rec_noisy_n,
                                            thr=rec_noisy_n // 3),
                     "tenant-critical GET": _g(500, p95=rec_p95,
                                               thr=rec_crit_thr),
                     "_window": {"seconds": 60}}}


def test_evaluate_pass_when_enforced():
    # 50 TPS limit, 120s enforced: ok_tps=50 <= 60; 24000 throttled
    st = _stats(noisy_ok=6000, noisy_thr=24000, crit_p95=20,
                rec_p95=10, base_p95=9)
    out = throttle_eval.evaluate(st, rule=RULE)
    assert out["enforcement_active"]
    assert out["overall"] == "PASS"
    assert all(v["verdict"] == "PASS" for v in out["verdicts"])


def test_evaluate_inconclusive_without_429s():
    st = _stats(noisy_ok=30000, noisy_thr=0, crit_p95=20,
                rec_p95=10, base_p95=9)
    out = throttle_eval.evaluate(st, rule=RULE)
    assert not out["enforcement_active"]
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["noisy_tps_under_limit"] == "INCONCLUSIVE"
    assert names["noisy_throttling_pct"] == "INCONCLUSIVE"
    assert names["noisy_load_sustained"] == "PASS"
    assert out["overall"] == "INCONCLUSIVE"


def test_evaluate_fail_on_sla_breach():
    st = _stats(noisy_ok=6000, noisy_thr=24000, crit_p95=999,
                rec_p95=10, base_p95=9)
    out = throttle_eval.evaluate(st, rule=RULE)
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["critical_p95_within_sla"] == "FAIL"
    assert out["overall"] == "FAIL"


def test_evaluate_fail_when_not_under_limit():
    st = _stats(noisy_ok=12000, noisy_thr=10, crit_p95=20,
                rec_p95=10, base_p95=9)   # ok_tps=100 > 50*1.2
    out = throttle_eval.evaluate(st, rule=RULE)
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["noisy_tps_under_limit"] == "FAIL"


def test_evaluate_sustained_check_fails_when_load_drops():
    st = _stats(noisy_ok=1200, noisy_thr=0, crit_p95=20,
                rec_p95=10, base_p95=9)   # offered=10 TPS < 50*0.8
    st["enforced"]["tenant-noisy PUT"]["n"] = 1200
    out = throttle_eval.evaluate(st, rule=RULE)
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["noisy_load_sustained"] == "FAIL"


def test_evaluate_fails_when_critical_throttled():
    # rule must not affect critical GETs — any 429 on them is a FAIL
    st = _stats(noisy_ok=6000, noisy_thr=24000, crit_p95=20,
                rec_p95=10, base_p95=9, rec_crit_thr=5)
    out = throttle_eval.evaluate(st, rule=RULE)
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["critical_not_throttled"] == "FAIL"
    assert out["overall"] == "FAIL"


def test_evaluate_fails_when_simulation_rejects():
    # dry-run must never produce client-visible 429s
    st = _stats(noisy_ok=6000, noisy_thr=24000, crit_p95=20,
                rec_p95=10, base_p95=9, sim_thr=50)
    out = throttle_eval.evaluate(st, rule=RULE)
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["simulation_did_not_reject"] == "FAIL"


def test_evaluate_inconclusive_when_noisy_stops_in_recovery():
    # recovery means protected-under-load; a silent noisy tenant proves nothing
    st = _stats(noisy_ok=6000, noisy_thr=24000, crit_p95=20,
                rec_p95=10, base_p95=9, rec_noisy_n=0)
    out = throttle_eval.evaluate(st, rule=RULE)
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["noisy_sustained_in_recovery"] == "INCONCLUSIVE"


def test_evaluate_fails_when_noisy_below_offer_in_recovery():
    st = _stats(noisy_ok=6000, noisy_thr=24000, crit_p95=20,
                rec_p95=10, base_p95=9, rec_noisy_n=600)  # 10 TPS < 40
    out = throttle_eval.evaluate(st, rule=RULE)
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["noisy_sustained_in_recovery"] == "FAIL"


def test_evaluate_delay_mode_enforcement():
    # ObjectScale delay-throttling: all 200s, ok_tps collapses to ~limit,
    # noisy latency spikes — enforcement proven without any 429s.
    st = _stats(noisy_ok=5160, noisy_thr=0, crit_p95=15,
                rec_p95=10, base_p95=9, enf_noisy_p95=140, cont_noisy_p95=15)
    out = throttle_eval.evaluate(st, rule=RULE)
    assert out["enforcement_mode"] == "delay" and out["enforcement_active"]
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["noisy_tps_under_limit"] == "PASS"      # 43 <= 50*1.2
    assert names["noisy_throttling_pct"] == "PASS"       # delay evidence
    assert names["noisy_sustained_in_recovery"] == "PASS"  # floor lowered
    assert names["recovery_p95_vs_baseline"] in ("PASS", "FAIL")


def test_evaluate_no_delay_when_latency_flat():
    # low ok_tps with no latency spike = load died, NOT delay-enforcement
    st = _stats(noisy_ok=2400, noisy_thr=0, crit_p95=20,
                rec_p95=10, base_p95=9, enf_noisy_p95=15, cont_noisy_p95=15)
    out = throttle_eval.evaluate(st, rule=RULE)
    assert not out["enforcement_active"]
    names = {v["name"]: v["verdict"] for v in out["verdicts"]}
    assert names["noisy_tps_under_limit"] == "INCONCLUSIVE"


def test_server_counters_reports_unavailable():
    srv = throttle_eval.server_counters(FakeThr())
    assert srv["available"] is False and "data" in srv
