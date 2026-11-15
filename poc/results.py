"""TC result JSON writer + scorecard. One JSON file per test case under reports/raw/.

Every result carries a `status` of PASS | FAIL | INCONCLUSIVE (`passed` is kept
for compatibility and is True only for PASS) and the run ID it belongs to. The
scorecard only counts results of the current run, so a stale file from an
earlier run can never turn an objective green.
"""
import json, os, re, time
from pathlib import Path
from . import config

RAW = Path(config.REPORT_DIR) / "raw"
RUN_ID_FILE = Path(config.REPORT_DIR) / "current_run_id"
RUN_ID_ENV = "POC_RUN_ID"

PASS, FAIL, INCONCLUSIVE, INCOMPLETE = "PASS", "FAIL", "INCONCLUSIVE", "INCOMPLETE"
STATUSES = (PASS, FAIL, INCONCLUSIVE)
STATUS_COLORS = {PASS: "#1e7d32", FAIL: "#b71c1c",
                 INCONCLUSIVE: "#ef6c00", INCOMPLETE: "#546e7a"}


def _normalise(rid):
    """Run IDs double as the S3 key prefix; 99_reset only cleans 'demo-*'."""
    rid = re.sub(r"[^A-Za-z0-9._-]", "-", str(rid).strip())
    return rid if rid.startswith("demo-") else f"demo-{rid}"


def _persist(rid):
    RUN_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not RUN_ID_FILE.exists() or RUN_ID_FILE.read_text().strip() != rid:
        RUN_ID_FILE.write_text(rid + "\n")
    return rid


def _timestamp_id():
    return f"demo-{time.strftime('%Y%m%d-%H%M%S')}"


def run_id():
    """Active run ID: POC_RUN_ID env (exported once per suite by run_all.sh),
    else the persisted reports/current_run_id, else RUN_ID from .env, else a
    timestamp. It is persisted so every notebook kernel shares one ID."""
    rid = os.environ.get(RUN_ID_ENV)
    if not rid and RUN_ID_FILE.exists():
        rid = RUN_ID_FILE.read_text().strip()
    return _persist(_normalise(rid or config.RUN_ID or _timestamp_id()))


def new_run():
    """Start a new run (called by 99_reset): the suite-wide POC_RUN_ID if one
    was exported, otherwise a fresh timestamp ID."""
    return _persist(_normalise(os.environ.get(RUN_ID_ENV) or _timestamp_id()))


def status_of(rec):
    """Status of a stored record (older files only have `passed`)."""
    st = rec.get("status")
    if st in STATUSES:
        return st
    return PASS if rec.get("passed") else FAIL


def write(tc_id, name, passed=None, metrics=None, detail=None, status=None):
    """Persist a TC result. Give either `status` (PASS|FAIL|INCONCLUSIVE) or a
    boolean `passed`; `status` wins when both are given."""
    if status is None:
        if isinstance(passed, str):
            status = passed
        else:
            status = PASS if passed else FAIL
    status = str(status).upper()
    if status not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}, got {status!r}")
    RAW.mkdir(parents=True, exist_ok=True)
    rec = {
        "tc": tc_id, "name": name, "status": status,
        "passed": status == PASS,
        "run_id": run_id(), "ts": int(time.time()),
        "metrics": metrics or {}, "detail": detail,
    }
    (RAW / f"{tc_id}.json").write_text(json.dumps(rec, indent=2, default=str))
    return rec


def load_all(run=None):
    """All stored results, or only those of run ID `run` when given."""
    out = {}
    for f in sorted(RAW.glob("*.json")):
        try:
            rec = json.loads(f.read_text())
        except Exception:
            continue
        if run is not None and rec.get("run_id") != run:
            continue
        out[rec.get("tc", f.stem)] = rec
    return out


def load_run(run=None):
    """Results of the current (or given) run only."""
    return load_all(run or run_id())


OBJECTIVES = {
    "O1": ("Spark 4.2 reads & writes ObjectScale S3 (S3A)",
           ["TC0.1", "TC1.1", "TC1.2", "TC1.3", "TC1.4", "TC1.5", "TC1.6"]),
    "O2": ("S3 Throttler enforces QoS (dry-run, 429, prefix scoping, recovery)",
           ["TC2.1", "TC2.2", "TC2.3", "TC2.4", "TC2.5", "TC2.6", "TC2.7",
            "TC2.8", "TC2.9", "TC2.10", "TC2.11"]),
    "O3": ("S3 Tables via Iceberg REST catalog (DDL/DML/MERGE/time-travel)",
           ["TC3.1", "TC3.2", "TC3.3", "TC3.4", "TC3.5", "TC3.6", "TC3.7"]),
}


def objective_status(statuses, expected):
    """INCOMPLETE if any expected TC is missing; else FAIL > INCONCLUSIVE > PASS."""
    if len(statuses) < expected:
        return INCOMPLETE
    if FAIL in statuses:
        return FAIL
    if INCONCLUSIVE in statuses:
        return INCONCLUSIVE
    return PASS


def scorecard(run=None):
    """Objective status for one run (default: the current run). Results from
    other runs are ignored and listed as `stale`."""
    run = run or run_id()
    everything = load_all()
    res = {tc: r for tc, r in everything.items() if r.get("run_id") == run}
    rows = []
    for oid, (label, tcs) in OBJECTIVES.items():
        have = [t for t in tcs if t in res]
        sts = [status_of(res[t]) for t in have]
        rows.append({
            "objective": oid, "description": label,
            "status": objective_status(sts, len(tcs)),
            "test_cases": f"{len(have)}/{len(tcs)}",
            "pass": sts.count(PASS), "fail": sts.count(FAIL),
            "inconclusive": sts.count(INCONCLUSIVE),
            "missing": [t for t in tcs if t not in res],
            "stale": [t for t in tcs if t not in res and t in everything],
            "not_pass": {t: status_of(res[t]) for t in have
                         if status_of(res[t]) != PASS},
            "run_id": run,
        })
    return rows


def overall(run=None):
    """Single status across all objectives (same precedence as an objective)."""
    sts = [r["status"] for r in scorecard(run)]
    if INCOMPLETE in sts:
        return INCOMPLETE
    return objective_status(sts, len(sts))
