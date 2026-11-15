"""TC result JSON writer + scorecard. One JSON file per test case under reports/raw/."""
import json, os, time
from pathlib import Path
from . import config

RAW = Path(config.REPORT_DIR) / "raw"

RUN_ID_ENV = "POC_RUN_ID"


def run_id():
    rid = os.environ.get(RUN_ID_ENV) or f"demo-{time.strftime('%Y%m%d-%H%M%S')}"
    os.environ[RUN_ID_ENV] = rid
    return rid


def write(tc_id, name, passed, metrics=None, detail=None):
    RAW.mkdir(parents=True, exist_ok=True)
    rec = {
        "tc": tc_id, "name": name, "passed": bool(passed),
        "run_id": run_id(), "ts": int(time.time()),
        "metrics": metrics or {}, "detail": detail,
    }
    (RAW / f"{tc_id}.json").write_text(json.dumps(rec, indent=2, default=str))
    return rec


def load_all():
    out = {}
    for f in sorted(RAW.glob("*.json")):
        try:
            rec = json.loads(f.read_text())
            out[rec.get("tc", f.stem)] = rec
        except Exception:
            pass
    return out


OBJECTIVES = {
    "O1": ("Spark 4.2 reads & writes ObjectScale S3 (S3A)",
           ["TC0.1", "TC1.1", "TC1.2", "TC1.3", "TC1.4", "TC1.5", "TC1.6"]),
    "O2": ("S3 Throttler enforces QoS (dry-run, 429, prefix scoping, recovery)",
           ["TC2.1", "TC2.2", "TC2.3", "TC2.4", "TC2.5", "TC2.6", "TC2.7",
            "TC2.8", "TC2.9", "TC2.10", "TC2.11"]),
    "O3": ("S3 Tables via Iceberg REST catalog (DDL/DML/MERGE/time-travel)",
           ["TC3.1", "TC3.2", "TC3.3", "TC3.4", "TC3.5", "TC3.6", "TC3.7"]),
}


def scorecard():
    res = load_all()
    rows = []
    for oid, (label, tcs) in OBJECTIVES.items():
        run = [t for t in tcs if t in res]
        ok = bool(run) and all(res[t]["passed"] for t in run)
        rows.append({"objective": oid, "description": label,
                     "test_cases": len(run), "status": "PASS" if ok else "FAIL"})
    return rows
