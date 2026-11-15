#!/usr/bin/env python3
"""Generate the six PoC notebooks in notebooks/ (output-free). Idempotent.

The notebooks are build artefacts: change them HERE and regenerate with
`python3 scripts/gen_notebooks.py` — never hand-edit the .ipynb files.
"""
import pathlib, sys

import nbformat

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

KERNEL = {"display_name": "PySpark 4.2 (OBS)", "language": "python",
          "name": "pyspark-obs"}


def md(src, hidden=False):
    c = nbformat.v4.new_markdown_cell(src)
    if hidden:
        c.metadata["jupyter"] = {"source_hidden": True}
    return c


def py(src):
    return nbformat.v4.new_code_cell(src.strip("\n"))


def notes(txt):
    return md(f"**Presenter notes** (hidden)\n\n{txt}", hidden=True)


def notebook(cells):
    n = nbformat.v4.new_notebook(cells=cells)
    for i, cell in enumerate(n.cells):
        cell.id = str(i)
    n.metadata["kernelspec"] = KERNEL
    n.metadata["language_info"] = {"name": "python", "version": "3.11"}
    return n


# ---------------------------------------------------------------- 00_smoke
def nb_00():
    return notebook([
 md("# 00 — Setup & Smoke Test\n"
    "**Objective:** prove Spark 4.2 runs on the standalone cluster and the "
    "ObjectScale PoC environment is configured.\n\n"
    "**Customer sees:** versions, cluster state, and a real Spark job.\n"
    "**TC:** TC0.1"),
 md("## Step 1 — Start the session\n"
    "Loads `.env`, builds a SparkSession on the standalone master, prints "
    "the run ID, the identities the session uses, masked credentials and "
    "versions read from the running components."),
 py('''
import time
import pandas as pd
from poc import config, results, viz
import poc.spark_helpers as sh

spark = sh.build_session("obs-poc")
viz.table(pd.DataFrame(sh.versions(spark).items(), columns=["component", "version"]))
print("run id:", results.run_id())
print("session identities:", sh.identities(spark))
print("users:", config.masked_users())
'''),
 notes("Point out: no secrets printed — all keys masked. Versions are read "
       "live (ObjectScale from the management API, Iceberg from the loaded "
       "runtime). Spark UI: see the table."),
 md("## Step 2 — Cluster smoke job (TC0.1)\n"
    "`spark.range(10M).count()` runs a real distributed job on the worker "
    "and returns the count — proves the cluster computes."),
 py('''
t0 = time.time(); n = spark.range(10_000_000).count(); dt = time.time() - t0
res = results.write("TC0.1", "spark.range(10M).count()", n == 10_000_000,
                    {"count": n, "seconds": round(dt, 1)})
viz.table(pd.DataFrame([{"count": n, "seconds": round(dt, 1)}]))
viz.banner(res["status"], "TC0.1 smoke", f"{n:,} rows in {dt:.1f}s")
spark.stop()
'''),
 notes("If this is slow, the worker may still be registering — wait a few "
       "seconds."),
    ])


# ------------------------------------------------- 01_spark_on_objectscale
def nb_01():
    return notebook([
 md("# 01 — Spark on ObjectScale\n"
    "**Objective:** Spark 4.2 reads and writes ObjectScale S3 via the S3A "
    "connector.\n\n"
    "**Customer sees:** a partitioned dataset written to OBS, listed in the "
    "OBS UI, read back and queried.\n**TC:** TC1.1–TC1.6"),
 md("## Step 1 — Session\nBuild the SparkSession and print versions "
    "(masked credentials)."),
 py('''
import time
import matplotlib.pyplot as plt
import pandas as pd
from poc import config, demo_data, results, throughput, viz
import poc.spark_helpers as sh

spark = sh.build_session("obs-poc")
viz.table(pd.DataFrame(sh.versions(spark).items(), columns=["component", "version"]))
print("run id:", results.run_id(), "| session identities:", sh.identities(spark))
'''),
 notes("Same session configuration as the smoke test — one source of truth "
       "in poc/spark_helpers.py."),
 md("## Step 2 — Generate the dataset (TC1.1)\n"
    "Synthetic sales orders generated **on the Spark side** from the row id "
    "— deterministic, 30 daily partitions from a fixed base date. Row count "
    "scales with `DEMO_SCALE_GB`."),
 py('''
t0 = time.time()
df = demo_data.orders_df(spark)
rows = df.count()
want = demo_data.n_rows_for_scale()
res = results.write("TC1.1", "dataset generated", rows == want,
                    {"rows": rows, "expected_rows": want,
                     "demo_scale_gb": config.DEMO_SCALE_GB,
                     "base_date": config.DEMO_BASE_DATE,
                     "seconds": round(time.time() - t0, 1)})
df.show(5)
viz.banner(res["status"], "TC1.1",
           f"{rows:,} rows (DEMO_SCALE_GB={config.DEMO_SCALE_GB:g}), "
           f"first partition {config.DEMO_BASE_DATE}")
'''),
 notes("Every value is a hash of the row id — the same data on every run, "
       "nothing materialised on the driver. DEMO_SCALE_GB counts "
       "uncompressed row data; see reports/environment.md for the sizing."),
 md("## Step 3 — Write Parquet partitioned by date (TC1.2)\n"
    "Data lands in `poc-spark-data/<run_id>/raw/orders` — one partition per "
    "order_date."),
 py('''
path = demo_data.data_path()
t0 = time.time()
df.write.mode("overwrite").partitionBy("order_date").parquet(path)
secs = round(time.time() - t0, 1)
marker, _ = demo_data.list_objects(config.BUCKET_DATA,
                                   f"{results.run_id()}/raw/orders/_SUCCESS")
res = results.write("TC1.2", "write partitioned parquet", len(marker) == 1,
                    {"path": path, "rows": rows, "seconds": secs,
                     "success_marker": len(marker) == 1})
viz.banner(res["status"], "TC1.2", f"{res['metrics']['seconds']}s -> {path}")
'''),
 notes("Show the Spark UI stages during the write. Parquet uses Spark's "
       "ParquetOutputCommitter (rename-based) on this cluster; the S3A "
       "magic committer is active for the other formats (README, 'Spark "
       "configuration')."),
 md("## Step 4 — Objects on ObjectScale (TC1.3)\n"
    "List the objects Spark just wrote, then **pause and show the bucket in "
    "the ObjectScale UI** — same files, visible in the storage console."),
 py('''
objs, total = demo_data.list_objects(config.BUCKET_DATA, f"{results.run_id()}/raw/")
viz.object_table([(o["Key"], o["Size"]) for o in objs])
res = results.write("TC1.3", "objects listed in bucket", total["objects"] > 0,
                    {**total, "mib": round(total["bytes"] / 2**20, 1)})
viz.banner(res["status"], "TC1.3",
           f"{total['objects']} objects, {total['bytes'] / 2**20:.1f} MiB "
           f"— show the OBS UI now")
'''),
 notes("UI: bucket poc-spark-data, prefix = the run id printed in Step 1."),
 md("## Step 5 — Read back + business queries (TC1.4)\n"
    "Read the Parquet dataset back and answer three business questions: "
    "revenue by day, top customers, orders by status."),
 py('''
spark.read.parquet(path).createOrReplaceTempView("orders")
q1 = spark.sql("SELECT order_date, round(sum(amount*qty),2) rev FROM orders GROUP BY 1 ORDER BY 1").toPandas()
q2 = spark.sql("SELECT customer, round(sum(amount*qty),2) rev FROM orders GROUP BY 1 ORDER BY rev DESC LIMIT 10").toPandas()
q3 = spark.sql("SELECT status, count(*) n FROM orders GROUP BY 1 ORDER BY 1").toPandas()
fig, ax = plt.subplots(1, 2, figsize=(12, 4))
q1.plot.bar(x="order_date", y="rev", ax=ax[0], legend=False); ax[0].set_title("revenue/day")
q2.plot.bar(x="customer", y="rev", ax=ax[1], legend=False); ax[1].set_title("top customers")
res = results.write("TC1.4", "read+queries", len(q1) > 0 and len(q2) > 0,
                    {"days": len(q1), "top": q2.iloc[0].customer})
viz.table(q3); plt.show(); viz.banner(res["status"], "TC1.4")
'''),
 notes("Queries hit OBS via S3A on every executor."),
 md("## Step 6 — Integrity check (TC1.5)\n"
    "Row count and column checksums of the stored dataset must equal the "
    "source — no data lost or altered."),
 py('''
agg = ["count(*) AS n", "sum(qty) AS qty", "round(sum(amount), 2) AS amount"]
src = df.selectExpr(*agg).first().asDict()
dst = spark.table("orders").selectExpr(*agg).first().asDict()
res = results.write("TC1.5", "integrity", src == dst, {"src": src, "dst": dst})
viz.table(pd.DataFrame([src, dst], index=["source", "stored"]))
viz.banner(res["status"], "TC1.5", f"{src['n']:,} rows, checksums "
           f"{'match' if src == dst else 'DIFFER'}")
'''),
 notes("Count plus sum(qty) and sum(amount) — a stronger check than a count."),
 md("## Step 7 — Multipart throughput vs concurrency (TC1.6)\n"
    "Same 256 MiB object uploaded at 1, 2, 4, 8 and 16 parallel part "
    "streams — shows how concurrency scales sustained write throughput."),
 py('''
levels = throughput.multipart_sweep(
    config.BUCKET_THROTTLE, f"{results.run_id()}/multipart",
    levels=(1, 2, 4, 8, 16), total_mb=256, part_mb=16)
viz.table(pd.DataFrame(levels))
res = results.write("TC1.6", "multipart write concurrency sweep",
                    all(r["mbs"] > 0 for r in levels),
                    {"levels": levels, "peak_mbs": max(r["mbs"] for r in levels)})
viz.banner(res["status"], "TC1.6", f"peak {res['metrics']['peak_mbs']:.0f} MB/s")
'''),
 notes("Objects land under poc-throttle/<run_id>/multipart/ — cleaned by "
       "99_reset. The client's connection pool is sized for 16 streams."),
 md("## Step 8 — Throughput chart\n"
    "Throughput vs concurrency — the ingest-scaling story."),
 py('''
sw = pd.DataFrame(res["metrics"]["levels"]).set_index("concurrency")
sw.plot.line(y="mbs", marker="o", figsize=(6, 3.5), legend=False)
plt.xlabel("concurrent part streams"); plt.ylabel("MB/s")
plt.title("S3 multipart write throughput vs concurrency"); plt.grid(alpha=.3)
plt.show()
spark.stop()
'''),
 notes("Scaling tapers when a host/NIC limit is hit — expected on a 2-core "
       "lab VM."),
    ])


# --------------------------------------------------------- 02_s3_throttler
def nb_02():
    return notebook([
 md("# 02 — ObjectScale S3 Throttler\n"
    "**Objective:** validate native S3 throttling — a noisy tenant is capped "
    "while a critical workload stays inside its SLA and recovers.\n\n"
    "**Customer sees:** measured phases with PASS / FAIL / INCONCLUSIVE "
    "verdicts against approved thresholds, and a live TPS/latency chart.\n"
    "**TC:** TC2.1–TC2.11\n\n"
    "**Flow:** rules are verified and stored first (throttler still "
    "disabled) so they have fully propagated before enforcement is "
    "measured → baseline (critical only, paced) → contention (+noisy, "
    "throttler disabled) → simulation (dry-run) → propagation wait with the "
    "noisy tenant paused → pre-baseline (critical alone) → reference "
    "(noisy paced at the rule limit) → enforcement (HTTP 429) → recovery "
    "→ optional enforced-24 queue probe → post-baseline (noisy stopped) → "
    "prefix rule → Spark write as the throttled tenant → restore.\n\n"
    "> ObjectScale caches the rule set for up to "
    "`rules_config_cache_expiration_time` = 300 s (read-only). The notebook "
    "therefore waits `PROPAGATION_S` (default 300 s) after storing the "
    "rules, with the noisy tenant paused so no write backlog builds up "
    "before enforcement. A verdict the evidence cannot decide is "
    "INCONCLUSIVE — never a pass.\n\n"
    "**Noisy host:** `NOISY_HOST` empty = noisy PUTs run locally on this "
    "host; `NOISY_HOST=192.168.1.41:8765` delegates only tenant-noisy "
    "control/events to `poc/noisy_agent.py` while critical GETs, phases, "
    "verdicts and the chart stay here. Remote mode refuses to run if the "
    "two host clocks differ by more than 50 ms, and re-checks the offset at "
    "every phase boundary."),
 md("## Step 1 — Session, tenant isolation, effective policy\n"
    "Prove noisy/critical are distinct IAM principals, then **read** the "
    "current throttler state. The pre-demo backup must exist before "
    "anything is changed. Any leftover load from an interrupted run is "
    "stopped first."),
 py('''
import atexit, json, os, time
import ipywidgets as w
import pandas as pd
from IPython.display import HTML, display
from poc import config, results, viz
from poc import throttle_eval as te
from poc.loadgen import LoadGen, RemoteNoisy
from poc.obs_admin import MgmtApi, Throttler
import poc.spark_helpers as sh

_old = globals().get("_POC_LG")
if _old is not None:
    _old.stop()
spark = sh.build_session("obs-poc")
viz.table(pd.DataFrame(sh.versions(spark).items(), columns=["component", "version"]))

PHASE_S = int(os.environ.get("THROTTLE_PHASE_S", "60"))
PROPAGATION_S = int(os.environ.get("PROPAGATION_S", "300"))
SETTLE_S = int(os.environ.get("THROTTLE_SETTLE_S", "120"))
CRITICAL_RATE = float(config.CRITICAL_RATE or 0)
QUEUE_TEST = bool(config.QUEUE_TEST)
DUR = {"baseline": PHASE_S, "contention": PHASE_S, "simulation": PHASE_S,
       "pre-baseline": PHASE_S, "reference": PHASE_S, "enforced": 2 * PHASE_S,
       "enforced-24": PHASE_S, "recovery": PHASE_S, "post-baseline": PHASE_S,
       "burst": max(15, PHASE_S // 3)}
RID = results.run_id()

lg = LoadGen(max_keys=0)
remote = None
if config.NOISY_HOST:
    if not config.NOISY_AGENT_TOKEN:
        raise RuntimeError("NOISY_HOST is set but NOISY_AGENT_TOKEN is empty — "
                           "the remote agent requires the shared X-POC-Token header")
    remote = RemoteNoisy(lg, config.NOISY_HOST, token=config.NOISY_AGENT_TOKEN,
                         run_id=RID, max_keys=0)
    try:
        offset, h = remote.check_clock()
        s3ok = (h.get("s3") or {}).get("ok")
        if not s3ok:
            raise RuntimeError("noisy agent cannot reach ObjectScale S3 on 9021: "
                               f"{(h.get('s3') or {}).get('error')}")
        if remote.running("tenant-noisy"):
            remote.stop_noisy()          # leftover from an interrupted run
        st = remote.status()
        if (st.get("noisy") or {}).get("running"):
            raise RuntimeError("remote noisy agent still reports running after stop")
        iso_rows, iso_ok = remote.verify_tenants()
    except Exception:
        remote.close()
        raise
else:
    iso_rows, iso_ok = lg.verify_tenants()
_POC_LG = lg
viz.table(pd.DataFrame(iso_rows))
thr = Throttler(MgmtApi())
pol = te.effective_policy(thr)
viz.table(pd.DataFrame([{"mode": pol["mode"], "response_code": pol["response_code"],
                         "existing_rules": len(pol["rules"]),
                         "backup": str(config.BACKUP_FILE),
                         "backup_exists": config.BACKUP_FILE.exists()}]))
if not config.BACKUP_FILE.exists():
    raise RuntimeError(f"{config.BACKUP_FILE} is missing — run scripts/prep_obs.py "
                       "while the throttler is in its pre-demo state")
if pol["mode"] == "enabled":
    raise RuntimeError("the throttler is already enforcing — this demo must start "
                       "from mode disabled/dry-run (restore the pre-demo state first)")
print("run id:", RID, "| phase seconds:", DUR, "| PROPAGATION_S:", PROPAGATION_S,
      "| CRITICAL_RATE:", CRITICAL_RATE, "| QUEUE_TEST:", QUEUE_TEST)
if remote is not None:
    print("noisy mode: REMOTE", remote.base_url,
          f"| U01/U02 clock offset {offset:+.1f} ms | agent S3 ok={s3ok}")
else:
    print("noisy mode: LOCAL (NOISY_HOST is empty)")
viz.banner(iso_ok, "tenant isolation",
           "distinct principals; effective policy read (unchanged)")

out = w.Output(); display(out)
chart = viz.LiveChart(lg)
PHASE_AUX = {}

def finish_phase(name):
    """Close a measured phase, settle its tail events, re-check the remote
    clock and record contact stats. Returns the aux dict."""
    end = lg.end_phase()
    aux = {"end": end, "start": None, "settle_error": None,
           "clock_offset_ms": None, "clock_rtt_ms": None, "clock_error": None,
           "remote": None, "contact_ok": None, "max_outage_s": None}
    try:
        lg.settle(end, timeout_s=30)
    except Exception as e:
        aux["settle_error"] = str(e)
    win = lg.phase_window(name) or (None, end)
    aux["start"] = win[0]
    if remote is not None:
        try:
            m = remote.recheck_clock(timeout=10)
            aux["clock_offset_ms"] = m.get("offset_ms")
            aux["clock_rtt_ms"] = m.get("rtt_ms")
        except Exception as e:
            aux["clock_error"] = str(e)
        try:
            st = remote.stats()
            aux["remote"] = {k: st.get(k) for k in
                             ("events_pulled", "poll_errors", "max_gap_s",
                              "contact_age_s", "last_error")}
            aux["contact_ok"] = bool(st.get("contact_ok"))
            if win[0]:
                aux["max_outage_s"] = remote.max_outage_between(win[0], end)
        except Exception as e:
            aux["contact_ok"] = False
            aux["contact_error"] = str(e)
    PHASE_AUX[name] = aux
    return aux

def clock_ok(name):
    a = PHASE_AUX.get(name, {})
    if a.get("clock_error"):
        return False
    off = a.get("clock_offset_ms")
    return off is None or abs(off) <= 50

def contact_reason(name):
    """None when the phase is clean, else the FAIL reason."""
    if remote is None:
        a = PHASE_AUX.get(name, {})
        return ("settle failed: " + a["settle_error"]) if a.get("settle_error") else None
    a = PHASE_AUX.get(name, {})
    if a.get("settle_error"):
        return "settle failed: " + a["settle_error"]
    if a.get("contact_ok") is False:
        err = ((a.get("remote") or {}).get("last_error")
               or a.get("contact_error") or "poll failing")
        return f"lost contact with noisy host ({err})"
    if (a.get("max_outage_s") or 0) > 5:
        return f"lost contact with noisy host ({a['max_outage_s']:.1f}s poll gap)"
    if a.get("clock_error"):
        return "clock check failed: " + a["clock_error"]
    if not clock_ok(name):
        return f"clock offset {a.get('clock_offset_ms')} ms exceeds 50 ms"
    return None

def aux_metrics(name):
    a = PHASE_AUX.get(name, {})
    return {"clock_offset_ms": a.get("clock_offset_ms"),
            "clock_rtt_ms": a.get("clock_rtt_ms"),
            "settle_error": a.get("settle_error"),
            "remote_pull": a.get("remote"),
            "max_outage_s": a.get("max_outage_s"),
            "contact_ok": a.get("contact_ok")}
'''),
 notes("Shared namespace bucket, separate principals — that is what "
       "principal-scoped rules discriminate on. LoadGen talks to S3 with "
       "SDK retries OFF so every 429 is seen as it arrives. Critical GETs "
       "are open-loop paced at CRITICAL_RATE; missed slots make a phase "
       "INCONCLUSIVE. In remote mode this cell also proves the agent "
       "reaches ObjectScale on 9021, writes as tenant-noisy, stops any "
       "leftover noisy load, and that U01/U02 clocks agree within 50 ms."),
 md("## Step 2 — Lab rules: schema verification, then store (TC2.2)\n"
    "Two rules for the noisy tenant: all PUTs capped at 50 TPS, and a "
    "tighter 10 TPS cap on one key prefix (`<run_id>/raw/*`). `verify_rule` "
    "checks both against the schema and the **live** action list. Only "
    "then are they stored — with the throttler mode left untouched "
    "(disabled), so nothing is enforced yet. The time of the push starts "
    "the propagation clock."),
 py('''
NOISY = f"urn:ecs:iam::{config.OBS_NAMESPACE}:user/tenant-noisy"
LIMIT, PREFIX_LIMIT = 50, 10
COVERED, CONTROL = f"{RID}/raw", f"{RID}/curated"
NOISY_RULE = {"id": "demo-noisy-put", "rateUnit": "TPS", "isDisabled": False,
              "filters": {"principals": [NOISY], "actions": ["s3:PutObject"]},
              "limit": {"value": LIMIT}, "scoping": {"per": ["principal"]}}
PREFIX_RULE = {"id": "demo-raw-prefix", "rateUnit": "TPS", "isDisabled": False,
               "filters": {"principals": [NOISY], "actions": ["s3:PutObject"],
                           "paths": {"matcher": "wildcard", "values": [
                               f"{config.OBS_NAMESPACE}/{config.BUCKET_THROTTLE}/{COVERED}/*"]}},
               "limit": {"value": PREFIX_LIMIT}, "scoping": {"per": ["principal"]}}
checks = {r["id"]: te.verify_rule(thr, r) for r in (NOISY_RULE, PREFIX_RULE)}
viz.table(pd.DataFrame([{"rule": rid, "schema_ok": c["ok"],
                         "problems": "; ".join(c["problems"]) or "—"}
                        for rid, c in checks.items()]))
display(HTML("<pre>" + json.dumps([NOISY_RULE, PREFIX_RULE], indent=2) + "</pre>"))
rules_ok = all(c["ok"] for c in checks.values())
res = results.write("TC2.2", "rules verified against the live schema", rules_ok,
                    {"rules": {rid: c["problems"] for rid, c in checks.items()}})
viz.banner(res["status"], "TC2.2", "rule schema + actions verified vs live API")
if not rules_ok:
    raise RuntimeError("a rule failed verification — nothing was pushed")

atexit.register(thr.restore_logged)          # restore even if a later cell dies
keep = [r for r in pol["rules"] if not str(r.get("id", "")).startswith("demo-")]
thr.put_config(keep + [NOISY_RULE, PREFIX_RULE])
PUSHED_AT = time.time()
print(f"rules stored at {time.strftime('%H:%M:%S', time.localtime(PUSHED_AT))} "
      f"(mode still {thr.get_mode()}); enforcement is measured no earlier than "
      f"{PROPAGATION_S}s after this push")
'''),
 notes("principals accept user:<name>, iam::user:<id>, or the full IAM URN. "
       "paths are <namespace>/<bucket>/<key-glob>; the wildcard spans '/'. "
       "The prefix rule is scoped per principal — scoping per 'path' would "
       "give every object key its own counter. Existing non-demo rules are "
       "kept in the pushed set."),
 md("## Step 3 — Phase 1: baseline (TC2.1)\n"
    "Critical GETs alone at the paced CRITICAL_RATE — the reference TPS "
    "and P50/P95/P99. Rules are stored but the throttler is disabled."),
 py('''
lg.start(noisy=False, critical=True, critical_threads=4,
         critical_rate=CRITICAL_RATE)
lg.start_phase("baseline"); chart.mark("baseline")
chart.refresh(out, seconds=DUR["baseline"])
aux = finish_phase("baseline")
ps_all = lg.phase_stats()
bp = ps_all["baseline"].get("tenant-critical GET", {})
bad = contact_reason("baseline")
st1 = "PASS" if (bp.get("ok", 0) >= 50 and bp.get("errors", 0) == 0
                 and bad is None) else ("FAIL" if bad else "INCONCLUSIVE")
res = results.write("TC2.1", "baseline critical GETs", status=st1,
    detail=(bad or f"{bp.get('tps', 0):.1f} TPS, p95 {bp.get('p95', 0):.0f} ms, "
                     f"p99 {bp.get('p99', 0):.0f} ms"),
    metrics={k: round(bp.get(k, 0), 2) for k in
             ("n", "tps", "p50", "p95", "p99", "errors", "throttled",
              "tenant-critical GET_paced", "tenant-critical GET_missed")}
            | {"critical_rate": CRITICAL_RATE, **aux_metrics("baseline")})
viz.banner(st1, "TC2.1 baseline", res["detail"])
'''),
 notes("Critical workload alone — the reference everything else is "
       "compared with. Latency is measured from the scheduled send time, so "
       "client stalls are counted, not hidden."),
 md("## Step 4 — Phase 2: contention, throttler disabled (TC2.3)\n"
    "Noisy PUTs are added on top of the critical GETs. The throttler is "
    "still disabled, so this is the unprotected impact the rule must fix."),
 py('''
lg.start_phase("contention"); chart.mark("contention"); lg.add_noisy(threads=6)
chart.refresh(out, seconds=DUR["contention"])
aux = finish_phase("contention")
ps_all = lg.phase_stats()
c = ps_all["contention"]
bl = ps_all["baseline"].get("tenant-critical GET", {})
ct, cn = c.get("tenant-critical GET", {}), c.get("tenant-noisy PUT", {})
bad = contact_reason("contention")
if min(ct.get("n", 0), cn.get("n", 0)) < 50:
    st3, why3 = "FAIL", "too few requests in the contention phase"
elif cn.get("throttled", 0):
    st3, why3 = "FAIL", f"{cn['throttled']} throttle responses while the throttler is disabled"
elif bad:
    st3, why3 = "FAIL", bad
else:
    st3, why3 = "PASS", (f"critical p95 {bl.get('p95', 0):.0f} -> {ct.get('p95', 0):.0f} ms "
                         f"with {cn.get('tps', 0):.0f} noisy PUT/s")
res = results.write("TC2.3", "contention (throttler disabled)", status=st3, detail=why3,
    metrics={"baseline_p95": round(bl.get("p95", 0), 1),
             "contention_p95": round(ct.get("p95", 0), 1),
             "contention_p99": round(ct.get("p99", 0), 1),
             "critical_tps": round(ct.get("tps", 0), 1),
             "noisy_ok_tps": round(cn.get("ok", 0) / max(1e-9, c["_window"]["seconds"]), 1),
             "noisy_throttled": cn.get("throttled", 0), "mode": thr.get_mode(),
             "client_cpu_pct": {"baseline": ps_all["baseline"]["_window"]["client_cpu_pct"],
                                "contention": c["_window"]["client_cpu_pct"]},
             "critical_missed_slots": c.get("tenant-critical GET_missed", 0),
             **aux_metrics("contention")})
viz.banner(st3, "TC2.3 contention", why3)
'''),
 notes("Unrestricted noisy PUTs degrade critical latency — the problem "
       "statement. Caveat for this lab: in LOCAL mode both tenants are "
       "driven from one 2-vCPU client host, and a CPU-only disturbance on "
       "that host (no S3 traffic) degrades the critical GETs just as much "
       "— so the size of this effect cannot be attributed to ObjectScale "
       "alone (reports/environment.md, 'Client-side contention'). "
       "client_cpu_pct is recorded per phase. With NOISY_HOST set, the "
       "noisy PUTs run on the second host and this client-side term is "
       "removed — compare the same phases between the two modes."),
 md("## Step 5 — Phase 3: simulation mode, dry-run (TC2.10)\n"
    "Mode **dry-run** (ObjectScale simulation mode): the server only counts "
    "rule matches. Both workloads continue — the client must see **zero** "
    "throttle responses."),
 py('''
thr.set_mode("dryrun")
lg.start_phase("simulation"); chart.mark("simulation (dry-run)")
chart.refresh(out, seconds=DUR["simulation"])
aux = finish_phase("simulation")
sp = lg.phase_stats()["simulation"]
sn = sp.get("tenant-noisy PUT", {})
srv_sim = te.server_counters(thr)
bad = contact_reason("simulation")
if sn.get("n", 0) < 50:
    st10, why10 = "INCONCLUSIVE", "too few noisy requests in the simulation phase"
elif sn.get("throttled", 0):
    st10, why10 = "FAIL", f"{sn['throttled']} client throttle responses in dry-run"
elif bad:
    st10, why10 = "FAIL", bad
else:
    st10, why10 = "PASS", (f"0 throttle responses in {sn['n']} noisy requests "
                           f"({sn.get('errors', 0)} other errors)")
res = results.write("TC2.10", "simulation mode (dry-run)", status=st10, detail=why10,
    metrics={"client_throttle_responses": sn.get("throttled", 0),
             "client_other_errors": sn.get("errors", 0),
             "noisy_ok_tps": round(sn.get("ok", 0) / max(1e-9, sp["_window"]["seconds"]), 1),
             "mode": thr.get_mode(),
             "server_metrics_available": srv_sim["available"],
             **aux_metrics("simulation")})
viz.banner(st10, "TC2.10 simulation", why10)
'''),
 notes("Dry-run validates rule matching safely. Simulated-match counters "
       "are a server-side concept: they are probed and reported as "
       "unavailable when the build does not expose them — never invented."),
 md("## Step 6 — Propagation wait + pre-baseline\n"
    "ObjectScale caches the rule set for up to 300 s. The noisy tenant is "
    "**paused** (critical GETs continue) so no write backlog builds up "
    "before enforcement. The last `PHASE_S` of the wait is measured as "
    "`pre-baseline` — a fresh critical-alone reading taken as close to "
    "enforcement as the cache allows."),
 py('''
lg.pause_noisy()
lg.start_phase("propagation"); chart.mark("propagation wait")
countdown = w.HTML(); display(countdown)
prop_end = PUSHED_AT + PROPAGATION_S - PHASE_S
while (left := prop_end - time.time()) > 0:
    countdown.value = (f"<b>rule propagation: {left:.0f}s left</b> of {PROPAGATION_S}s "
                       f"(noisy paused; critical still running, dry-run)")
    chart.refresh(out, seconds=min(10, left))
aux_prop = finish_phase("propagation")

lg.start_phase("pre-baseline"); chart.mark("pre-baseline (noisy paused)")
chart.refresh(out, seconds=DUR["pre-baseline"])
aux = finish_phase("pre-baseline")
RULE_AGE_S = round(time.time() - PUSHED_AT, 1)
countdown.value = (f"<b>rules are {RULE_AGE_S:.0f}s old</b> — propagation wait "
                   f"complete; pre-baseline p95 recorded</b>")
print(f"rules stored {RULE_AGE_S:.0f}s ago (>= PROPAGATION_S={PROPAGATION_S}s) — "
      f"enforcement can now be measured")
'''),
 notes("The countdown is live in JupyterLab; headless runs print the final "
       "line. Pausing the noisy tenant during the wait keeps the pre-"
       "enforcement queue empty, so the reference and recovery phases "
       "measure the throttler rather than a backlog."),
 md("## Step 7 — Reference: noisy paced at the rule limit\n"
    "The noisy tenant resumes at exactly `LIMIT` PUT/s (open-loop paced) "
    "while the throttler is still in dry-run. This is the best result any "
    "throttler could give: the critical workload sharing the system with "
    "the maximum admissible noisy rate. Recovery is judged against it."),
 py('''
lg.resume_noisy(); lg.set_noisy_rate(LIMIT)
lg.start_phase("reference"); chart.mark("reference (noisy @ limit, dry-run)")
chart.refresh(out, seconds=DUR["reference"])
aux = finish_phase("reference")
ps_all = lg.phase_stats()
rf = ps_all["reference"].get("tenant-critical GET", {})
rn = ps_all["reference"].get("tenant-noisy PUT", {})
print(f"reference: critical p95 {rf.get('p95', 0):.1f} ms, noisy ok "
      f"{rn.get('ok', 0) / max(1e-9, ps_all['reference']['_window']['seconds']):.1f} TPS")
'''),
 notes("No verdict is written for this phase — it is the comparison "
       "baseline for recovery_vs_reference in TC2.9."),
 md("## Step 8 — Phase 4: enforcement, HTTP 429 (TC2.4)\n"
    "Mode **enabled**, response code 429, noisy back to as-fast-as-"
    "possible 6 threads. The switch is read back, the notebook waits until "
    "the new mode is visibly in effect (tagged `switching`), then measures "
    "the `enforced` phase with both workloads running."),
 py('''
lg.set_noisy_rate(None)
thr.set_response_code(429); thr.set_mode("enabled")
time.sleep(2)                                   # let the config write settle
eff = {"mode": thr.get_mode(), "response_code": thr.get_response_code(),
       "rules": [r.get("id") for r in thr.get_rules()]}
assert eff["mode"] == "enabled" and eff["response_code"] == 429, eff
lg.start_phase("switching"); chart.mark("mode -> enabled")
t_eff = te.wait_effective(lg, LIMIT, timeout_s=SETTLE_S)
print("mode switch visible after", t_eff, "s" if t_eff is not None
      else f"— NOT visible within {SETTLE_S}s; measuring anyway")

lg.start_phase("enforced"); chart.mark("enforced (429)")
half = DUR["enforced"] // 2
chart.refresh(out, seconds=half)
sustained_mid = lg.sustained(10, LIMIT * 0.25)
chart.refresh(out, seconds=DUR["enforced"] - half)
sustained_end = lg.sustained(10, LIMIT * 0.25)
aux = finish_phase("enforced")

ps_all = lg.phase_stats()
ep, cp = ps_all["enforced"], ps_all["contention"]
en, ec = ep.get("tenant-noisy PUT", {}), ep.get("tenant-critical GET", {})
st4, why4, enforced, enf_mode = te.tc24_status(
    ps_all, LIMIT, sustained=sustained_mid and sustained_end)
bad = contact_reason("enforced")
if bad:
    st4, why4 = "FAIL", bad
sig = te.signals(ps_all, LIMIT)
res = results.write("TC2.4", "enforcement (HTTP 429)", status=st4, detail=why4,
    metrics={"noisy": {k: round(en.get(k, 0), 2) for k in
                       ("n", "ok", "throttled", "r429", "r503_slowdown", "r503_other",
                        "errors", "retries_ok", "retries_fail", "boundary",
                        "ok_p50", "ok_p95", "ok_p99", "fail_p50", "fail_p95")},
             "noisy_ok_tps_before": round(sig["ok_tps_before"], 1),
             "noisy_ok_tps_enforced": round(sig["ok_tps"], 1),
             "limit_tps": LIMIT, "codes": en.get("codes", {}),
             "critical_tps": round(ec.get("tps", 0), 1),
             "critical_p95": round(ec.get("p95", 0), 1),
             "critical_p99": round(ec.get("p99", 0), 1),
             "critical_throttled": ec.get("throttled", 0),
             "critical_errors": ec.get("errors", 0),
             "critical_missed_slots": ep.get("tenant-critical GET_missed", 0),
             "sustained_mid": sustained_mid, "sustained_end": sustained_end,
             "rule_age_at_enable_s": RULE_AGE_S, "seconds_to_effect": t_eff,
             "effective_config": eff,
             "enforcement_active": enforced, "enforcement_mode": enf_mode,
             **aux_metrics("enforced")})
viz.banner(st4, "TC2.4 enforcement", why4)
'''),
 notes("With client retries off ObjectScale shows both mechanisms: it "
       "queues requests to hold the accepted rate at the limit (latency "
       "tail grows) and rejects the overflow with 429 TooManyRequests. "
       "codes{} lists the S3 error codes exactly as received."),
 md("## Step 9 — Phase 5: recovery observation\n"
    "**No configuration change here** — enforcement from Step 8 stays "
    "active and the noisy tenant keeps offering as-fast-as-possible. This "
    "phase measures critical latency while noisy is capped at the limit; "
    "the verdict is written after post-baseline so the drift guard has "
    "both critical-alone readings."),
 py('''
lg.start_phase("recovery"); chart.mark("recovery (enforcement continues)")
chart.refresh(out, seconds=DUR["recovery"])
aux = finish_phase("recovery")
print("recovery measured; verdict written after post-baseline")
'''),
 notes("The mechanism is the already-running enforcement, observed longer "
       "— not a second action. The rule still admits ~50 noisy PUT/s by "
       "design, so the critical workload shares the system with that "
       "admitted load."),
 md("## Step 10 — Optional queue-cost probe: enforced-24 (QUEUE_TEST=1)\n"
    "Same as the enforced phase but with **24** noisy threads instead of 6. "
    "If critical p95 gets clearly worse while accepted noisy TPS stays at "
    "the limit, requests waiting in ObjectScale's throttle queue are "
    "costing the other tenant — reported as a product finding."),
 py('''
if QUEUE_TEST:
    lg.stop_noisy(); lg.add_noisy(threads=24)
    lg.start_phase("enforced-24"); chart.mark("enforced-24 (queue probe)")
    chart.refresh(out, seconds=DUR["enforced-24"])
    aux24 = finish_phase("enforced-24")
    ps24 = lg.phase_stats()["enforced-24"]
    c24, n24 = ps24.get("tenant-critical GET", {}), ps24.get("tenant-noisy PUT", {})
    print(f"enforced-24: critical p95 {c24.get('p95', 0):.1f} ms, noisy ok "
          f"{n24.get('ok', 0) / max(1e-9, ps24['_window']['seconds']):.1f} TPS")
else:
    print("QUEUE_TEST=0 — skipping enforced-24")
'''),
 notes("Disabled by default. Enable with QUEUE_TEST=1 for the queue-cost "
       "experiment."),
 md("## Step 11 — Post-baseline + recovery verdict (TC2.5)\n"
    "The noisy tenant is **stopped** while enforcement stays on: a second "
    "critical-alone reading that detects environment drift. TC2.5 is "
    "written here so `baseline_stable` and `recovery_vs_reference` have "
    "every phase they need."),
 py('''
lg.stop_noisy()
lg.start_phase("post-baseline"); chart.mark("post-baseline (noisy stopped)")
chart.refresh(out, seconds=DUR["post-baseline"])
aux = finish_phase("post-baseline")
ps_all = lg.phase_stats()
verdicts = te.evaluate(ps_all, rule=NOISY_RULE)
REC_CHECKS = {"recovery_vs_reference", "baseline_stable",
              "critical_p95_within_sla", "paced_client_kept_up",
              "noisy_sustained_in_recovery", "critical_not_throttled"}
st5 = te.combine(verdicts["verdicts"], REC_CHECKS)
why5 = "; ".join(f"{v['name']}={v['verdict']}" for v in verdicts["verdicts"]
                 if v["name"] in REC_CHECKS)
contact_fails = {p: contact_reason(p) for p in
                 ("baseline", "contention", "simulation", "pre-baseline",
                  "reference", "enforced", "recovery", "enforced-24",
                  "post-baseline") if p in PHASE_AUX}
contact_fails = {p: r for p, r in contact_fails.items() if r}
if contact_fails:
    st5 = "FAIL"
    why5 = (why5 + "; " if why5 else "") + "; ".join(
        f"{p}: {r}" for p, r in contact_fails.items())
rp = ps_all.get("recovery", {})
rc, rn = rp.get("tenant-critical GET", {}), rp.get("tenant-noisy PUT", {})
rec_span = max(1e-9, rp.get("_window", {}).get("seconds", 1))
res = results.write("TC2.5", "recovery observation under sustained throttling",
    status=st5, detail=why5,
    metrics={"baseline_p95": round(ps_all["baseline"].get("tenant-critical GET", {}).get("p95", 0), 1),
             "pre_baseline_p95": round(ps_all.get("pre-baseline", {}).get("tenant-critical GET", {}).get("p95", 0), 1),
             "reference_p95": round(ps_all.get("reference", {}).get("tenant-critical GET", {}).get("p95", 0), 1),
             "post_baseline_p95": round(ps_all.get("post-baseline", {}).get("tenant-critical GET", {}).get("p95", 0), 1),
             "contention_p95": round(ps_all["contention"].get("tenant-critical GET", {}).get("p95", 0), 1),
             "critical_p95": round(rc.get("p95", 0), 1),
             "critical_p99": round(rc.get("p99", 0), 1),
             "critical_tps": round(rc.get("tps", 0), 1),
             "critical_errors": rc.get("errors", 0),
             "critical_throttled": rc.get("throttled", 0),
             "noisy_offered_tps": round(rn.get("n", 0) / rec_span, 1),
             "noisy_ok_tps": round(rn.get("ok", 0) / rec_span, 1),
             "noisy_throttled": rn.get("throttled", 0),
             "client_cpu_pct": {ph: ps_all[ph]["_window"]["client_cpu_pct"]
                                for ph in ("baseline", "contention", "reference",
                                           "enforced", "recovery", "post-baseline")
                                if ph in ps_all},
             "checks": {v["name"]: v["detail"] for v in verdicts["verdicts"]
                        if v["name"] in REC_CHECKS},
             "phase_aux": {p: aux_metrics(p) for p in PHASE_AUX}})
viz.banner(st5, "TC2.5 recovery",
           f"critical p95 {rc.get('p95', 0):.0f} ms (baseline "
           f"{res['metrics']['baseline_p95']:.0f}, reference "
           f"{res['metrics']['reference_p95']:.0f}); noisy still offering "
           f"{rn.get('n', 0) / rec_span:.0f} req/s")
'''),
 notes("The mechanism is the already-running enforcement, observed longer "
       "— not a second action. recovery_vs_reference is the scored "
       "isolation verdict; recovery_p95_vs_baseline is information only "
       "because single-host CPU noise made it unfair."),
 md("## Step 12 — Prefix-scoped rule (TC2.6)\n"
    "The second rule caps the noisy tenant at 10 TPS under "
    "`<run_id>/raw/*` only. Real traffic decides: one PUT burst to the "
    "covered prefix and one to a control prefix (`<run_id>/curated/`). "
    "PASS only if the covered prefix is held at its limit while the "
    "control prefix runs clearly above it."),
 py('''
lg.pause_noisy(); time.sleep(3)
lg.start_phase("prefix-test"); chart.mark("prefix bursts")
cov = lg.noisy_burst(config.BUCKET_THROTTLE, COVERED, seconds=DUR["burst"], threads=4)
ctl = lg.noisy_burst(config.BUCKET_THROTTLE, CONTROL, seconds=DUR["burst"], threads=4)
pfx = te.evaluate_prefix(cov, ctl, PREFIX_LIMIT)
stored = [r.get("id") for r in thr.get_rules()]
if PREFIX_RULE["id"] not in stored:
    pfx = {"status": "FAIL", "reason": f"prefix rule not stored on the server: {stored}"}
cols = ("n", "ok", "ok_tps", "throttled", "r429", "errors", "ok_p95", "seconds")
viz.table(pd.DataFrame([{"prefix": "covered  " + COVERED, **{k: round(cov[k], 1) for k in cols}},
                        {"prefix": "control  " + CONTROL, **{k: round(ctl[k], 1) for k in cols}}]))
aux = finish_phase("prefix-test")
res = results.write("TC2.6", "prefix-scoped rule", status=pfx["status"], detail=pfx["reason"],
    metrics={"prefix_limit_tps": PREFIX_LIMIT, "tenant_limit_tps": LIMIT,
             "rule_path": PREFIX_RULE["filters"]["paths"]["values"][0],
             "covered": {k: round(cov[k], 1) for k in cols},
             "control": {k: round(ctl[k], 1) for k in cols},
             "covered_codes": cov["codes"], "control_codes": ctl["codes"],
             **aux_metrics("prefix-test")})
viz.banner(pfx["status"], "TC2.6 prefix rule", pfx["reason"])
lg.resume_noisy()
'''),
 notes("Both bursts use the tenant-noisy identity with retries off. The "
       "control prefix is still subject to the tenant-wide rule, so it is "
       "expected near 50 TPS, the covered prefix near 10."),
 md("## Step 13 — Spark write as the throttled tenant (TC2.7)\n"
    "The same Spark job runs twice while the noisy load keeps the tenant at "
    "its limit: first as **poc-admin** (not targeted by the rule), then — "
    "in a session rebuilt with the **tenant-noisy** S3A identity — under "
    "the enforced rule. PASS: the throttled job completes and throttling "
    "was demonstrably active while it ran."),
 py('''
lg.add_noisy(threads=6)
lg.start_phase("spark-write"); chart.mark("spark writes")

def spark_write(session, label):
    target = f"{RID}/spark-write-{label}"
    t0 = time.time()
    session.range(200_000).repartition(20).write.mode("overwrite").json(
        f"s3a://{config.BUCKET_THROTTLE}/{target}")
    return round(time.time() - t0, 1), target

t_admin, _ = spark_write(spark, "admin")
spark.stop()
spark = sh.build_session("obs-poc-noisy", s3a_user="noisy")
ident = sh.identities(spark)
t_start = time.time()
try:
    t_noisy, target = spark_write(spark, "noisy"); job_error = None
except Exception as e:
    t_noisy, target, job_error = None, f"{RID}/spark-write-noisy", str(e).splitlines()[0][:300]
during = lg.snapshot(since=t_start).get("tenant-noisy", {}).get("PUT", {})
s3a_stats = sh.s3a_committer_stats(config.BUCKET_THROTTLE, target) if not job_error else {}
s3a_throttled = sum(v for k, v in s3a_stats.items()
                    if "throttl" in k and isinstance(v, (int, float)))
mode_now = thr.get_mode()
aux = finish_phase("spark-write")
if job_error:
    st7, why7 = "FAIL", f"Spark write as tenant-noisy failed: {job_error}"
elif ident["s3a"] != "noisy":
    st7, why7 = "FAIL", f"session S3A identity is {ident['s3a']}, not tenant-noisy"
elif mode_now != "enabled" or not (during.get("throttled", 0) or s3a_throttled):
    st7, why7 = "INCONCLUSIVE", (f"job completed in {t_noisy}s, but no throttling was "
                                 f"observed for tenant-noisy while it ran (mode={mode_now})")
elif contact_reason("spark-write"):
    st7, why7 = "FAIL", contact_reason("spark-write")
else:
    st7, why7 = "PASS", (f"completed under enforcement in {t_noisy}s vs {t_admin}s as "
                         f"poc-admin ({t_noisy / max(t_admin, 0.1):.1f}x); "
                         f"{during.get('throttled', 0)} tenant-noisy 429s during the job")
res = results.write("TC2.7", "spark write as throttled tenant", status=st7, detail=why7,
    metrics={"seconds_admin_unthrottled": t_admin, "seconds_noisy_throttled": t_noisy,
             "slowdown": round(t_noisy / max(t_admin, 0.1), 2) if t_noisy else None,
             "s3a_identity": ident["s3a"], "s3a_key": ident["s3a_key"],
             "mode": mode_now,
             "tenant_noisy_429_during_job": during.get("throttled", 0),
             "tenant_noisy_ok_tps_during_job": round(
                 during.get("ok", 0) / max(1e-9, time.time() - t_start), 1),
             "s3a_committer_stats": s3a_stats,
             **aux_metrics("spark-write")})
viz.table(pd.DataFrame([{"identity": "poc-admin (not in the rule)", "seconds": t_admin},
                        {"identity": "tenant-noisy (rule enforced)", "seconds": t_noisy}]))
viz.banner(st7, "TC2.7 Spark under throttling", why7)
'''),
 notes("The S3A identity is set per session (fs.s3a.access.key), verified "
       "by build_session. S3A retries throttled requests "
       "(fs.s3a.retry.throttle.*), so the job slows down instead of "
       "failing. The 429 count is the tenant's LoadGen view during the "
       "job; S3A's own counters are shown when the committer reports them."),
 md("## Step 14 — Stop the load: worker lifecycle (TC2.11)\n"
    "Stop and restart the noisy group once to prove it restarts safely, "
    "then stop every worker. lg.stop() also does the final remote flush so "
    "no tail events are dropped."),
 py('''
leftover = lg.stop_noisy()
restarted = lg.add_noisy(1) == 1
time.sleep(3)
restarted = restarted and lg.running("tenant-noisy")
leftover += lg.stop()
res = results.write("TC2.11", "worker lifecycle", restarted and leftover == 0,
                    {"noisy_restartable": restarted, "lingering_workers": leftover,
                     "settle_error": lg.last_settle_error})
viz.banner(res["status"], "TC2.11 worker lifecycle",
           "restart ok, no lingering workers" if res["passed"]
           else f"restartable={restarted}, lingering={leftover}")
'''),
 notes("stop() joins every worker; a worker that outlives its join timeout "
       "is kept in the count instead of being forgotten. In remote mode the "
       "same calls go to the agent on the noisy host and lg.stop() also "
       "closes the event poller."),
 md("## Step 15 — Verdicts + server-side correlation (TC2.9)\n"
    "Score all phases against the approved thresholds and correlate the "
    "client-side throttle responses with ObjectScale's own counters where "
    "the build exposes them. The status **is** the overall verdict — "
    "INCONCLUSIVE is not a pass."),
 py('''
ps_all = lg.phase_stats()
verdicts = te.evaluate(ps_all, rule=NOISY_RULE)
srv = te.server_counters(thr)
rows = [{"check": v["name"], "verdict": v["verdict"], "detail": v["detail"]}
        for v in verdicts["verdicts"]]
queue_test = None
if "enforced-24" in ps_all:
    e6, e24 = ps_all.get("enforced", {}), ps_all["enforced-24"]
    c6, c24 = e6.get("tenant-critical GET", {}), e24.get("tenant-critical GET", {})
    n6, n24 = e6.get("tenant-noisy PUT", {}), e24.get("tenant-noisy PUT", {})
    s6, s24 = e6.get("_window", {}).get("seconds", 1), e24.get("_window", {}).get("seconds", 1)
    queue_test = {"critical_p95_6_threads": round(c6.get("p95", 0), 1),
                  "critical_p95_24_threads": round(c24.get("p95", 0), 1),
                  "noisy_ok_tps_6": round(n6.get("ok", 0) / max(1e-9, s6), 1),
                  "noisy_ok_tps_24": round(n24.get("ok", 0) / max(1e-9, s24), 1),
                  "critical_missed_24": e24.get("tenant-critical GET_missed", 0)}
    print("queue probe:", queue_test)
contact_fails = {p: contact_reason(p) for p in PHASE_AUX}
contact_fails = {p: r for p, r in contact_fails.items() if r}
if contact_fails:
    verdicts["overall"] = "FAIL"
    rows.append({"check": "remote_contact_or_clock", "verdict": "FAIL",
                 "detail": "; ".join(f"{p}: {r}" for p, r in contact_fails.items())})
display(pd.DataFrame(rows).style.apply(viz.status_style, column="verdict", axis=1))
corr = {"client_throttled_enforced_phase": ps_all.get("enforced", {}).get(
            "tenant-noisy PUT", {}).get("throttled", 0),
        "server_metrics_available": srv["available"], "probed": srv["probed"]}
if srv["available"]:
    corr["server_data"] = srv["data"]
else:
    print("server-side throttle counters are not exposed on this build — "
          "client-side observations only")
res = results.write("TC2.9", "verdicts + correlation", status=verdicts["overall"],
                    metrics={"overall": verdicts["overall"],
                             "enforcement_mode": verdicts["enforcement_mode"],
                             "thresholds": verdicts["thresholds"],
                             "verdicts": rows, **corr,
                             "queue_test": queue_test,
                             "clock_offsets": {p: a.get("clock_offset_ms")
                                               for p, a in PHASE_AUX.items()},
                             "contact_failures": contact_fails})
viz.banner(verdicts["overall"], "TC2.9 overall verdict",
           f"enforcement mode: {verdicts['enforcement_mode']}")
'''),
 notes("A 50 TPS limit does not yield exactly 50 ok/s — the thresholds "
       "carry a stated tolerance. They are customer-approved values in "
       "te.DEFAULT_THRESHOLDS, not tuned to the result. recovery_vs_"
       "reference is the scored isolation verdict; recovery_p95_vs_"
       "baseline is information only."),
 md("## Step 16 — Phase summary\n"
    "Every phase side by side: request counts, TPS, latencies, throttle "
    "responses, errors, paced slots and client CPU."),
 py('''
def _win(gs):
    w = gs.get("_window")
    return w if isinstance(w, dict) else {}
summary = [{"phase": ph, "client_cpu_pct": _win(gs).get("client_cpu_pct"),
            "workload": k, "n": v.get("n"), "tps": round(v.get("tps", 0), 1),
            "ok_p50": round(v.get("ok_p50", 0)), "ok_p95": round(v.get("ok_p95", 0)),
            "ok_p99": round(v.get("ok_p99", 0)), "throttled": v.get("throttled", 0),
            "r429": v.get("r429", 0), "errors": v.get("errors", 0),
            "boundary": v.get("boundary", 0),
            "paced": gs.get("tenant-critical GET_paced"),
            "missed": gs.get("tenant-critical GET_missed")}
           for ph, gs in ps_all.items() for k, v in gs.items()
           if k != "_window" and isinstance(v, dict) and "n" in v]
viz.table(pd.DataFrame(summary))
chart.refresh(out, seconds=2)
'''),
 notes("client_cpu_pct = CPU utilisation of the load-generator host in "
       "that phase. Shaded bands mark the phases; the dotted line is the throttled "
       "(429) rate. `boundary` counts requests that were still in flight "
       "when their phase ended."),
 md("## Step 17 — Restore (TC2.8)\n"
    "Restore the pre-demo throttler backup and verify the live state "
    "against the backup file — mode, response code and rule IDs."),
 py('''
backup = json.loads(config.BACKUP_FILE.read_text())
want = {"mode": backup["mode"], "response_code": int(backup["response_code"]),
        "rules": sorted(r.get("id") for r in backup["config"].get("throttle", {}).get("rules", []))}
thr.restore()
live = {"mode": thr.get_mode(), "response_code": thr.get_response_code(),
        "rules": sorted(r.get("id") for r in thr.get_rules())}
res = results.write("TC2.8", "restore pre-demo throttler state", live == want,
                    {"live": live, "backup": want})
viz.banner(res["status"], "TC2.8 restore",
           f"mode={live['mode']}, response_code={live['response_code']}, "
           f"rules={live['rules'] or 'none'}")
spark.stop()
'''),
 notes("Backup: reports/throttle_backup.json — written once by "
       "scripts/prep_obs.py from a clean pre-demo state. The restore is "
       "also registered at kernel exit."),
    ])


# --------------------------------------------------- 03_s3_tables_iceberg
def nb_03():
    return notebook([
 md("# 03 — S3 Tables (Apache Iceberg)\n"
    "**Objective:** native Iceberg tables on ObjectScale via Spark's REST "
    "catalog — no data movement.\n\n"
    "**Customer sees:** table bucket + namespace, DDL/DML, MERGE, schema "
    "evolution, time travel, and a read-only denial.\n**TC:** TC3.1–TC3.7"),
 md("## Step 1 — Session\nSparkSession with the `os` REST catalog pointed "
    "at the ObjectScale TableHead endpoint (SigV4, table bucket ARN as "
    "prefix). The catalog identity is passed explicitly per session."),
 py('''
import pandas as pd
from poc import config, results, s3util, viz
import poc.spark_helpers as sh

spark = sh.build_session("obs-poc", tables_user="tables-rw")
viz.table(pd.DataFrame(sh.versions(spark).items(), columns=["component", "version"]))
print("run id:", results.run_id(), "| session identities:", sh.identities(spark))
print("catalog os ->", config.ICEBERG_REST_URI, "| warehouse", config.ICEBERG_WAREHOUSE)
T = f"os.{config.TABLES_NAMESPACE}.orders_ice"
'''),
 notes("Catalog `os`: Iceberg REST on the TableHead endpoint (HTTPS 19021), "
       "warehouse = the poc-lakehouse table bucket ARN."),
 md("## Step 2 — Table bucket & namespace via S3 Tables API (TC3.1)\n"
    "Confirm the table bucket and namespace through the AWS-compatible API "
    "and show the responses."),
 py('''
st = s3util.s3tables("tables-rw")
arn = config.table_bucket_arn()
tbl = st.list_table_buckets()["tableBuckets"]
namespaces = st.list_namespaces(tableBucketARN=arn).get("namespaces", [])
viz.table(pd.DataFrame(tbl)); viz.table(pd.DataFrame(namespaces)[["namespace"]])
has_bucket = any(b.get("name") == config.TABLE_BUCKET for b in tbl)
has_ns = any(n["namespace"] == [config.TABLES_NAMESPACE] for n in namespaces)
res = results.write("TC3.1", "bucket+namespace", has_bucket and has_ns,
                    {"table_bucket": config.TABLE_BUCKET, "found": has_bucket,
                     "namespace": config.TABLES_NAMESPACE, "ns_found": has_ns})
viz.banner(res["status"], "TC3.1",
           f"bucket={config.TABLE_BUCKET} ns={config.TABLES_NAMESPACE}")
'''),
 notes("boto3 s3tables client + x-emc-namespace header."),
 md("## Step 3 — SHOW NAMESPACES + CREATE TABLE (TC3.2)\n"
    "The same objects are visible through Spark's Iceberg catalog — one "
    "control plane, no ETL."),
 py('''
spark.sql("SHOW NAMESPACES IN os").show()
spark.sql(f"DROP TABLE IF EXISTS {T}")
spark.sql(f"""CREATE TABLE {T}
 (order_id string, customer string, status string, amount double, qty int)
 USING iceberg""")
created = "orders_ice" in [r["tableName"] for r in
                           spark.sql(f"SHOW TABLES IN os.{config.TABLES_NAMESPACE}").collect()]
res = results.write("TC3.2", "create table", created, {"table": T})
viz.banner(res["status"], "TC3.2", f"{T} created")
'''),
 notes("SparkCatalog 'os' — type=rest, SigV4-signed."),
 md("## Step 4 — INSERT + SELECT (TC3.3)\n"
    "Insert rows through the Iceberg catalog; they land in the managed "
    "`--table-s3` bucket on ObjectScale."),
 py('''
spark.sql(f"""INSERT INTO {T}
 VALUES ('o1','cust-001','PAID',199.0,2),('o2','cust-002','NEW',49.5,1),
        ('o3','cust-003','SHIPPED',1200.0,10)""")
rows = spark.sql(f"SELECT * FROM {T} ORDER BY order_id").toPandas()
viz.table(rows)
res = results.write("TC3.3", "insert+select", len(rows) == 3, {"rows": len(rows)})
viz.banner(res["status"], "TC3.3", f"{len(rows)} rows")
'''),
 notes("Data files are visible in the OBS UI under <table-uuid>--table-s3."),
 md("## Step 5 — MERGE INTO (TC3.4)\n"
    "Upsert a changed row — Iceberg's row-level update over object "
    "storage, before and after side by side."),
 py('''
before = rows.copy()
spark.sql(f"""MERGE INTO {T} t
 USING (SELECT 'o2' order_id,'cust-002' customer,'PAID' status,55.0 amount,1 qty) u
 ON t.order_id=u.order_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
after = spark.sql(f"SELECT * FROM {T} ORDER BY order_id").toPandas()
viz.table(before.assign(state="before")); viz.table(after.assign(state="after"))
merged = (len(after) == 3
          and after.loc[after.order_id == "o2", "status"].iloc[0] == "PAID")
res = results.write("TC3.4", "merge", merged, {"rows_after": len(after)})
viz.banner(res["status"], "TC3.4", "o2 NEW -> PAID")
'''),
 notes("MERGE INTO needs the Iceberg SQL extensions — already configured."),
 md("## Step 6 — Schema evolution (TC3.5)\n"
    "Add a column without rewriting data."),
 py('''
spark.sql(f"ALTER TABLE {T} ADD COLUMNS (region string)")
cols = spark.sql(f"DESCRIBE {T}").toPandas()
viz.table(cols)
res = results.write("TC3.5", "add column", "region" in set(cols.col_name))
viz.banner(res["status"], "TC3.5", "column region added")
'''),
 notes("Metadata-only operation — instant."),
 md("## Step 7 — Time travel & snapshot history (TC3.6)\n"
    "Query the first snapshot vs the current state, and show the snapshot "
    "log."),
 py('''
snaps = spark.sql(f"SELECT snapshot_id, committed_at FROM {T}.snapshots ORDER BY committed_at").toPandas()
old = spark.sql(f"SELECT status FROM {T} VERSION AS OF {snaps.snapshot_id.iloc[0]} WHERE order_id='o2'").toPandas()
cur = spark.sql(f"SELECT status FROM {T} WHERE order_id='o2'").toPandas()
viz.table(snaps)
viz.table(pd.concat([old.assign(view="first snapshot"), cur.assign(view="current")]))
travelled = (len(snaps) >= 2 and old.status.iloc[0] == "NEW"
             and cur.status.iloc[0] == "PAID")
res = results.write("TC3.6", "time travel", travelled,
                    {"snapshots": len(snaps), "o2_first": old.status.iloc[0],
                     "o2_current": cur.status.iloc[0]})
viz.banner(res["status"], "TC3.6",
           f"{len(snaps)} snapshots; o2 was {old.status.iloc[0]}, is {cur.status.iloc[0]}")
'''),
 notes("VERSION AS OF uses snapshot_id; TIMESTAMP AS OF is also supported."),
 md("## Step 8 — Read-only denial (TC3.7)\n"
    "One full, valid INSERT is run twice. As **tables-rw** it must succeed "
    "(positive control). Then the session is rebuilt with the "
    "**tables-ro** identity: SELECT must still work and the *same* INSERT "
    "must fail with an access denial. PASS only if the error really is a "
    "permission error and no row was added."),
 py('''
INSERT = f"INSERT INTO {T} VALUES ('o9','cust-009','NEW',10.0,1,'EU')"
count = lambda s: s.sql(f"SELECT count(*) FROM {T}").first()[0]

n0 = count(spark)
spark.sql(INSERT)                                  # positive control: tables-rw
n1 = count(spark)
control_ok = n1 == n0 + 1
spark.stop()

spark = sh.build_session("obs-poc-ro", tables_user="tables-ro")
ident = sh.identities(spark)
try:
    ro_rows, ro_select_error = count(spark), None
except Exception as e:
    ro_rows, ro_select_error = None, str(e).splitlines()[0][:200]
try:
    spark.sql(INSERT)
    inserted, denial, first_line = True, None, "INSERT succeeded"
except Exception as e:
    inserted, denial = False, sh.access_denied_reason(e)
    first_line = str(e).strip().splitlines()[0][:200]
n2 = count(spark) if ro_select_error is None else None

if inserted or (n2 is not None and n2 != n1):
    st37, why37 = "FAIL", f"the read-only identity was able to write ({n1} -> {n2} rows)"
elif ident["tables"] != "tables-ro":
    st37, why37 = "INCONCLUSIVE", f"session ran as {ident['tables']}, not tables-ro"
elif not control_ok:
    st37, why37 = "INCONCLUSIVE", "positive control failed: the INSERT did not succeed as tables-rw"
elif ro_select_error:
    st37, why37 = "INCONCLUSIVE", f"tables-ro could not even read: {ro_select_error}"
elif not denial:
    st37, why37 = "INCONCLUSIVE", f"INSERT failed, but not with an access denial: {first_line}"
else:
    st37, why37 = "PASS", f"tables-ro reads {ro_rows} rows; INSERT denied: {denial}"
res = results.write("TC3.7", "read-only identity denied", status=st37, detail=why37,
    metrics={"identity": ident["tables"], "identity_key": ident["tables_key"],
             "rw_control_insert_ok": control_ok, "ro_select_rows": ro_rows,
             "rows_before_ro_insert": n1, "rows_after_ro_insert": n2,
             "ro_insert_error": denial or first_line})
viz.table(pd.DataFrame([
    {"identity": "tables-rw", "statement": "INSERT", "result": "OK" if control_ok else "FAILED"},
    {"identity": ident["tables"], "statement": "SELECT count(*)",
     "result": ro_rows if ro_select_error is None else ro_select_error},
    {"identity": ident["tables"], "statement": "INSERT", "result": denial or first_line}]))
viz.banner(st37, "TC3.7 read-only denial", why37)
'''),
 notes("tables-ro: S3TablesReadOnlyAccess + ECSS3ReadOnlyAccess. The "
       "identity is switched through catalog properties "
       "(rest.access-key-id / s3.access-key-id), not environment variables, "
       "and build_session verifies the running session really has it."),
 md("## Step 9 — List tables via API\n"
    "Back to the control plane: list tables in the namespace via the S3 "
    "Tables API."),
 py('''
tabs = st.list_tables(tableBucketARN=arn, namespace=config.TABLES_NAMESPACE)["tables"]
viz.table(pd.DataFrame(tabs)[["name", "type"]])
print(f"{len(tabs)} table(s) in {config.TABLES_NAMESPACE}")
spark.stop()
'''),
 notes("The same table list is visible in the OBS UI under S3 Tables."),
    ])


# -------------------------------------------------------------- 04_summary
def nb_04():
    return notebook([
 md("# 04 — Summary & Scorecard\n"
    "**Objective:** pull every test-case result of **this run** into an "
    "O1–O3 objective scorecard.\n\n"
    "**Customer sees:** four possible states per objective — "
    "PASS (green), FAIL (red), INCONCLUSIVE (amber: the evidence does not "
    "decide) and INCOMPLETE (grey: a test case of this run is missing)."),
 md("## Step 1 — Results of the current run"),
 py('''
import pandas as pd
from IPython.display import display
from poc import results, viz

RID = results.run_id()
allr = results.load_run(RID)
print("run id:", RID, "|", len(allr), "test-case results in this run")
'''),
 md("## Step 2 — Objective scorecard\n"
    "O1 Spark↔S3, O2 Throttler, O3 S3 Tables — each mapped to its test "
    "cases. Results from other runs are ignored (listed as stale)."),
 py('''
sc = pd.DataFrame(results.scorecard(RID))
display(sc[["objective", "description", "status", "test_cases", "pass", "fail",
            "inconclusive", "missing", "not_pass"]]
        .style.apply(viz.status_style, axis=1))
'''),
 notes("Rule: any missing test case -> INCOMPLETE; else any FAIL -> FAIL; "
       "else any INCONCLUSIVE -> INCONCLUSIVE; else PASS."),
 md("## Step 3 — Test cases\n"
    "Every test case of this run with its status and reason."),
 py('''
tcs = pd.DataFrame([{"tc": tc, "name": r["name"], "status": results.status_of(r),
                     "detail": r.get("detail") or ""}
                    for tc, r in sorted(allr.items(),
                                        key=lambda kv: [float(x) for x in kv[0][2:].split(".")])])
display(tcs.style.apply(viz.status_style, axis=1))
'''),
 md("## Step 4 — Key metrics\n"
    "Headline numbers pulled from the raw results."),
 py('''
kv = []
for tc in ("TC1.6", "TC2.4", "TC2.5", "TC2.6", "TC2.7", "TC3.7"):
    r = allr.get(tc)
    if r:
        kv.append({"tc": tc, **{k: v for k, v in r["metrics"].items()
                                if isinstance(v, (int, float, str))}})
viz.table(pd.DataFrame(kv).set_index("tc").T.fillna(""))
'''),
 md("## Step 5 — Overall\n"
    "`reports/PoC_Report.html` (scripts/report.py) carries the same "
    "scorecard for the same run ID."),
 py('''
overall = results.overall(RID)
viz.banner(overall, "all objectives",
           f"run {RID} — see reports/PoC_Report.html")
'''),
    ])


# ---------------------------------------------------------------- 99_reset
def nb_99():
    return notebook([
 md("# 99 — Reset (presenter only)\n"
    "Removes what **this PoC** created (demo objects, PoC tables and their "
    "data buckets), restores the throttler backup, re-seeds, and starts a "
    "new run ID. Re-runnable."),
 md("## Step 1 — Dry run: what would be deleted\n"
    "Nothing is changed in this step. Review the list — buckets ending in "
    "`--table-s3` that do not belong to a PoC table are listed as "
    "*left alone* and are never deleted."),
 py('''
import pandas as pd
from poc import results, viz
from poc.reset import full_reset

def show(r):
    viz.table(pd.DataFrame([r["objects_removed"]], index=["demo-* objects"]))
    viz.table(pd.DataFrame({"tables": r["tables_removed"] or ["(none)"]}))
    viz.table(pd.DataFrame(
        [{"data bucket": b, "state": s} for b, s in r["data_buckets_deleted"].items()]
        + [{"data bucket": b, "state": "left alone (not a PoC table)"}
           for b in r["unattributed_left_alone"]]
        or [{"data bucket": "(none)", "state": ""}]))
    for note in r["notes"]:
        print("note:", note)

print("previous run id:", results.run_id())
plan = full_reset(dry_run=True)
show(plan)
print("throttler:", plan["restore_throttler"], "| table bucket:", plan["table_bucket_deleted"])
'''),
 md("## Step 2 — Full reset\n"
    "Restores `reports/throttle_backup.json`, deletes `demo-*` objects in "
    "both buckets, drops the PoC tables/namespaces, their own data buckets "
    "and the PoC table bucket, then re-seeds bucket + namespace and starts "
    "a new run ID."),
 py('''
done = full_reset()
show(done)
problems = [f"{b}: {s}" for b, s in done["data_buckets_deleted"].items() if s.startswith("FAILED")]
problems += [t for t in done["tables_removed"] if "FAILED" in t]
pending = [b for b, s in done["data_buckets_deleted"].items() if s.startswith("pending")]
RID = results.new_run()
print("throttler restored:", done["restore_throttler"])
print("seed:", done["seed"], "| new run id:", RID)
viz.banner(not problems, "reset",
           f"new run {RID}" + (f"; {len(pending)} data bucket(s) still being emptied "
                               f"by ObjectScale — retried on the next reset" if pending else "")
           + (f"; problems: {problems}" if problems else ""))
'''),
 notes("ObjectScale empties a dropped table's data bucket by itself and "
       "refuses changes meanwhile (EmptyBucketInProgress) — such buckets "
       "are reported as pending and remembered for the next reset."),
    ])


NOTEBOOKS = {
    "00_smoke.ipynb": nb_00,
    "01_spark_on_objectscale.ipynb": nb_01,
    "02_s3_throttler.ipynb": nb_02,
    "03_s3_tables_iceberg.ipynb": nb_03,
    "04_summary.ipynb": nb_04,
    "99_reset.ipynb": nb_99,
}


def generate(out_dir=None):
    out_dir = pathlib.Path(out_dir or ROOT / "notebooks")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, build in NOTEBOOKS.items():
        nbformat.write(build(), out_dir / name)
        print("wrote", out_dir / name)
    return out_dir


if __name__ == "__main__":
    generate(sys.argv[1] if len(sys.argv) > 1 else None)
    print("done")
