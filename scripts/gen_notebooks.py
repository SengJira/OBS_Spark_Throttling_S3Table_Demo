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
    "throttler disabled) → simulation (dry-run) → enforcement (HTTP 429) → "
    "recovery → post-baseline (noisy stopped) → prefix rule → Spark write "
    "as the throttled tenant → restore.\n\n"
    "> ObjectScale caches the rule set for up to "
    "`rules_config_cache_expiration_time` = 300 s (read-only). The rules "
    "are staged by `99_reset` minutes before this notebook runs, so the "
    "propagation wait is normally already over — a short countdown appears "
    "only if they are still younger than 300 s. A verdict the evidence "
    "cannot decide is INCONCLUSIVE — never a pass.\n\n"
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
SETTLE_S = int(os.environ.get("THROTTLE_SETTLE_S", "120"))
CRITICAL_RATE = float(config.CRITICAL_RATE or 0)
RULE_PROPAGATION_S = 300          # rules_config_cache_expiration_time (read-only)
DUR = {"baseline": PHASE_S, "contention": PHASE_S, "simulation": PHASE_S,
       "enforced": 2 * PHASE_S, "recovery": PHASE_S, "post-baseline": PHASE_S,
       "burst": max(15, PHASE_S // 3)}
RID = results.run_id()

lg = LoadGen(max_keys=0, noisy_chunk=config.NOISY_OBJECT_KB * 1024,
             noisy_random=bool(config.PAYLOAD_RANDOM),
             critical_objects=config.CRITICAL_OBJECTS,
             critical_object_kb=config.CRITICAL_OBJECT_KB)
remote = None
if config.NOISY_HOST:
    if not config.NOISY_AGENT_TOKEN:
        raise RuntimeError("NOISY_HOST is set but NOISY_AGENT_TOKEN is empty — "
                           "the remote agent requires the shared X-POC-Token header")
    remote = RemoteNoisy(lg, config.NOISY_HOST, token=config.NOISY_AGENT_TOKEN,
                         run_id=RID, max_keys=0,
                         noisy_random=bool(config.PAYLOAD_RANDOM))
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
print("run id:", RID, "| phase seconds:", DUR,
      "| CRITICAL_RATE:", CRITICAL_RATE,
      "| NOISY_THREADS:", config.NOISY_THREADS,
      "| NOISY_OBJECT_KB:", config.NOISY_OBJECT_KB,
      "| PAYLOAD_RANDOM:", config.PAYLOAD_RANDOM,
      "| CRITICAL_OBJECTS:", config.CRITICAL_OBJECTS,
      "x", config.CRITICAL_OBJECT_KB, "KiB")
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
                              "contact_age_s", "last_error", "event_gap")}
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
    if (a.get("remote") or {}).get("event_gap"):
        return ("noisy events dropped before pull: "
                + a["remote"]["event_gap"])
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
 md("## Step 2 — Lab rules: staged by 99_reset, verified here (TC2.2)\n"
    "Two rules for the noisy tenant: all PUTs capped at 50 TPS, and a "
    "tighter 10 TPS cap on one key prefix (`demo-raw-covered/*`). `99_reset` "
    "already stored them with the throttler disabled; this step verifies "
    "them against the live schema and reads their age. If they are missing "
    "they are staged now and the propagation clock restarts."),
 py('''
from poc import demo_rules
NOISY_RULE = demo_rules.noisy_put_rule()
PREFIX_RULE = demo_rules.prefix_put_rule()
LIMIT, PREFIX_LIMIT = (NOISY_RULE["limit"]["value"],
                       PREFIX_RULE["limit"]["value"])
COVERED, CONTROL = "demo-raw-covered", f"{RID}/curated"
STAGED_FILE = config.REPORT_DIR / "rules_staged_at"
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
stored = thr.get_rules()
by_id = {r.get("id"): r for r in stored}
missing = [r["id"] for r in (NOISY_RULE, PREFIX_RULE) if r["id"] not in by_id]
if missing:
    keep = [r for r in stored if not demo_rules.is_demo_rule(r)]
    thr.set_mode("disabled")
    thr.put_config(keep + [NOISY_RULE, PREFIX_RULE])
    staged_at = time.time()
    STAGED_FILE.write_text(str(staged_at))
    print(f"rules were NOT staged by 99_reset (missing {missing}) — staged now; "
          f"the propagation clock restarts")
else:
    try:
        staged_at = float(STAGED_FILE.read_text())
    except Exception:
        staged_at = time.time()
        print("rules_staged_at missing — treating rule age as 0 (conservative)")
PUSHED_AT = staged_at
RULE_AGE_S = round(time.time() - PUSHED_AT, 1)
print(f"rules stored {RULE_AGE_S:.0f}s ago (mode {thr.get_mode()}); "
      f"enforcement waits until they are {RULE_PROPAGATION_S}s old")
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
# lg.start() seeds the critical objects once (head-then-put, skipped when
# they already exist for this run); say what was seeded.
print(f"critical reads: {config.CRITICAL_OBJECTS} x "
      f"{config.CRITICAL_OBJECT_KB} KiB objects, seeded once before baseline")
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
    "Noisy PUTs are added on top of the critical GETs with the throttler "
    "disabled. This measures how much the noisy writer affects the "
    "critical reader, if at all."),
 py('''
lg.start_phase("contention"); chart.mark("contention")
lg.add_noisy(threads=config.NOISY_THREADS)
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
    delta = ct.get("p95", 0) - bl.get("p95", 0)
    effect = ("no measurable impact" if delta < 1
              else f"+{delta:.0f} ms measured impact")
    st3, why3 = "PASS", (f"critical p95 {bl.get('p95', 0):.0f} -> "
                         f"{ct.get('p95', 0):.0f} ms ({delta:+.0f} ms) "
                         f"with {cn.get('tps', 0):.0f} noisy PUT/s — "
                         f"{effect}")
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
 notes("Noisy PUTs are added on top of the critical GETs with the "
       "throttler disabled — this measures how much the noisy writer "
       "affects the critical reader, if at all. If there is no impact, "
       "the story is fairness and quotas: the throttler caps the noisy "
       "tenant before it can become a problem. TC2.3 is a sanity check "
       "(both workloads ran, nothing was throttled) — it does not fail "
       "on 'no impact'. Caveat for this lab: in LOCAL mode both tenants "
       "are driven from one 2-vCPU client host, and a CPU-only "
       "disturbance on that host (no S3 traffic) degrades the critical "
       "GETs just as much — so the size of this effect cannot be "
       "attributed to ObjectScale alone (reports/environment.md, "
       "'Client-side contention'). client_cpu_pct is recorded per phase. "
       "With NOISY_HOST set, the noisy PUTs run on the second host and "
       "this client-side term is removed — compare the same phases "
       "between the two modes."),
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
 md("## Step 6 — Phase 4: enforcement, HTTP 429 (TC2.4)\n"
    "Mode **enabled**, response code 429; the noisy tenant keeps running "
    "as fast as it can. The switch is read back, the notebook waits until "
    "the new mode is visibly in effect (tagged `switching`), then measures "
    "the `enforced` phase with both workloads running."),
 py('''
wait_end = PUSHED_AT + RULE_PROPAGATION_S
if (left := wait_end - time.time()) > 0:
    countdown = w.HTML(); display(countdown)
    while (l := wait_end - time.time()) > 0:
        countdown.value = (f"<b>rule propagation: {l:.0f}s left</b> "
                           f"(rules staged by 99_reset; noisy still running, dry-run)")
        chart.refresh(out, seconds=min(10, l))
    print("propagation wait complete")
RULE_AGE_S = round(time.time() - PUSHED_AT, 1)
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
 md("## Step 7 — Phase 5: sustained enforcement\n"
    "**No configuration change here** — enforcement from Step 6 stays "
    "active and the noisy tenant keeps offering as fast as it can. This "
    "phase shows the cap holding for another minute: it measures critical "
    "latency while noisy is still capped at the limit. The verdict is "
    "written after post-baseline so the drift guard has both "
    "critical-alone readings. (Internal phase id stays `recovery` for "
    "result compatibility.)"),
 py('''
lg.start_phase("recovery"); chart.mark("sustained enforcement")
chart.refresh(out, seconds=DUR["recovery"])
aux = finish_phase("recovery")
print("sustained enforcement measured; verdict written after post-baseline")
'''),
 notes("Nothing changes in this phase — the already-running enforcement "
       "is observed for another minute to show the cap holds. The rule "
       "still admits ~50 noisy PUT/s by design, so the critical workload "
       "shares the system with that admitted load."),
 md("## Step 8 — Post-baseline + recovery verdict (TC2.5)\n"
    "The noisy tenant is **stopped** while enforcement stays on: a second "
    "critical-alone reading that detects environment drift. TC2.5 is "
    "written here so `baseline_stable` and `recovery_improvement` have "
    "every phase they need."),
 py('''
lg.stop_noisy()
lg.start_phase("post-baseline"); chart.mark("post-baseline (noisy stopped)")
chart.refresh(out, seconds=DUR["post-baseline"])
aux = finish_phase("post-baseline")
ps_all = lg.phase_stats()
verdicts = te.evaluate(ps_all, rule=NOISY_RULE)
REC_CHECKS = {"recovery_improvement", "baseline_stable",
              "critical_p95_within_sla", "paced_client_kept_up",
              "noisy_sustained_in_recovery", "critical_not_throttled"}
st5 = te.combine(verdicts["verdicts"], REC_CHECKS)
why5 = "; ".join(f"{v['name']}={v['verdict']}" for v in verdicts["verdicts"]
                 if v["name"] in REC_CHECKS)
contact_fails = {p: contact_reason(p) for p in
                 ("baseline", "contention", "simulation", "enforced",
                  "recovery", "post-baseline") if p in PHASE_AUX}
contact_fails = {p: r for p, r in contact_fails.items() if r}
if contact_fails:
    st5 = "FAIL"
    why5 = (why5 + "; " if why5 else "") + "; ".join(
        f"{p}: {r}" for p, r in contact_fails.items())
rp = ps_all.get("recovery", {})
rc, rn = rp.get("tenant-critical GET", {}), rp.get("tenant-noisy PUT", {})
rec_span = max(1e-9, rp.get("_window", {}).get("seconds", 1))
res = results.write("TC2.5", "sustained enforcement under throttling",
    status=st5, detail=why5,
    metrics={"baseline_p95": round(ps_all["baseline"].get("tenant-critical GET", {}).get("p95", 0), 1),
             "post_baseline_p95": round(ps_all.get("post-baseline", {}).get("tenant-critical GET", {}).get("p95", 0), 1),
             "contention_p95": round(ps_all["contention"].get("tenant-critical GET", {}).get("p95", 0), 1),
             "critical_p95": round(rc.get("p95", 0), 1),
             "critical_p99": round(rc.get("p99", 0), 1),
             "critical_tps": round(rc.get("tps", 0), 1),
             "critical_errors": rc.get("errors", 0),
             "critical_throttled": rc.get("throttled", 0),
             "noisy_offered_tps": round(rn.get("n", 0) / rec_span, 1),
             "noisy_accepted_tps": round(
                 (rn.get("n", 0) - rn.get("throttled", 0)) / rec_span, 1),
             "noisy_ok_tps": round(rn.get("ok", 0) / rec_span, 1),
             "noisy_throttled": rn.get("throttled", 0),
             "client_cpu_pct": {ph: ps_all[ph]["_window"]["client_cpu_pct"]
                                for ph in ("baseline", "contention", "simulation",
                                           "enforced", "recovery", "post-baseline")
                                if ph in ps_all},
             "checks": {v["name"]: v["detail"] for v in verdicts["verdicts"]
                        if v["name"] in REC_CHECKS},
             "phase_aux": {p: aux_metrics(p) for p in PHASE_AUX}})
pp = ps_all.get("post-baseline", {}).get("tenant-critical GET", {})
rc_p95 = rc.get("p95", 0)
v5 = next((v for v in verdicts["verdicts"]
           if v["name"] == "recovery_improvement"), {})
viz.banner(st5, "TC2.5 sustained enforcement",
           f"during sustained enforcement: critical p95 {rc_p95:.0f} ms "
           f"(baseline {res['metrics']['baseline_p95']:.0f}), noisy offered "
           f"{rn.get('n', 0) / rec_span:.0f} req/s, "
           f"{(rn.get('n', 0) - rn.get('throttled', 0)) / rec_span:.0f} "
           f"accepted · post-baseline (noisy stopped): critical p95 "
           f"{pp.get('p95', 0):.0f} ms"
           + (f" · {v5.get('mode')}" if v5.get("mode") else ""))
'''),
 notes("The mechanism is the already-running enforcement, observed longer "
       "— not a second action. recovery_improvement is the scored "
       "isolation verdict (mode 'improvement' when contention was "
       "measured, 'unaffected' when the critical workload never slowed); "
       "recovery_p95_vs_baseline is information only because single-host "
       "CPU noise made it unfair."),
 md("## Step 9 — Prefix-scoped rule (TC2.6)\n"
    "The second rule caps the noisy tenant at 10 TPS under "
    "`demo-raw-covered/` only. The background noisy load is already "
    "stopped (Step 8), so these bursts run alone. Real traffic decides: "
    "one PUT burst to the covered prefix and one to a control prefix "
    "(`<run_id>/curated/`). PASS only if the covered prefix is held at "
    "its limit while the control prefix runs clearly above it."),
 py('''
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
'''),
 notes("Both bursts use the tenant-noisy identity with retries off. The "
       "control prefix is still subject to the tenant-wide rule, so it is "
       "expected near 50 TPS, the covered prefix near 10."),
 md("## Step 10 — Spark write as the throttled tenant (TC2.7)\n"
    "The same Spark job runs while the noisy load keeps the tenant at "
    "its limit. One untimed warm-up write as **poc-admin** absorbs JVM "
    "warm-up, then each identity is timed twice — poc-admin, "
    "tenant-noisy, poc-admin, tenant-noisy — and the median of each is "
    "reported. Job size is configurable: `SPARK_WRITE_ROWS` (default "
    "200,000) and `SPARK_WRITE_PARTITIONS` (default 20); raise them until "
    "the job's PUT rate exceeds the 50 TPS limit."),
 py('''
import statistics
lg.add_noisy(threads=config.NOISY_THREADS)
lg.start_phase("spark-write"); chart.mark("spark writes")

ROWS_W = config.SPARK_WRITE_ROWS
PARTS_W = config.SPARK_WRITE_PARTITIONS

def spark_write(tag):
    target = f"{RID}/spark-write-{tag}"
    t0 = time.time()
    spark.range(ROWS_W).repartition(PARTS_W).write.mode("overwrite").json(
        f"s3a://{config.BUCKET_THROTTLE}/{target}")
    return round(time.time() - t0, 1), target

def rebuild(identity):
    global spark
    spark.stop()
    spark = (sh.build_session("obs-poc") if identity == "admin"
             else sh.build_session("obs-poc-noisy", s3a_user=identity))

_wt, _ = spark_write("warmup-admin")
print(f"warm-up write: {_wt}s (untimed)")

t_start = time.time()
times = {"admin": [], "noisy": []}
noisy_targets, idents, job_error = [], {}, None
for i, ident in enumerate(["admin", "noisy", "admin", "noisy"]):
    rebuild(ident)
    idents[ident] = sh.identities(spark)
    try:
        t, tgt = spark_write(f"{ident}-{i}")
        times[ident].append(t)
        if ident == "noisy":
            noisy_targets.append(tgt)
    except Exception as e:
        job_error = str(e).splitlines()[0][:300]
        break

t_admin = statistics.median(times["admin"]) if times["admin"] else None
t_noisy = statistics.median(times["noisy"]) if times["noisy"] else None
during = lg.snapshot(since=t_start).get("tenant-noisy", {}).get("PUT", {})
n429 = during.get("throttled", 0)
s3a_stats, s3a_throttled = {}, 0
for tgt in noisy_targets:
    st = sh.s3a_committer_stats(config.BUCKET_THROTTLE, tgt)
    s3a_stats[tgt] = st
    s3a_throttled += sum(v for k, v in st.items()
                         if "throttl" in k and isinstance(v, (int, float)))
mode_now = thr.get_mode()
aux = finish_phase("spark-write")
if job_error:
    st7, why7 = "FAIL", f"Spark write as tenant-noisy failed: {job_error}"
elif idents.get("noisy", {}).get("s3a") != "noisy":
    st7, why7 = "FAIL", (f"session S3A identity is "
                         f"{(idents.get('noisy') or {}).get('s3a')}, not tenant-noisy")
elif mode_now != "enabled" or not (n429 or s3a_throttled):
    st7, why7 = "INCONCLUSIVE", (f"job completed in {t_noisy}s, but no throttling was "
                                 f"observed for tenant-noisy while it ran (mode={mode_now})")
elif contact_reason("spark-write"):
    st7, why7 = "FAIL", contact_reason("spark-write")
else:
    st7, why7 = "PASS", (f"median {t_noisy}s vs {t_admin}s as poc-admin "
                         f"({t_noisy / max(t_admin or 0.1, 0.1):.1f}x); "
                         f"{n429} tenant-noisy 429s during the job "
                         f"(load generator + Spark)")
res = results.write("TC2.7", "spark write as throttled tenant", status=st7, detail=why7,
    metrics={"seconds_admin_runs": times["admin"],
             "seconds_noisy_runs": times["noisy"],
             "seconds_admin_median": t_admin, "seconds_noisy_median": t_noisy,
             "slowdown": round(t_noisy / max(t_admin or 0.1, 0.1), 2)
                        if t_noisy and t_admin else None,
             "warmup_seconds": _wt,
             "spark_write_rows": ROWS_W, "spark_write_partitions": PARTS_W,
             "s3a_identity": (idents.get("noisy") or {}).get("s3a"),
             "s3a_key": (idents.get("noisy") or {}).get("s3a_key"),
             "mode": mode_now,
             "tenant_noisy_429_during_job": n429,
             "tenant_noisy_ok_tps_during_job": round(
                 during.get("ok", 0) / max(1e-9, time.time() - t_start), 1),
             "s3a_committer_stats": s3a_stats,
             "s3a_throttled_requests": s3a_throttled,
             **aux_metrics("spark-write")})
viz.table(pd.DataFrame([
    {"identity": "poc-admin (not in the rule)", "runs_s": times["admin"],
     "median_s": t_admin},
    {"identity": "tenant-noisy (rule enforced)", "runs_s": times["noisy"],
     "median_s": t_noisy}]))
viz.banner(st7, "TC2.7 Spark under throttling", why7)
'''),
 notes("The S3A identity is set per session (fs.s3a.access.key), verified "
       "by build_session; each identity is rebuilt and timed twice, the "
       "median reported. S3A retries throttled requests "
       "(fs.s3a.retry.throttle.*), so the job slows down instead of "
       "failing. The 429 count is tenant-noisy 429s during the job from "
       "the load generator plus Spark combined; Spark's own throttling "
       "is only claimed when the S3A committer counters show it "
       "(s3a_throttled_requests)."),
 md("## Step 11 — Stop the load: worker lifecycle (TC2.11)\n"
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
 md("## Step 12 — Verdicts + server-side correlation (TC2.9)\n"
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
                             "clock_offsets": {p: a.get("clock_offset_ms")
                                               for p, a in PHASE_AUX.items()},
                             "contact_failures": contact_fails})
viz.banner(verdicts["overall"], "TC2.9 overall verdict",
           f"enforcement mode: {verdicts['enforcement_mode']}")
'''),
 notes("A 50 TPS limit does not yield exactly 50 ok/s — the thresholds "
       "carry a stated tolerance. They are customer-approved values in "
       "te.DEFAULT_THRESHOLDS, not tuned to the result. recovery_"
       "improvement is the scored isolation verdict; recovery_p95_vs_"
       "baseline is information only."),
 md("## Step 13 — Phase summary\n"
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
 md("## Step 14 — Restore (TC2.8)\n"
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
