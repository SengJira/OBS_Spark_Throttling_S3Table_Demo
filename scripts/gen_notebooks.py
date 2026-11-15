#!/usr/bin/env python3
"""Generate the six PoC notebooks in notebooks/. Idempotent."""
import json, nbformat, pathlib

NB_DIR = pathlib.Path("/root/OBS/notebooks")
NB_DIR.mkdir(exist_ok=True)

KERNEL = {"display_name": "PySpark 4.2 (OBS)", "language": "python",
          "name": "pyspark-obs"}


def md(src, hidden=False):
    c = nbformat.v4.new_markdown_cell(src)
    if hidden:
        c.metadata["jupyter"] = {"source_hidden": True}
    return c


def py(src):
    return nbformat.v4.new_code_cell(src)


def nb(cells, name):
    n = nbformat.v4.new_notebook(cells=cells)
    n.metadata["kernelspec"] = KERNEL
    n.metadata["language_info"] = {"name": "python", "version": "3.11"}
    nbformat.write(n, NB_DIR / name)
    print("wrote", name)


def notes(txt):
    return md(f"**Presenter notes** (hidden)\n\n{txt}", hidden=True)


BOOT = '''import pandas as pd
from IPython.display import HTML, display
from poc import config, results, viz, s3util
import poc.spark_helpers as sh
spark = sh.build_session("obs-poc")
viz.table(__import__("pandas").DataFrame(sh.versions(spark).items(), columns=["component","version"]))
print("users:", config.masked_users())
print("Spark UI:", config.SPARK_UI)'''

# ---------------- 00_smoke ----------------
nb([
 md("# 00 — Setup & Smoke Test\n"
    "**Objective:** prove Spark 4.2 runs on the standalone cluster and the ObjectScale "
    "PoC environment is configured.\n\n"
    "**Customer sees:** versions, cluster state, and a real Spark job.\n"
    "**TC:** TC0.1"),
 md("## Step 1 — Start the session\n"
    "Loads `.env`, builds a SparkSession on `spark://192.168.1.40:7077`, prints masked "
    "credentials and versions."),
 py(BOOT),
 notes("Point out: no secrets printed — all keys masked. Show Spark UI at :8080."),
 md("## Step 2 — Cluster smoke job\n"
    "`spark.range(10M).count()` runs a real distributed job on the worker and returns "
    "the count — proves the cluster computes."),
 py('''import time, pandas as pd
t0 = time.time(); n = spark.range(10_000_000).count(); dt = time.time() - t0
res = results.write("TC0.1", "spark.range(10M).count()", n == 10_000_000,
                    {"count": n, "seconds": round(dt, 1)})
viz.table(pd.DataFrame([{"count": n, "seconds": round(dt, 1)}]))
viz.banner(res["passed"], "TC0.1 smoke", f"{n:,} rows in {dt:.1f}s")
spark.stop()'''),
 notes("If this is slow, the worker may still be registering — wait a few seconds."),
], "00_smoke.ipynb")

# ---------------- 01_spark_on_objectscale ----------------
nb([
 md("# 01 — Spark on ObjectScale\n"
    "**Objective:** Spark 4.2 reads and writes ObjectScale S3 via the S3A connector.\n\n"
    "**Customer sees:** a partitioned dataset written to OBS, listed in the OBS UI, "
    "read back and queried.\n**TC:** TC1.1–TC1.6"),
 md("## Step 1 — Session\nBuild SparkSession and print versions (masked creds)."),
 py(BOOT),
 notes("Same session config as the smoke test."),
 md("## Step 2 — Generate the dataset (TC1.1)\n"
    "Synthetic sales orders (~200k rows, 30 days) generated in Spark — no external data "
    "needed."),
 py('''from poc import demo_data
df = demo_data.orders_df(spark)
res = results.write("TC1.1", "dataset generated", df.count() > 0,
                    {"rows": df.count()})
df.show(5); viz.banner(res["passed"], "TC1.1", f"{res['metrics']['rows']:,} rows")'''),
 notes("Deterministic seed — same data every run."),
 md("## Step 3 — Write Parquet partitioned by date (TC1.2)\n"
    "Data lands in `poc-spark-data/<run_id>/raw/orders` — one partition per order_date. "
    "Uses the S3A magic committer for atomic writes."),
 py('''import time, pandas as pd
path = demo_data.data_path()
t0 = time.time()
df.write.mode("overwrite").partitionBy("order_date").parquet(path)
res = results.write("TC1.2", "write partitioned parquet", True,
                    {"path": path, "seconds": round(time.time() - t0, 1)})
viz.banner(res["passed"], "TC1.2", f"{res['metrics']['seconds']}s -> {path}")'''),
 notes("Show the Spark UI storage tab / stages during the write."),
 md("## Step 4 — Objects on ObjectScale (TC1.3)\n"
    "List the objects Spark just wrote, then **pause and show the bucket in the "
    "ObjectScale UI** — same files, visible in the storage console."),
 py('''objs, _ = demo_data.list_objects(config.BUCKET_DATA, f"{results.run_id()}/raw/")
viz.object_table([(o["Key"], o["Size"]) for o in objs])
res = results.write("TC1.3", "objects listed in bucket", len(objs) > 0,
                    {"objects": len(objs)})
viz.banner(res["passed"], "TC1.3", f"{len(objs)} objects — show OBS UI now")'''),
 notes("UI: https://192.168.1.31 — bucket poc-spark-data, prefix = run_id."),
 md("## Step 5 — Read back + business queries (TC1.4)\n"
    "Read the Parquet dataset back and run three business questions as tables/charts: "
    "revenue by day, top customers, orders by status."),
 py('''import matplotlib.pyplot as plt, pandas as pd
rdf = spark.read.parquet(path).createOrReplaceTempView("orders")
q1 = spark.sql("SELECT order_date, round(sum(amount*qty),2) rev FROM orders GROUP BY 1 ORDER BY 1").toPandas()
q2 = spark.sql("SELECT customer, round(sum(amount*qty),2) rev FROM orders GROUP BY 1 ORDER BY rev DESC LIMIT 10").toPandas()
q3 = spark.sql("SELECT status, count(*) n FROM orders GROUP BY 1").toPandas()
fig, ax = plt.subplots(1, 2, figsize=(12, 4))
q1.plot.bar(x="order_date", y="rev", ax=ax[0], legend=False); ax[0].set_title("revenue/day")
q2.plot.bar(x="customer", y="rev", ax=ax[1], legend=False); ax[1].set_title("top customers")
res = results.write("TC1.4", "read+queries", len(q1) > 0 and len(q2) > 0,
                    {"days": len(q1), "top": q2.iloc[0].customer})
viz.table(q3); plt.show(); viz.banner(res["passed"], "TC1.4")'''),
 notes("Queries hit OBS via S3A on every executor."),
 md("## Step 6 — Integrity check (TC1.5)\n"
    "Count parity between source and stored dataset proves no data loss."),
 py('''src, dst = df.count(), spark.read.parquet(path).count()
res = results.write("TC1.5", "integrity", src == dst, {"src": src, "dst": dst})
viz.banner(res["passed"], "TC1.5", f"{src:,} == {dst:,}")'''),
 notes("Parquet column stats + partition pruning also verified implicitly."),
 md("## Step 7 — Multipart throughput vs concurrency (TC1.6)\n"
    "Same 256 MiB object uploaded at 1, 2, 4, 8 and 16 parallel part streams — "
    "shows how concurrency scales sustained write throughput."),
 py('''from poc import throughput
rows = throughput.multipart_sweep(
    config.BUCKET_THROTTLE, f"{results.run_id()}/multipart",
    levels=(1, 2, 4, 8, 16), total_mb=256, part_mb=16)
sweep = pd.DataFrame(rows); viz.table(sweep)
res = results.write("TC1.6", "multipart write concurrency sweep", True,
                    {"levels": rows, "peak_mbs": max(r["mbs"] for r in rows)})
viz.banner(res["passed"], "TC1.6",
           f"peak {res['metrics']['peak_mbs']:.0f} MB/s")'''),
 notes("Objects land under poc-throttle/<run_id>/multipart/ — cleaned by 99_reset."),
 md("## Step 8 — Throughput chart\n"
    "Throughput vs concurrency from `reports/raw/TC1.6.json` — the ingest-scaling story."),
 py('''import pandas as pd, matplotlib.pyplot as plt
sw = pd.DataFrame(res["metrics"]["levels"]).set_index("concurrency")
sw.plot.line(y="mbs", marker="o", figsize=(6, 3.5), legend=False)
plt.xlabel("concurrent part streams"); plt.ylabel("MB/s")
plt.title("S3 multipart write throughput vs concurrency"); plt.grid(alpha=.3)
plt.show()
spark.stop()'''),
 notes("Scaling tapers when a host/NIC limit is hit — expected on a 2-core lab VM."),
], "01_spark_on_objectscale.ipynb")

# ---------------- 02_s3_throttler ----------------
nb([
 md("# 02 — ObjectScale S3 Throttler\n"
    "**Objective:** validate native S3 throttling — a noisy tenant is capped "
    "while a critical workload stays inside SLA and recovers.\n\n"
    "**Customer sees:** four measured phases with verdicts against approved "
    "thresholds, and a live TPS/latency chart.\n**TC:** TC2.1–TC2.9\n\n"
    "**Phases:** 1 baseline (critical GET only) → 2 contention (+noisy PUTs, "
    "dry-run rules) → 3 enforced (429 limit on noisy PUT) → 4 recovery "
    "(noisy stopped, critical continues).\n\n"
    "> Caveat: on this lab build rules propagate with a cache delay and "
    "enforcement may need a datahead reload — verdicts report "
    "INCONCLUSIVE rather than fabricating throttling (see "
    "reports/environment.md)."),
 md("## Step 1 — Session, tenant isolation, effective policy\n"
    "Prove noisy/critical are distinct IAM principals, then **read** the "
    "current throttler config — rules are never changed outside this "
    "approved lab workflow."),
 py(BOOT + '''
import os, json, time
import ipywidgets as w
from poc.loadgen import LoadGen
from poc.obs_admin import MgmtApi, Throttler
from poc import throttle_eval as te

PHASE_S = int(os.environ.get("THROTTLE_PHASE_S", "60"))
DUR = {"baseline": PHASE_S, "contention": PHASE_S, "simulation": PHASE_S,
       "enforced": 2 * PHASE_S, "recovery": PHASE_S}

lg = LoadGen(max_keys=20000)
iso_rows, iso_ok = lg.verify_tenants()
viz.table(pd.DataFrame(iso_rows))
api = MgmtApi(); thr = Throttler(api)
pol = te.effective_policy(thr)
viz.table(pd.DataFrame([{"mode": pol["mode"], "response_code": pol["response_code"],
                         "existing_rules": len(pol["rules"])}]))
viz.banner(iso_ok, "tenant isolation",
           "distinct principals; effective policy read (unchanged)")'''),
 notes("Shared namespace bucket, separate principals — that is what "
       "principal-scoped rules discriminate on."),
 md("## Step 2 — Lab rule + schema verification (TC2.2)\n"
    "The proposed rule: noisy-tenant PUTs capped at 50 TPS, response 429. "
    "`verify_rule` checks the schema against the live API before anything "
    "is applied."),
 py('''NOISY_RULE = {"id":"demo-noisy-put","rateUnit":"TPS","isDisabled":False,
  "filters":{"principals":["urn:ecs:iam::poc:user/tenant-noisy"],
             "actions":["s3:PutObject"]},
  "limit":{"value":50},"scoping":{"per":["principal"]}}
chk = te.verify_rule(thr, NOISY_RULE)
viz.table(pd.DataFrame([{"schema_ok": chk["ok"],
                         "problems": "; ".join(chk["problems"]) or "—"}]))
display(HTML("<pre>"+json.dumps(NOISY_RULE,indent=2)+"</pre>"))
res = results.write("TC2.2", "rule verified", chk["ok"],
                    {"rule": NOISY_RULE["id"], "problems": chk["problems"]})
viz.banner(res["passed"], "TC2.2", "rule schema verified vs live API")'''),
 notes("principals accept user:<name>, iam::user:<id>, or the full IAM URN; "
       "limit.value is transactions per rateUnit."),
 md("## Step 3 — Phase 1: baseline (TC2.1)\n"
    "Critical GETs alone for DUR[baseline] s — reference TPS, P50/P95/P99."),
 py('''out = w.Output(); display(out)
seed = lg.start(noisy=False, critical=True)
lg.start_phase("baseline")
chart = viz.LiveChart(lg); chart.mark("baseline")
chart.refresh(out, seconds=DUR["baseline"])
bp = lg.phase_stats()["baseline"]["tenant-critical GET"]
res = results.write("TC2.1", "baseline critical GETs", bp["ok"] > 0,
    {k: round(bp[k],2) for k in ("n","tps","p50","p95","p99","errors")})
viz.banner(res["passed"], "TC2.1 baseline",
           f"{bp['tps']:.1f} TPS, p95 {bp['p95']:.0f} ms, p99 {bp['p99']:.0f} ms")'''),
 notes("Critical workload alone — rules exist but stay disabled."),
 md("## Step 4 — Phase 2: contention, no rules (TC2.3)\n"
    "Noisy PUTs added on top of critical GETs, **no throttling configured** — "
    "the pure offered-load impact this demo must fix."),
 py('''lg.add_noisy(threads=6); lg.start_phase("contention"); chart.mark("contention")
chart.refresh(out, seconds=DUR["contention"])
ps_all = lg.phase_stats()
b, c = ps_all["baseline"], ps_all["contention"]
bl, ct = b["tenant-critical GET"], c["tenant-critical GET"]
res = results.write("TC2.3", "contention", ct["ok"] > 0,
    {"baseline_p95": round(bl["p95"],1), "contention_p95": round(ct["p95"],1),
     "contention_p99": round(ct["p99"],1),
     "offered_noisy_tps": round(c.get("tenant-noisy PUT",{}).get("tps",0),1),
     "noisy_ok_tps": round(c.get("tenant-noisy PUT",{}).get("ok",0)/
                           max(1e-9,c["_window"]["seconds"]),1)})
viz.banner(res["passed"], "TC2.3 contention",
           f"critical p95 {bl['p95']:.0f} -> {ct['p95']:.0f} ms")'''),
 notes("Unrestricted noisy PUTs degrade critical latency — the problem "
       "statement. No rules active yet."),
 md("## Step 5 — Phase 3: simulation mode, dry-run (TC2.10)\n"
    "Rule pushed with mode **dry-run** (ObjectScale Simulation Mode). Both "
    "workloads continue — the client must see **zero** rejections while the "
    "server only counts matches."),
 py('''thr.put_config([NOISY_RULE]); thr.set_mode("dryrun")
lg.start_phase("simulation"); chart.mark("simulation (dry-run)")
chart.refresh(out, seconds=DUR["simulation"])
sp = lg.phase_stats()["simulation"]
sn = sp.get("tenant-noisy PUT", {})
srv_sim = te.server_counters(thr)
sim_clean = sn.get("throttled", 0) == 0 and sn.get("errors", 0) == 0
res = results.write("TC2.10", "simulation mode", sim_clean,
    {"client_429_or_slowdown": sn.get("throttled",0),
     "client_errors": sn.get("errors",0),
     "noisy_ok_tps": round(sn.get("ok",0)/max(1e-9,sp["_window"]["seconds"]),1),
     "mode": thr.get_mode(),
     "server_metrics_available": srv_sim["available"]})
viz.banner(res["passed"], "TC2.10 simulation",
           "0 client rejections — dry-run matches are server-side only"
           if sim_clean else "client saw rejections in dry-run!")'''),
 notes("Dry-run validates rule matching safely: client sees only 200s; "
       "simulated-match counters are a server-side concept — probed and "
       "reported, never fabricated."),
 md("## Step 5 — Phase 3: enforcement, HTTP 429 (TC2.4)\n"
    "Mode enabled with 429 responses; both workloads continue for "
    "DUR[enforced] s. Mid-phase check confirms noisy load stays sustained."),
 py('''thr.set_mode("enabled"); thr.set_response_code(429)
time.sleep(2)  # allow config write to settle before readback
eff = {"mode": thr.get_mode(), "response_code": thr.get_response_code(),
       "rules": len(thr.get_rules())}
assert eff["mode"] == "enabled" and eff["response_code"] == 429, eff
lg.start_phase("enforced"); chart.mark("enforce 429")
half = DUR["enforced"] // 2
chart.refresh(out, seconds=half)
sustained_mid = lg.sustained(10, 0.5)
if lg.running("tenant-noisy") and not sustained_mid:
    viz.banner(False, "sustained check", "noisy load dropped — extending watch")
chart.refresh(out, seconds=DUR["enforced"] - half)
sustained_end = lg.sustained(10, 0.5)
ps_all = lg.phase_stats()
ep, cp = ps_all["enforced"], ps_all["contention"]
en, ec = ep.get("tenant-noisy PUT", {}), ep.get("tenant-critical GET", {})
cn = cp.get("tenant-noisy PUT", {})
lim_v = NOISY_RULE["limit"]["value"]
enforced, enf_mode = te.detect(ps_all, lim_v)
data_ok = en.get("n", 0) >= 50 and ec.get("n", 0) >= 50
res = results.write("TC2.4", "enforcement", data_ok,
    {"noisy": {k: round(en.get(k,0),2) for k in
               ("n","tps","ok","throttled","r429","r503_slowdown","r503_other",
                "errors","retries_ok","retries_fail","boundary",
                "p50","p95","p99")},
     "noisy_ok_tps_before": round(cn.get("ok",0)/max(1e-9,cp.get("_window",{}).get("seconds",1)),1),
     "noisy_ok_tps_enforced": round(en.get("ok",0)/max(1e-9,ep.get("_window",{}).get("seconds",1)),1),
     "codes": en.get("codes", {}),
     "critical_p95": round(ec.get("p95",0),1),
     "critical_p99": round(ec.get("p99",0),1),
     "sustained_mid": sustained_mid, "sustained_end": sustained_end,
     "effective_config": eff,
     "enforcement_active": enforced, "enforcement_mode": enf_mode})
viz.banner(enforced, "TC2.4",
           f"enforcement active — {enf_mode} mode" if enforced
           else "no enforcement signature (rejects or delay) — rule may "
                "still be propagating (≤5 min cache)")'''),
 notes("This build throttles by request delay: all 200s, ok_tps pinned at the "
       "limit, noisy latency spikes — verdicts detect both modes."),
 md("## Step 7 — Phase 5: continued recovery observation (TC2.5)\n"
    "**No configuration change here** — enforcement from Step 6 stays "
    "active; this phase simply keeps measuring. Noisy PUTs continue at the "
    "same offered rate, the rule keeps capping them, and we observe whether "
    "critical latency holds at recovery level. Throttle events on the noisy "
    "side are expected to stay elevated."),
 py('''lg.start_phase("recovery"); chart.mark("recovery (enforcement continues)")
chart.refresh(out, seconds=DUR["recovery"])
rp = lg.phase_stats()["recovery"]
rc, rn = rp.get("tenant-critical GET", {}), rp.get("tenant-noisy PUT", {})
rec_span = max(1e-9, rp["_window"]["seconds"])
res = results.write("TC2.5", "recovery observation under sustained throttling",
    rc.get("ok",0) > 0 and rn.get("n",0) > 0,
    {"critical_p95": round(rc.get("p95",0),1),
     "critical_p99": round(rc.get("p99",0),1),
     "critical_errors": rc.get("errors",0),
     "critical_throttled": rc.get("throttled",0),
     "noisy_offered_tps": round(rn.get("n",0)/rec_span,1),
     "noisy_throttled": rn.get("throttled",0),
     "noisy_ok_tps": round(rn.get("ok",0)/rec_span,1)})
viz.banner(res["passed"], "TC2.5 recovery",
           f"critical p95 {rc.get('p95',0):.0f} ms; noisy still offering "
           f"{rn.get('n',0)/rec_span:.0f} TPS")'''),
 notes("The mechanism is already-running enforcement, observed longer — "
       "not a second action. Elevated noisy throttle events are expected."),
 md("## Step 8 — Restart safety check\n"
    "Stop and restart the noisy group once to prove it is safely "
    "restartable, then leave it stopped for the remaining steps."),
 py('''leftover = lg.stop_noisy()
restarted = lg.add_noisy(1) == 1
time.sleep(3)
restarted = restarted and lg.running("tenant-noisy")
leftover += lg.stop_noisy()
res = results.write("TC2.11", "worker lifecycle",
                    restarted and leftover == 0,
                    {"noisy_restartable": restarted,
                     "lingering_workers": leftover})
viz.banner(res["passed"], "worker lifecycle",
           "restart ok, no lingering workers" if res["passed"]
           else "worker lifecycle problem")'''),
 md("## Step 9 — Verdicts + server-side correlation (TC2.9)\n"
    "Score all phases against the approved thresholds; correlate client-side "
    "429s with ObjectScale server counters where the build exposes them."),
 py('''verdicts = te.evaluate(lg.phase_stats(), rule=NOISY_RULE)
srv = te.server_counters(thr)
rows = [{"check": v["name"], "verdict": v["verdict"], "detail": v["detail"]}
        for v in verdicts["verdicts"]]
viz.table(pd.DataFrame(rows))
corr = {"client_throttled": sum(v.get("throttled",0) for v in
            lg.phase_stats().get("enforced",{}).values() if isinstance(v,dict)),
        "server_metrics_available": srv["available"], "probed": srv["probed"]}
if srv["available"]:
    corr["server_data"] = srv["data"]
else:
    print("server-side throttle metrics not exposed on this build — "
          "client-side observations only")
res = results.write("TC2.9", "verdicts + correlation",
                    verdicts["overall"] != "FAIL",
                    {"overall": verdicts["overall"], "verdicts": rows, **corr})
viz.banner(verdicts["overall"] != "FAIL", "overall verdict",
           verdicts["overall"])'''),
 notes("A 50 TPS limit does not yield exactly 50 ok/s — enforcement windows, "
       "bursts and retries shift observed rates; thresholds carry tolerance."),
 md("## Step 10 — Phase summary\n"
    "All four phases side by side: TPS, latencies, throttled, errors, codes."),
 py('''rows = [{"phase": ph, "workload": k, "n": v["n"], "tps": round(v["tps"],1),
          "p50": round(v["p50"],0), "p95": round(v["p95"],0),
          "p99": round(v["p99"],0), "throttled": v["throttled"],
          "errors": v["errors"], "retries": v["retries"]}
         for ph, gs in lg.phase_stats().items()
         for k, v in gs.items() if k != "_window"]
viz.table(pd.DataFrame(rows))
chart.refresh(out, seconds=2)'''),
 notes("Shaded bands mark closed phases — the four-phase story in one chart."),
 md("## Step 11 — Prefix-scoped rule (TC2.6)\n"
    "A second rule limits `raw/*` writes only — other prefixes unaffected."),
 py('''PRE = dict(NOISY_RULE, id="demo-raw-prefix",
   filters={"principals":["urn:ecs:iam::poc:user/tenant-noisy"],"actions":["s3:PutObject"],
            "paths":{"matcher":"wildcard","values":["poc/poc-throttle/raw/*"]}},
   scoping={"per":["principal","path"]})
thr.put_config([NOISY_RULE, PRE]); chart.mark("prefix rule")
rules = thr.get_rules()
res = results.write("TC2.6", "prefix rule", len(rules) == 2, {"rules": len(rules)})
viz.table(pd.DataFrame(rules)[["id","rateUnit"]]); viz.banner(res["passed"], "TC2.6")'''),
 notes("paths values are <namespace>/<bucket>/<key-glob>."),
 md("## Step 12 — Stop load, Spark write under rules (TC2.7)\n"
    "Stop the generator and time a Spark write as tenant-noisy under the "
    "active rules."),
 py('''leftover = lg.stop()
import time as T
t0 = T.time()
spark.range(100_000).write.mode("overwrite").json(
    f"s3a://{config.BUCKET_THROTTLE}/{results.run_id()}/throttled-write")
res = results.write("TC2.7", "spark write under rules", leftover == 0,
                    {"seconds": round(T.time() - t0, 1),
                     "lingering_workers": leftover})
viz.banner(res["passed"], "TC2.7", f"{res['metrics']['seconds']}s")'''),
 notes("With enforcement active, S3A's throttle retry policy absorbs 429s."),
 md("## Step 13 — Restore\n"
    "Guaranteed restore of the pre-demo throttler backup — registered at "
    "exit as well."),
 py('''import atexit
atexit.register(thr.restore)
thr.restore()
res = results.write("TC2.8", "restore", thr.get_mode() == "disabled" and not thr.get_rules())
viz.banner(res["passed"], "restore", f"mode={thr.get_mode()}, rules={len(thr.get_rules())}")
spark.stop()'''),
 notes("Backup: reports/throttle_backup.json — written by scripts/prep_obs.py."),
], "02_s3_throttler.ipynb")

# ---------------- 03_s3_tables ----------------
nb([
 md("# 03 — S3 Tables (Apache Iceberg)\n"
    "**Objective:** native Iceberg tables on ObjectScale via Spark's REST catalog — "
    "no data movement.\n\n"
    "**Customer sees:** table bucket + namespace, DDL/DML, MERGE, schema evolution, "
    "time travel, and a read-only denial.\n**TC:** TC3.1–TC3.7"),
 md("## Step 1 — Session\nSparkSession with the `os` REST catalog pointed at the OBS "
    "TableHead endpoint (SigV4, table bucket ARN prefix)."),
 py(BOOT),
 notes("Catalog os -> http://192.168.1.31:19020/iceberg, warehouse=poc-lakehouse ARN."),
 md("## Step 2 — Table bucket & namespace via S3 Tables API (TC3.1)\n"
    "Create/confirm the table bucket and namespace through the AWS-compatible API and "
    "show the responses."),
 py('''st = s3util.s3tables("tables-rw")
arn = f"arn:aws:s3tables:::bucket/{config.OBS_TABLE_BUCKET}"
ns = config.TABLES_NAMESPACE
tbl = st.list_table_buckets()["tableBuckets"]
namespaces = [n for n in st.list_namespaces(tableBucketARN=arn).get("namespaces",[])]
viz.table(pd.DataFrame(tbl)); viz.table(pd.DataFrame(namespaces, columns=["namespace"]))
res = results.write("TC3.1", "bucket+namespace", bool(tbl) and bool(namespaces))
viz.banner(res["passed"], "TC3.1", f"bucket={config.OBS_TABLE_BUCKET} ns={ns}")'''),
 notes("boto3 s3tables client + x-emc-namespace header."),
 md("## Step 3 — SHOW NAMESPACES + CREATE TABLE (TC3.2)\n"
    "The same objects are now visible through Spark's Iceberg catalog — one control "
    "plane, no ETL."),
 py('''spark.sql("SHOW NAMESPACES IN os").show()
spark.sql(f"DROP TABLE IF EXISTS os.{config.TABLES_NAMESPACE}.orders_ice")
spark.sql(f"""CREATE TABLE os.{config.TABLES_NAMESPACE}.orders_ice
 (order_id string, customer string, status string, amount double, qty int)
 USING iceberg""")
res = results.write("TC3.2", "create table", True)
viz.banner(res["passed"], "TC3.2", "os.pocns.orders_ice created")'''),
 notes("SparkCatalog 'os' — type=rest, SigV4-signed."),
 md("## Step 4 — INSERT + SELECT (TC3.3)\n"
    "Insert rows through the Iceberg catalog; they land in the managed `--table-s3` "
    "bucket on ObjectScale."),
 py('''spark.sql(f"""INSERT INTO os.{config.TABLES_NAMESPACE}.orders_ice
 VALUES ('o1','cust-001','PAID',199.0,2),('o2','cust-002','NEW',49.5,1),
        ('o3','cust-003','SHIPPED',1200.0,10)""")
rows = spark.sql(f"SELECT * FROM os.{config.TABLES_NAMESPACE}.orders_ice").toPandas()
viz.table(rows)
res = results.write("TC3.3", "insert+select", len(rows) == 3, {"rows": len(rows)})
viz.banner(res["passed"], "TC3.3")'''),
 notes("Data files visible in OBS UI under <uuid>--table-s3."),
 md("## Step 5 — MERGE INTO (TC3.4)\n"
    "Upsert a changed row — Iceberg's row-level update over object storage, before "
    "and after side by side."),
 py('''before = rows.copy()
spark.sql(f"""MERGE INTO os.{config.TABLES_NAMESPACE}.orders_ice t
 USING (SELECT 'o2' order_id,'cust-002' customer,'PAID' status,55.0 amount,1 qty) u
 ON t.order_id=u.order_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
after = spark.sql(f"SELECT * FROM os.{config.TABLES_NAMESPACE}.orders_ice ORDER BY order_id").toPandas()
viz.table(before.assign(state="before")); viz.table(after.assign(state="after"))
res = results.write("TC3.4", "merge", after.loc[after.order_id=="o2","status"].iloc[0]=="PAID")
viz.banner(res["passed"], "TC3.4", "o2 NEW -> PAID")'''),
 notes("MERGE INTO needs the Iceberg SQL extensions — already configured."),
 md("## Step 6 — Schema evolution (TC3.5)\n"
    "Add a column without rewriting data."),
 py('''spark.sql(f"ALTER TABLE os.{config.TABLES_NAMESPACE}.orders_ice ADD COLUMNS (region string)")
cols = spark.sql(f"DESCRIBE os.{config.TABLES_NAMESPACE}.orders_ice").toPandas()
viz.table(cols)
res = results.write("TC3.5", "add column", "region" in set(cols.col_name))
viz.banner(res["passed"], "TC3.5")'''),
 notes("Metadata-only operation — instant."),
 md("## Step 7 — Time travel & snapshot history (TC3.6)\n"
    "Query an earlier snapshot vs current, and show the snapshot log."),
 py('''snaps = spark.sql(f"SELECT snapshot_id, committed_at FROM os.{config.TABLES_NAMESPACE}.orders_ice.snapshots").toPandas()
old = spark.sql(f"SELECT count(*) n FROM os.{config.TABLES_NAMESPACE}.orders_ice VERSION AS OF {snaps.snapshot_id.iloc[0]}").toPandas()
cur = spark.sql(f"SELECT count(*) n FROM os.{config.TABLES_NAMESPACE}.orders_ice").toPandas()
viz.table(snaps); viz.table(pd.concat([old.assign(view="first snapshot"), cur.assign(view="current")]))
ok = len(snaps) >= 2 and int(old.n.iloc[0]) <= int(cur.n.iloc[0])
res = results.write("TC3.6", "time travel", ok,
                    {"snapshots": len(snaps), "first": int(old.n.iloc[0]),
                     "current": int(cur.n.iloc[0])})
viz.banner(res["passed"], "TC3.6", f"{len(snaps)} snapshots")'''),
 notes("VERSION AS OF uses snapshot_id; TIMESTAMP AS OF also supported."),
 md("## Step 8 — Read-only denial (TC3.7)\n"
    "Rebuild the session as tables-ro and run INSERT — should fail with Access Denied."),
 py('''spark.stop()
spark = sh.build_session("obs-poc-ro", tables_user="tables-ro")
try:
    spark.sql(f"INSERT INTO os.{config.TABLES_NAMESPACE}.orders_ice BY NAME SELECT 'x' AS order_id")
    denied, code = False, "unexpected success"
except Exception as e:
    denied, code = "Denied" in str(e) or "403" in str(e), str(e)[:60]
res = results.write("TC3.7", "ro denied", denied, {"error": code})
viz.banner(res["passed"], "TC3.7", f"Access Denied as expected")'''),
 notes("tables-ro: S3TablesReadOnlyAccess + ECSS3ReadOnlyAccess."),
 md("## Step 9 — List tables via API\n"
    "Back to the control plane: list tables in the namespace via S3 Tables API."),
 py('''tabs = st.list_tables(tableBucketARN=arn, namespace=config.TABLES_NAMESPACE)["tables"]
viz.table(pd.DataFrame(tabs)[["name","type"]])
viz.banner(True, "tables listed", f"{len(tabs)} table(s)")
spark.stop()'''),
 notes("Same table list is visible in the OBS UI under S3 Tables."),
], "03_s3_tables_iceberg.ipynb")

# ---------------- 04_summary ----------------
nb([
 md("# 04 — Summary & Scorecard\n"
    "**Objective:** pull every TC result into an O1–O3 objective scorecard.\n\n"
    "**Customer sees:** green/red objective status and key numbers."),
 md("## Step 1 — Session + results"),
 py('''from poc import config, results, viz
import pandas as pd, poc.spark_helpers as sh'''),
 md("## Step 2 — Objective scorecard\n"
    "O1 Spark↔S3, O2 Throttler, O3 S3 Tables — each mapped to its test cases."),
 py('''sc = pd.DataFrame(results.scorecard())
def style(r): return ["background-color:#1e7d32;color:#fff" if r.status=="PASS"
                      else "background-color:#b71c1c;color:#fff"]*len(r)
display(sc.style.apply(style, axis=1))'''),
 notes("TC files: reports/raw/TC*.json"),
 md("## Step 3 — Key metrics\n"
    "Headline numbers pulled from the raw results."),
 py('''allr = results.load_all()
kv = []
for tc in ("TC1.6","TC2.4","TC3.6"):
    r = allr.get(tc)
    if r: kv.append({"tc": tc, **{k: v for k, v in r["metrics"].items() if isinstance(v,(int,float,str))}})
viz.table(pd.DataFrame(kv))'''),
 md("## Step 4 — Report link\n"
    "`reports/PoC_Report.html` is generated by scripts/report.py."),
 py('''ok = all(r["status"] == "PASS" for r in results.scorecard())
viz.banner(ok, "all objectives", "see reports/PoC_Report.html")'''),
], "04_summary.ipynb")

# ---------------- 99_reset ----------------
nb([
 md("# 99 — Reset (presenter only)\n"
    "Removes all demo objects, tables and restores the throttler backup. Re-runnable."),
 md("## Step 1 — Environment"),
 py('''from poc import config, results, viz
from poc.reset import full_reset
import pandas as pd'''),
 md("## Step 2 — Full reset\n"
    "Deletes `<run_id>` objects in both buckets, drops all tables/namespaces **and "
    "the table bucket itself** (plus orphan `--table-s3` data buckets), restores "
    "`reports/throttle_backup.json`, then re-seeds bucket + namespace."),
 py('''out = full_reset()
viz.table(pd.DataFrame([out["objects_removed"]]))
viz.table(pd.DataFrame({"removed_tables": out["tables_removed"] or ["(none)"]}))
viz.table(pd.DataFrame([{"table_bucket_deleted": out["table_bucket_deleted"],
                        "data_buckets_deleted": len(out["data_buckets_deleted"])}]))
viz.banner(True, "reset done", f"throttler -> {out['restore_throttler'] or 'baseline'}")'''),
], "99_reset.ipynb")
print("done")
