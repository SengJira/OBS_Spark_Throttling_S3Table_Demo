#!/usr/bin/env python3
"""Generate reports/PoC_Report.html from reports/raw/TC*.json + environment."""
import json, pathlib, html, time
import sys
sys.path.insert(0, "/root/OBS")
from poc import results, config

rows = results.scorecard()
allr = results.load_all()
tr = "".join(
    f"<tr><td>{o['objective']}</td><td>{html.escape(o['description'])}</td>"
    f"<td class='{ 'pass' if o['status']=='PASS' else 'fail' }'>{o['status']}</td>"
    f"<td>{o['test_cases']} TCs</td></tr>" for o in rows)

detail = "".join(
    f"<tr><td>{tc}</td><td>{html.escape(str(r.get('name','')))}</td>"
    f"<td class='{ 'pass' if r.get('passed') else 'fail' }'>"
    f"{'PASS' if r.get('passed') else 'FAIL'}</td>"
    f"<td><pre>{html.escape(json.dumps(r.get('metrics',{}),indent=1)[:400])}</pre></td></tr>"
    for tc, r in sorted(allr.items()))

thr_html = ""
t29 = allr.get("TC2.9", {}).get("metrics", {})
if t29:
    vrows = "".join(
        f"<tr><td>{html.escape(v['check'])}</td>"
        f"<td class='{'pass' if v['verdict']=='PASS' else 'fail' if v['verdict']=='FAIL' else 'inc'}"
        f">{v['verdict']}</td><td>{html.escape(str(v['detail']))}</td></tr>"
        for v in t29.get("verdicts", []))
    phases = "".join(
        f"<tr><td>{tc}</td><td><pre>{html.escape(json.dumps(allr[tc]['metrics'],indent=1)[:600])}</pre></td></tr>"
        for tc in ("TC2.1","TC2.3","TC2.4","TC2.5") if tc in allr)
    corr = (f"client-side throttled events: {t29.get('client_throttled')} — "
            f"server-side metrics {'available' if t29.get('server_metrics_available') else 'not exposed on this build'}"
            f" (probed: {html.escape(json.dumps(t29.get('probed',{})))})")
    thr_html = f"""<h2>S3 Throttler — performance isolation</h2>
<p><b>Overall verdict: {html.escape(str(t29.get('overall','?')))}</b> · {corr}</p>
<table><tr><th>Check</th><th>Verdict</th><th>Detail</th></tr>{vrows}</table>
<h3>Phase metrics</h3><table><tr><th>TC</th><th>Metrics</th></tr>{phases}</table>"""

env = ""
ep = pathlib.Path("/root/OBS/reports/environment.md")
if ep.exists():
    env = f"<h2>Environment</h2><pre>{html.escape(ep.read_text()[:6000])}</pre>"

doc = f"""<!doctype html><meta charset=utf-8><title>ObjectScale 4.4 + Spark 4.2 PoC</title>
<style>
body{{font-family:sans-serif;margin:2em}}table{{border-collapse:collapse}}
td,th{{border:1px solid #ccc;padding:6px 12px}}.pass{{background:#1e7d32;color:#fff}}
.fail{{background:#b71c1c;color:#fff}}.inc{{background:#ef6c00;color:#fff}}
pre{{font-size:12px;margin:0}}</style>
<h1>Spark 4.2 × Dell ObjectScale 4.4 — PoC Report</h1>
<p>Run {html.escape(results.run_id())} — generated {time.strftime('%Y-%m-%d %H:%M')}.
ObjectScale {html.escape(str(config.OBS_MGMT))} | Spark {html.escape(str(config.SPARK_MASTER))}</p>
<h2>Objectives</h2><table><tr><th>ID</th><th>Objective</th><th>Status</th><th>TCs</th></tr>{tr}</table>
<h2>Test-case detail</h2><table><tr><th>TC</th><th>Name</th><th>Status</th><th>Metrics</th></tr>{detail}</table>
{thr_html}
{env}
"""
pathlib.Path("/root/OBS/reports/PoC_Report.html").write_text(doc)
print("wrote reports/PoC_Report.html")
