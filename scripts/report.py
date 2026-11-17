#!/usr/bin/env python3
"""Generate reports/PoC_Report.html from reports/raw/TC*.json + environment.

The report covers ONE run: the run ID stored in the result files (the active
run from reports/current_run_id, else the most recent result) — it never
mints a new ID. Results of other runs are listed as stale, not scored.
"""
import html, json, os, pathlib, sys, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from poc import results, config          # noqa: E402

CSS = {"PASS": "pass", "FAIL": "fail", "INCONCLUSIVE": "inc", "INCOMPLETE": "incomplete"}


def report_run_id():
    """The active run only — never fall back to another run's results."""
    rid = os.environ.get(results.RUN_ID_ENV)
    if not rid and results.RUN_ID_FILE.exists():
        rid = results.RUN_ID_FILE.read_text().strip()
    return rid


def cell(status):
    return f"<td class='{CSS.get(status, 'fail')}'>{html.escape(status)}</td>"


def tc_key(tc):
    try:
        return [float(x) for x in tc[2:].split(".")]
    except ValueError:
        return [1e9]


def build(rid=None):
    rid = rid or report_run_id()
    if not rid:
        raise SystemExit("no results in reports/raw — run the notebooks first")
    rows = results.scorecard(rid)
    allr = results.load_all(rid)
    stale = sorted(set(results.load_all()) - set(allr), key=tc_key)
    tr = "".join(
        f"<tr><td>{o['objective']}</td><td>{html.escape(o['description'])}</td>"
        f"{cell(o['status'])}<td>{o['test_cases']}</td>"
        f"<td>{html.escape(', '.join(o['missing']) or '—')}</td>"
        f"<td>{html.escape(', '.join(f'{t}: {s}' for t, s in o['not_pass'].items()) or '—')}</td></tr>"
        for o in rows)
    detail = ("<tr><td>—</td><td>no results</td>"
              "<td class='incomplete'>INCOMPLETE</td>"
              f"<td>no results for run {html.escape(rid)}</td><td>—</td></tr>"
              if not allr else "".join(
        f"<tr><td>{tc}</td><td>{html.escape(str(r.get('name', '')))}</td>"
        f"{cell(results.status_of(r))}"
        f"<td>{html.escape(str(r.get('detail') or ''))}</td>"
        f"<td><pre>{html.escape(json.dumps(r.get('metrics', {}), indent=1)[:700])}</pre></td></tr>"
        for tc, r in sorted(allr.items(), key=lambda kv: tc_key(kv[0]))))

    thr_html = ""
    t29 = allr.get("TC2.9", {}).get("metrics", {})
    if t29:
        vrows = "".join(
            f"<tr><td>{html.escape(v['check'])}</td>{cell(v['verdict'])}"
            f"<td>{html.escape(str(v['detail']))}</td></tr>"
            for v in t29.get("verdicts", []))
        phases = "".join(
            f"<tr><td>{tc}</td>{cell(results.status_of(allr[tc]))}"
            f"<td><pre>{html.escape(json.dumps(allr[tc]['metrics'], indent=1)[:1200])}</pre></td></tr>"
            for tc in ("TC2.1", "TC2.3", "TC2.10", "TC2.4", "TC2.5", "TC2.6", "TC2.7")
            if tc in allr)
        srv = ("available" if t29.get("server_metrics_available")
               else "not exposed on this build")
        corr = (f"client-side throttle responses in the enforced phase: "
                f"{t29.get('client_throttled_enforced_phase')} — server-side counters {srv} "
                f"(probed: {html.escape(json.dumps(t29.get('probed', {})))})")
        thr_html = f"""<h2>S3 Throttler — performance isolation</h2>
<p><b>Overall verdict: {html.escape(str(t29.get('overall', '?')))}</b>
 · enforcement mode: {html.escape(str(t29.get('enforcement_mode', '?')))} · {corr}</p>
<table><tr><th>Check</th><th>Verdict</th><th>Detail</th></tr>{vrows}</table>
<h3>Phase metrics</h3><table><tr><th>TC</th><th>Status</th><th>Metrics</th></tr>{phases}</table>"""

    env = ""
    ep = config.REPORT_DIR / "environment.md"
    if ep.exists():
        env = f"<h2>Environment</h2><pre>{html.escape(ep.read_text())}</pre>"
    overall = results.overall(rid)
    stale_note = (f"<p class='note'>Ignored (other runs): {html.escape(', '.join(stale))}</p>"
                  if stale else "")
    return f"""<!doctype html><meta charset=utf-8><title>ObjectScale 4.4 + Spark 4.2 PoC</title>
<style>
body{{font-family:sans-serif;margin:2em}}table{{border-collapse:collapse;margin-bottom:1em}}
td,th{{border:1px solid #ccc;padding:6px 12px;vertical-align:top}}
.pass{{background:{results.STATUS_COLORS['PASS']};color:#fff}}
.fail{{background:{results.STATUS_COLORS['FAIL']};color:#fff}}
.inc{{background:{results.STATUS_COLORS['INCONCLUSIVE']};color:#fff}}
.incomplete{{background:{results.STATUS_COLORS['INCOMPLETE']};color:#fff}}
pre{{font-size:12px;margin:0;white-space:pre-wrap}}.note{{color:#555}}
.legend span{{padding:2px 8px;margin-right:6px}}</style>
<h1>Spark 4.2 × Dell ObjectScale 4.4 — PoC Report</h1>
<p>Run <b>{html.escape(rid)}</b> — generated {time.strftime('%Y-%m-%d %H:%M')}.
ObjectScale {html.escape(str(config.OBS_MGMT))} | Spark {html.escape(str(config.SPARK_MASTER))}</p>
<p class=legend><span class=pass>PASS</span><span class=fail>FAIL</span>
<span class=inc>INCONCLUSIVE — evidence does not decide; not a pass</span>
<span class=incomplete>INCOMPLETE — a test case of this run is missing</span></p>
<h2>Objectives — overall <span class='{CSS.get(overall, 'fail')}'>&nbsp;{overall}&nbsp;</span></h2>
<table><tr><th>ID</th><th>Objective</th><th>Status</th><th>TCs</th><th>Missing</th><th>Not PASS</th></tr>{tr}</table>
{stale_note}
<h2>Test-case detail</h2>
<table><tr><th>TC</th><th>Name</th><th>Status</th><th>Reason</th><th>Metrics</th></tr>{detail}</table>
{thr_html}
{env}
"""


if __name__ == "__main__":
    out = config.REPORT_DIR / "PoC_Report.html"
    out.write_text(build())
    print(f"wrote {out} (run {report_run_id()})")
