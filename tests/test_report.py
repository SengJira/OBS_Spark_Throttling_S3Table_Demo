"""V4-7: the HTML report covers the active run only — never another run's
results, and shows INCOMPLETE when the active run has no results."""
import importlib.util
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import results  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "poc_report", ROOT / "scripts" / "report.py")
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)


def _write_result(run_id, tc="TC0.1", status="PASS"):
    results.RAW.mkdir(parents=True, exist_ok=True)
    rec = {"tc": tc, "run_id": run_id, "name": "x", "status": status,
           "detail": "", "metrics": {}, "ts": 1}
    (results.RAW / f"{tc}.json").write_text(json.dumps(rec))


def test_report_run_id_is_active_run_only():
    _write_result("demo-other-run")
    # conftest sets POC_RUN_ID=demo-unit-test; the stored result belongs to
    # a different run and must not be picked up as the report's run.
    assert report.report_run_id() == "demo-unit-test"


def test_build_reports_incomplete_for_active_run_without_results():
    _write_result("demo-other-run")
    html = report.build("demo-unit-test")
    assert "no results for run demo-unit-test" in html
    assert "INCOMPLETE" in html
    assert "demo-other-run" not in html.split("Ignored (other runs)")[0]


def test_build_scores_only_the_given_run():
    _write_result("demo-other-run", tc="TC0.1", status="PASS")
    _write_result("demo-unit-test", tc="TC0.1", status="FAIL")
    html = report.build("demo-unit-test")
    assert "demo-unit-test" in html
    # the other run's PASS result must not leak into this run's detail
    detail = html.split("Test-case detail")[1]
    assert detail.count("<tr><td>") == 1
