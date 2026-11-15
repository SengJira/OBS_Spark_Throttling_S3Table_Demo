"""Scorecard semantics: missing, stale, inconclusive and failed test cases."""
import json, pathlib, sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import results

O1 = results.OBJECTIVES["O1"][1]


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(results, "RAW", tmp_path / "raw")
    monkeypatch.setattr(results, "RUN_ID_FILE", tmp_path / "current_run_id")
    monkeypatch.setattr(results.config, "RUN_ID", None)
    monkeypatch.setenv(results.RUN_ID_ENV, "demo-run-a")
    yield


def _row(oid="O1"):
    return next(r for r in results.scorecard() if r["objective"] == oid)


def _write_all(tcs, status="PASS"):
    for tc in tcs:
        results.write(tc, "t", status=status)


def test_all_pass():
    _write_all(O1)
    assert _row()["status"] == "PASS"
    assert _row()["test_cases"] == f"{len(O1)}/{len(O1)}"


def test_missing_tc_is_incomplete():
    _write_all(O1[:-1])
    row = _row()
    assert row["status"] == "INCOMPLETE" and row["missing"] == [O1[-1]]


def test_no_results_is_incomplete_not_fail_or_pass():
    assert _row()["status"] == "INCOMPLETE"


def test_inconclusive_is_never_pass():
    _write_all(O1)
    results.write(O1[0], "t", status="INCONCLUSIVE")
    assert _row()["status"] == "INCONCLUSIVE"
    assert _row()["not_pass"] == {O1[0]: "INCONCLUSIVE"}


def test_fail_beats_inconclusive():
    _write_all(O1)
    results.write(O1[0], "t", status="INCONCLUSIVE")
    results.write(O1[1], "t", False)
    assert _row()["status"] == "FAIL"


def test_stale_results_from_another_run_are_ignored(monkeypatch):
    _write_all(O1)                                  # run a: all green
    monkeypatch.setenv(results.RUN_ID_ENV, "demo-run-b")
    _write_all(O1[:2])                              # run b: only two so far
    row = _row()
    assert row["run_id"] == "demo-run-b"
    assert row["status"] == "INCOMPLETE"
    assert row["stale"] == O1[2:] == row["missing"]


def test_write_status_and_passed_compat():
    assert results.write("TC0.1", "t", True)["status"] == "PASS"
    assert results.write("TC0.1", "t", False)["status"] == "FAIL"
    rec = results.write("TC0.1", "t", status="INCONCLUSIVE")
    assert rec["status"] == "INCONCLUSIVE" and rec["passed"] is False
    with pytest.raises(ValueError):
        results.write("TC0.1", "t", status="MAYBE")


def test_legacy_record_without_status_field():
    _write_all(O1)
    f = results.RAW / f"{O1[0]}.json"
    rec = json.loads(f.read_text()); del rec["status"]; rec["passed"] = False
    f.write_text(json.dumps(rec))
    assert _row()["status"] == "FAIL"


def test_overall_follows_precedence():
    for _, tcs in results.OBJECTIVES.values():
        _write_all(tcs)
    assert results.overall() == "PASS"
    results.write("TC2.9", "t", status="INCONCLUSIVE")
    assert results.overall() == "INCONCLUSIVE"


# ---- run id (#11) ----

def test_run_id_env_wins_and_is_persisted():
    assert results.run_id() == "demo-run-a"
    assert results.RUN_ID_FILE.read_text().strip() == "demo-run-a"


def test_run_id_shared_through_file_across_kernels(monkeypatch):
    monkeypatch.delenv(results.RUN_ID_ENV)
    first = results.run_id()                       # minted + persisted
    assert first.startswith("demo-")
    assert results.run_id() == first               # a second "kernel" agrees


def test_run_id_from_dotenv_is_prefixed_for_reset(monkeypatch):
    monkeypatch.delenv(results.RUN_ID_ENV)
    monkeypatch.setattr(results.config, "RUN_ID", "customerA")
    assert results.run_id() == "demo-customerA"


def test_new_run_starts_a_fresh_id(monkeypatch):
    monkeypatch.delenv(results.RUN_ID_ENV)
    results.RUN_ID_FILE.write_text("demo-old\n")
    assert results.run_id() == "demo-old"
    new = results.new_run()
    assert new != "demo-old" and results.run_id() == new


def test_new_run_keeps_suite_wide_id():
    assert results.new_run() == "demo-run-a"
