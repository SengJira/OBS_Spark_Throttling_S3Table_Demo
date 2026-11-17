"""Shared fixtures: offline tests must never touch the lab's run state."""
import pathlib, sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import results


@pytest.fixture(autouse=True)
def _isolated_run_state(tmp_path, monkeypatch):
    monkeypatch.setattr(results, "RAW", tmp_path / "raw")
    monkeypatch.setattr(results, "RUN_ID_FILE", tmp_path / "current_run_id")
    monkeypatch.setenv(results.RUN_ID_ENV, "demo-unit-test")
    yield
