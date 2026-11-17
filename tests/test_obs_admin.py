"""Unit tests for poc.obs_admin with a mocked requests session (no network)."""
import json, pathlib, sys, types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import obs_admin


class FakeSession:
    """Scripted requests.Session: pops one status per request()."""

    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = []
        self.logins = 0

    def get(self, url, auth=None, timeout=None):
        self.logins += 1
        return types.SimpleNamespace(
            status_code=200, headers={"X-SDS-AUTH-TOKEN": f"tok{self.logins}"},
            raise_for_status=lambda: None)

    def request(self, method, url, headers=None, **kw):
        self.calls.append({"method": method, "url": url,
                           "headers": dict(headers or {}), "kw": kw})
        return types.SimpleNamespace(status_code=self.statuses.pop(0),
                                     text="", headers={})


def _api(statuses):
    api = obs_admin.MgmtApi(base="https://mgmt.invalid:4443", password="x")
    api.s = FakeSession(statuses)
    return api


def test_401_retry_keeps_custom_headers_and_refreshes_token():
    api = _api([401, 200])
    r = api.iam("ListUsers", namespace="poc")
    assert r.status_code == 200
    first, second = api.s.calls
    assert first["headers"]["x-emc-namespace"] == "poc"
    assert second["headers"]["x-emc-namespace"] == "poc"
    assert first["headers"]["X-SDS-AUTH-TOKEN"] == "tok1"
    assert second["headers"]["X-SDS-AUTH-TOKEN"] == "tok2"
    assert second["kw"]["params"]["Action"] == "ListUsers"


def test_401_is_retried_only_once():
    api = _api([401, 401])
    assert api.get("/x").status_code == 401
    assert len(api.s.calls) == 2


# ---------- throttler backup / restore (#10) ----------

class RecordingThrottler(obs_admin.Throttler):
    """Throttler with the HTTP layer replaced by an in-memory state + op log."""

    def __init__(self, rules=(), mode="disabled", code=503):
        self.rules, self.mode, self.code = list(rules), mode, code
        self.ops = []

    def get_config(self):
        return {"throttle": {"rules": list(self.rules)}}

    def get_mode(self):
        return self.mode

    def get_response_code(self):
        return self.code

    def set_mode(self, mode):
        self.ops.append(("mode", mode)); self.mode = mode

    def set_response_code(self, code):
        self.ops.append(("code", code)); self.code = code

    def put_config(self, rules):
        self.ops.append(("rules", [r["id"] for r in rules]))
        self.rules = list(rules)


def test_safe_backup_writes_once_and_never_overwrites(tmp_path):
    f = tmp_path / "b.json"
    t = RecordingThrottler()
    assert t.safe_backup(f)[0] == "written"
    t.rules, t.mode = [{"id": "demo-x"}], "enabled"     # demo now running
    assert t.safe_backup(f) == ("kept", None)
    assert json.loads(f.read_text())["config"]["throttle"]["rules"] == []


@pytest.mark.parametrize("rules,mode", [
    ([{"id": "demo-noisy-put"}], "disabled"),
    ([], "enabled"),
    ([], "dryrun"),
])
def test_safe_backup_refuses_tainted_live_state(tmp_path, rules, mode):
    f = tmp_path / "b.json"
    with pytest.raises(obs_admin.BackupRefused):
        RecordingThrottler(rules, mode).safe_backup(f)
    assert not f.exists()


def test_safe_backup_force_overrides(tmp_path):
    f = tmp_path / "b.json"
    f.write_text("{}")
    what, snap = RecordingThrottler([{"id": "demo-x"}], "enabled").safe_backup(
        f, force=True)
    assert what == "written" and snap["mode"] == "enabled"


def test_safe_backup_accepts_non_demo_production_rules(tmp_path):
    what, _ = RecordingThrottler([{"id": "prod-rule"}]).safe_backup(
        tmp_path / "b.json")
    assert what == "written"


def test_restore_disables_first_then_rules_then_mode_and_code(tmp_path):
    f = tmp_path / "b.json"
    f.write_text(json.dumps({"config": {"throttle": {"rules": [{"id": "prod"}]}},
                             "mode": "enabled", "response_code": 429}))
    t = RecordingThrottler([{"id": "demo-x"}], "enabled", 503)
    t.restore(f)
    assert t.ops == [("mode", "disabled"), ("rules", ["prod"]),
                     ("code", 429), ("mode", "enabled")]


def test_restore_missing_backup_raises_clear_error(tmp_path):
    t = RecordingThrottler([{"id": "demo-x"}], "enabled")
    with pytest.raises(obs_admin.BackupMissing, match="not found"):
        t.restore(tmp_path / "nope.json")
    assert t.ops == []          # nothing touched


def test_restore_logged_reports_instead_of_raising(tmp_path, capsys):
    assert RecordingThrottler().restore_logged(tmp_path / "nope.json") is None
    assert "restore failed" in capsys.readouterr().out
