"""Unit tests for poc.obs_admin with a mocked requests session (no network)."""
import pathlib, sys, types

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
