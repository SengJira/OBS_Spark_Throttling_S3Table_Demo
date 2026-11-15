"""ObjectScale management API client (port 4443).

Token auth: GET /login (basic root) -> X-SDS-AUTH-TOKEN.
The 4443 cert has SAN=localhost only, so hostname checking is disabled for this
endpoint ONLY — the certificate chain itself is still verified against the
imported OBS CA (no blanket verify=False)."""
import requests, urllib3, threading
from requests.adapters import HTTPAdapter

from . import config

# hostname verification disabled only on the session used for 4443;
# chain verification stays enabled (adapter still uses cert chain via CA bundle).
_urllib3_warnings_done = False


class _PinNoHostnameAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **kw):
        kw["assert_hostname"] = False
        kw["cert_reqs"] = "CERT_REQUIRED"
        kw["ca_certs"] = "/etc/ssl/certs/ca-certificates.crt"
        return super().init_poolmanager(connections, maxsize, block, **kw)


class MgmtApi:
    def __init__(self, base=None, user="root", password=None):
        self.base = (base or config.OBS_MGMT).rstrip("/")
        self.user = user
        self._password = password or config.OBS_ROOT_PASSWORD
        self.s = requests.Session()
        self.s.mount("https://", _PinNoHostnameAdapter())
        self.s.verify = "/etc/ssl/certs/ca-certificates.crt"
        self._token = None
        self._lock = threading.Lock()

    def login(self):
        r = self.s.get(f"{self.base}/login", auth=(self.user, self._password), timeout=15)
        r.raise_for_status()
        self._token = r.headers["X-SDS-AUTH-TOKEN"]
        return self._token

    def _hdrs(self):
        with self._lock:
            if not self._token:
                self.login()
        return {"X-SDS-AUTH-TOKEN": self._token}

    def req(self, method, path, retry=True, **kw):
        kw.setdefault("timeout", 30)
        hdrs = self._hdrs()
        if "headers" in kw:
            hdrs = {**hdrs, **kw.pop("headers")}
        r = self.s.request(method, f"{self.base}{path}", headers=hdrs, **kw)
        if r.status_code == 401 and retry:  # token expired
            with self._lock:
                self._token = None
            return self.req(method, path, retry=False, **kw)
        return r

    def get(self, path, **kw):
        return self.req("GET", path, **kw)

    def put(self, path, body=None, **kw):
        return self.req("PUT", path, json=body, **kw)

    def post(self, path, body=None, **kw):
        return self.req("POST", path, json=body, **kw)

    def delete(self, path, **kw):
        return self.req("DELETE", path, **kw)

    # ---------------- IAM Query API ----------------
    def iam(self, action, namespace=None, **params):
        """AWS IAM query-protocol call on 4443 with mgmt token.
        namespace: target OBS namespace (x-emc-namespace); defaults to
        config.OBS_NAMESPACE — IAM users are namespaced."""
        params = {"Action": action, "Version": "2010-05-08", **params}
        r = self.req("POST", "/iam", params=params,
                     headers={"x-emc-namespace": namespace or config.OBS_NAMESPACE})
        return r

    # ---------------- CMF config ----------------
    def get_config(self, name):
        """Read a CMF config param; returns configured or default value."""
        import xml.etree.ElementTree as ET
        r = self.get(f"/config/{name}")
        if r.status_code != 200:
            return None
        root = ET.fromstring(r.text)
        for tag in ("configured_value", "default_value"):
            el = root.find(f".//{tag}")
            if el is not None and el.text:
                return el.text
        return None

    def set_config(self, name, value):
        r = self.put("/config-admin",
                     body={"list_of_params": [{"name": name, "value": str(value)}]})
        r.raise_for_status()
        return r


# ---------------- Throttler ----------------
THROTTLE_STATE = "com.emc.ecs.common.request.throttle.mvp.state"
THROTTLE_RESPONSE_CODE = "com.emc.ecs.common.request.throttle.mvp.response_code"


class Throttler:
    """ObjectScale S3 Throttler (4.4 mvp). See reports/environment.md for schema."""

    def __init__(self, api: MgmtApi):
        self.api = api

    def get_config(self):
        r = self.api.get("/throttle/config")
        if r.status_code == 404:
            return {"throttle": {"rules": []}}
        r.raise_for_status()
        return r.json()

    def get_rules(self):
        return self.get_config().get("throttle", {}).get("rules", [])

    def put_config(self, rules):
        """Replace ALL rules. Empty list -> DELETE config (PUT requires >=1 rule)."""
        if not rules:
            r = self.api.delete("/throttle/config")
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return {}
        r = self.api.put("/throttle/config", body={"throttle": {"rules": rules}})
        r.raise_for_status()
        return r.json() if r.text else {}

    def update_rule(self, rule):
        r = self.api.put(f"/throttle/config/rule/{rule['id']}", body=rule)
        r.raise_for_status()
        return r.json() if r.text else {}

    def delete_rules(self, rule_ids=None):
        """Delete given rules (or all)."""
        if rule_ids is None:
            r = self.api.delete("/throttle/config")
        else:
            r = self.api.req("DELETE", "/throttle/config/rules",
                             json={"rules": [{"id": i} for i in rule_ids]})
        if r.status_code == 404:
            return
        r.raise_for_status()

    # ---- mode & response code ----
    def get_mode(self):
        v = self.api.get_config(THROTTLE_STATE)
        return (v or "Disabled").replace("_", "").lower()  # dryrun|enabled|disabled

    def set_mode(self, mode):
        """mode: 'enabled' | 'dryrun' | 'disabled'"""
        m = {"enabled": "Enabled", "dryrun": "Dry_Run", "disabled": "Disabled"}[mode.lower().replace("_", "")]
        self.api.set_config(THROTTLE_STATE, m)

    def get_response_code(self):
        return int(self.api.get_config(THROTTLE_RESPONSE_CODE) or 503)

    def set_response_code(self, code):
        assert code in (429, 503)
        self.api.set_config(THROTTLE_RESPONSE_CODE, str(code))

    def list_actions(self):
        r = self.api.get("/throttle/config/actions")
        r.raise_for_status()
        return r.json()

    def backup(self, path):
        import json
        snap = {
            "config": self.get_config(),
            "mode": self.get_mode(),
            "response_code": self.get_response_code(),
        }
        pathlib = __import__("pathlib")
        pathlib.Path(path).write_text(json.dumps(snap, indent=2))
        return snap

    def restore(self, path=None):
        path = path or config.BACKUP_FILE
        import json
        snap = json.loads(__import__("pathlib").Path(str(path)).read_text())
        self.put_config(snap["config"]["throttle"]["rules"])
        self.set_mode(snap["mode"])
        self.set_response_code(snap["response_code"])


def rule(id, limit_tps, principals=None, verbs=None, actions=None,
         paths=None, ips=None, scoping=None, rate_unit="TPS",
         description=None, disabled=False):
    """Build a throttle rule dict (schema verified in environment.md)."""
    f = {}
    if principals: f["principals"] = list(principals)
    if verbs:      f["verbs"] = list(verbs)
    if actions:    f["actions"] = list(actions)
    if paths:
        f["paths"] = {"values": [p[0] for p in paths],
                      "matcher": paths[0][1] if isinstance(paths[0], tuple) else "wildcard"}
    if ips:
        f["ips"] = {"values": [i[0] for i in ips],
                    "matcher": ips[0][1] if isinstance(ips[0], tuple) else "exact"}
    r = {"id": id, "rateUnit": rate_unit,
         "limit": {"value": int(limit_tps)}, "filters": f}
    if description: r["description"] = description
    if disabled:    r["isDisabled"] = True
    if scoping:     r["scoping"] = {"per": scoping}
    return r
