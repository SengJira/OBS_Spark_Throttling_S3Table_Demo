"""Remote noisy-PUT agent for the split-host throttling demo.

Runs on the second client host (for example U02/192.168.1.41). It owns only
the tenant-noisy S3 credentials and reuses poc.loadgen.LoadGen for the PUT
workers, so request timing/status telemetry is identical to local mode.

U01's notebook controls it over a small token-authenticated HTTP API and
pulls events back; phase attribution stays on U01 by request-start timestamp.

Run as a service:  python -m poc.noisy_agent   (see systemd/noisy-agent.service)
"""
import json
import logging
import threading
import time
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import config, loadgen, s3util

log = logging.getLogger("poc.noisy_agent")


class AgentState:
    def __init__(self, token="", idle_s=60.0):
        self.token = token
        self.idle_s = idle_s
        self.lg = None
        self.params = None
        self.last_seen = time.time()
        self.lock = threading.Lock()
        self._s3_checked = (0.0, None)
        self.idle_stopped = False
        self.watchdog_stop = threading.Event()

    def touch(self):
        self.last_seen = time.time()

    def noisy_status(self):
        if self.lg is None:
            return {"running": False, "threads": 0, "lingering": 0,
                    "totals": {"events": 0}}
        return {"running": self.lg.running("tenant-noisy"),
                "threads": len(self.lg.threads["tenant-noisy"]),
                "lingering": self.lg.lingering("tenant-noisy"),
                "totals": self.lg.totals()}

    def ensure_lg(self, body):
        run_id = body.get("run_id")
        if not run_id:
            raise ValueError("run_id is required")
        params = {"run_id": run_id,
                  "bucket": body.get("bucket") or config.OBS_BUCKET_THROTTLE,
                  "max_keys": int(body.get("max_keys", loadgen.MAX_KEYS)),
                  "chunk": int(body.get("chunk", loadgen.CHUNK)),
                  "timeout_s": tuple(body.get("timeout_s") or (3, 10))}
        with self.lock:
            if (self.lg is not None and self.params == params
                    and self.lg.running("tenant-noisy")):
                return self.lg
            if self.lg is not None and self.lg.running("tenant-noisy"):
                return self.lg          # never replace a live generator
            self.lg = loadgen.LoadGen(run_id=params["run_id"],
                                      bucket=params["bucket"],
                                      max_keys=params["max_keys"],
                                      chunk=params["chunk"],
                                      timeout_s=params["timeout_s"])
            self.params = params
            self.idle_stopped = False
            return self.lg

    def s3_status(self):
        now = time.time()
        if now - self._s3_checked[0] < 10:
            return self._s3_checked[1]
        try:
            cli = s3util.s3("noisy", max_attempts=1, timeout_s=(3, 5))
            cli.head_bucket(Bucket=config.OBS_BUCKET_THROTTLE)
            res = {"ok": True, "endpoint": config.OBS_S3_ENDPOINT,
                   "error": None}
        except Exception as e:
            res = {"ok": False, "endpoint": config.OBS_S3_ENDPOINT,
                   "error": f"{type(e).__name__}: {e}"[:200]}
        self._s3_checked = (now, res)
        return res

    def preflight(self, run_id):
        """One tenant-noisy PUT+GET proof from THIS host. The object stays
        under the demo run prefix so 99_reset cleans it."""
        key = f"{run_id}/probe/noisy-agent-{uuid.uuid4().hex[:8]}.bin"
        try:
            cli = s3util.s3("noisy", max_attempts=1)
            cli.put_object(Bucket=config.OBS_BUCKET_THROTTLE, Key=key,
                           Body=b"poc-noisy-agent")
            r = cli.get_object(Bucket=config.OBS_BUCKET_THROTTLE, Key=key)
            with r["Body"] as body:
                body.read()
            return {"ok": True,
                    "row": {"tenant": "tenant-noisy",
                            "access_key": config.mask(
                                (config.USERS.get("noisy") or ("", ""))[0]),
                            "own_object_rw": "OK",
                            "host": "remote-agent"},
                    "error": None}
        except Exception as e:
            return {"ok": False,
                    "row": {"tenant": "tenant-noisy", "access_key": "…",
                            "own_object_rw": "FAIL", "host": "remote-agent"},
                    "error": f"{type(e).__name__}: {e}"[:300]}

    def watchdog(self):
        """Safety: if U01 stops polling (kernel dead / network down), stop the
        noisy workers instead of leaving ObjectScale under load."""
        while not self.watchdog_stop.wait(5):
            try:
                if (self.lg is not None and self.lg.running("tenant-noisy")
                        and time.time() - self.last_seen > self.idle_s):
                    log.warning("no U01 heartbeat for %.0fs — stopping noisy",
                                time.time() - self.last_seen)
                    self.lg.stop_noisy()
                    self.idle_stopped = True
            except Exception:
                log.exception("watchdog failed")


def _event_json(e):
    return {"ts": e.ts, "tenant": e.tenant, "op": e.op, "lat_ms": e.lat_ms,
            "status": e.status, "code": e.code, "retries": e.retries,
            "phase": e.phase}


class Handler(BaseHTTPRequestHandler):
    server_version = "poc-noisy-agent/1"

    def log_message(self, fmt, *args):
        log.debug("%s - %s", self.address_string(), fmt % args)

    # ---------- helpers ----------
    def _send(self, code, obj):
        raw = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _json(self, obj, code=200):
        self._send(code, obj)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        return json.loads(self.rfile.read(n) or b"{}")

    def _authed(self):
        state = self.server.state
        want = state.token
        got = self.headers.get("X-POC-Token", "")
        if want and got != want:
            self._json({"ok": False, "error": "bad token"}, 401)
            return False
        state.touch()
        return True

    def _qs(self):
        return urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

    # ---------- GET ----------
    def do_GET(self):
        if not self._authed():
            return
        state = self.server.state
        path = urllib.parse.urlparse(self.path).path
        try:
            if path == "/health":
                client_ts = self.headers.get("X-POC-Time")
                offset_ms = None
                if client_ts:
                    try:
                        offset_ms = round(
                            (time.time() - float(client_ts)) * 1000, 3)
                    except ValueError:
                        pass
                self._json({"ok": True, "service": "poc-noisy-agent",
                            "agent_time": time.time(), "offset_ms": offset_ms,
                            "noisy": state.noisy_status(),
                            "s3": state.s3_status()})
            elif path == "/status":
                self._json({"ok": True, "agent_time": time.time(),
                            "noisy": state.noisy_status(),
                            "idle_stopped": state.idle_stopped})
            elif path == "/events":
                since = int(self._qs().get("since", ["0"])[0])
                if state.lg is None:
                    self._json({"events": [], "cursor": since,
                                "agent_time": time.time()})
                    return
                evs, cur = state.lg.events_since(since)
                self._json({"events": [_event_json(e) for e in evs],
                            "cursor": cur, "agent_time": time.time()})
            else:
                self._json({"ok": False, "error": "not found"}, 404)
        except Exception as e:
            self._json({"ok": False, "error": f"{type(e).__name__}: {e}"}, 500)

    # ---------- POST ----------
    def do_POST(self):
        if not self._authed():
            return
        state = self.server.state
        path = urllib.parse.urlparse(self.path).path
        try:
            body = self._body()
            if path == "/preflight":
                run_id = body.get("run_id")
                if not run_id:
                    self._json({"ok": False, "error": "run_id required"}, 400)
                    return
                self._json(state.preflight(run_id))
            elif path == "/noisy/start":
                lg = state.ensure_lg(body)
                n = lg.add_noisy(int(body.get("threads", 6)))
                self._json({"ok": True, "launched": n,
                            "running": lg.running("tenant-noisy"),
                            "threads": len(lg.threads["tenant-noisy"]),
                            "lingering": lg.lingering("tenant-noisy"),
                            "run_id": lg.run_id})
            elif path == "/noisy/stop":
                n = state.lg.stop_noisy() if state.lg is not None else 0
                self._json({"ok": True, "lingering": n,
                            "running": bool(state.lg and
                                            state.lg.running("tenant-noisy"))})
            elif path == "/noisy/pause":
                if state.lg is not None:
                    state.lg.pause_noisy()
                self._json({"ok": True})
            elif path == "/noisy/resume":
                if state.lg is not None:
                    state.lg.resume_noisy()
                self._json({"ok": True})
            elif path == "/burst":
                agg = loadgen.put_burst(
                    "noisy", body.get("bucket") or config.OBS_BUCKET_THROTTLE,
                    body.get("prefix", ""), seconds=float(body.get("seconds", 20)),
                    threads=int(body.get("threads", 4)),
                    size=int(body.get("size", 1024)),
                    keys_per_thread=int(body.get("keys_per_thread", 50)))
                self._json({"ok": True, "burst": agg})
            else:
                self._json({"ok": False, "error": "not found"}, 404)
        except Exception as e:
            self._json({"ok": False, "error": f"{type(e).__name__}: {e}"}, 500)


def create_server(host, port, state):
    srv = ThreadingHTTPServer((host, port), Handler)
    srv.state = state
    return srv


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    state = AgentState(token=config.NOISY_AGENT_TOKEN,
                       idle_s=config.NOISY_AGENT_IDLE_S)
    host = config.NOISY_AGENT_BIND or "0.0.0.0"
    port = int(config.NOISY_AGENT_PORT)
    if not state.token and host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit("NOISY_AGENT_TOKEN is required when the agent binds "
                         "beyond loopback")
    srv = create_server(host, port, state)
    wd = threading.Thread(target=state.watchdog, daemon=True)
    wd.start()
    log.info("noisy agent listening on %s:%s (token %s, idle stop %.0fs)",
             host, port, "set" if state.token else "DISABLED",
             state.idle_s)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        state.watchdog_stop.set()
        srv.server_close()


if __name__ == "__main__":
    main()
