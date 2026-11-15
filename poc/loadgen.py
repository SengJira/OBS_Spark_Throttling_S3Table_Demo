"""Background S3 load generator: noisy tenant PUTs, critical tenant GETs.

v2 — per-tenant worker control (start/stop/pause/resume), named phases with
start/end timestamps and per-phase accumulation (immune to event-deque
eviction), full request telemetry (HTTP status, S3 error code, retry count,
monotonic duration), split ok/failed percentiles, 429-vs-503 classification,
and a sustained-load check for the noisy tenant.

Events are tagged with the phase that was active when the request STARTED;
requests straddling a phase boundary are counted in their start phase and
reported separately as `boundary` ops. The bounded deque feeds the live
chart only — phase_stats() never loses data to deque overflow.

All objects land under the demo run_id prefix so 99_reset can clean them.
This module never touches throttle configuration — rule changes belong to
the approved lab workflow (notebook 02 / scripts/prep_obs.py backup+restore).
"""
import threading, time, collections, statistics, itertools, contextlib
from . import s3util, config, results

MAX_KEYS = 20000           # cap noisy objects per run (0 = unlimited)
CHUNK = 64 * 1024          # 64 KiB payload per PUT
STATUS_OK = 200
TENANTS = ("tenant-noisy", "tenant-critical")

Event = collections.namedtuple(
    "Event", "ts tenant op lat_ms status code retries phase")


def _status(e):
    return (getattr(e, "response", None) or {}).get(
        "ResponseMetadata", {}).get("HTTPStatusCode", 599)


def _code(e):
    return (getattr(e, "response", None) or {}).get(
        "Error", {}).get("Code", "")


def _retries(e):
    return int((getattr(e, "response", None) or {}).get(
        "ResponseMetadata", {}).get("RetryAttempts", 0))


def _ok_retries(r):
    return int((r or {}).get("ResponseMetadata", {}).get("RetryAttempts", 0))


def classify(status, code=""):
    """ok | r429 | r503s (SlowDown) | r503o (other 503) | error."""
    if status == 429:
        return "r429"
    if status == 503:
        return "r503s" if code == "SlowDown" else "r503o"
    if status >= 400:
        return "error"
    return "ok"


THROTTLED = {"r429", "r503s"}          # what counts as a throttling signal


def pctile(lats, p):
    """Linear-interpolation percentile (0-100) without numpy."""
    if not lats:
        return 0.0
    s = sorted(lats)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    return s[f] if f == k else s[f] + (s[f + 1] - s[f]) * (k - f)


class LoadGen:
    def __init__(self, bucket=config.OBS_BUCKET_THROTTLE, chunk=CHUNK,
                 max_keys=MAX_KEYS, timeout_s=(3, 10)):
        self.bucket = bucket
        self.chunk = chunk
        self.max_keys = max_keys
        self.timeout_s = timeout_s
        self.events = collections.deque(maxlen=250000)   # live-chart buffer
        self.stop_flag = threading.Event()
        self.paused = threading.Event()                  # global pause
        self._tenant_stop = {t: threading.Event() for t in TENANTS}
        self._tenant_pause = {t: threading.Event() for t in TENANTS}
        self.threads = {t: [] for t in TENANTS}
        self._lock = threading.Lock()
        self._keys_reserved = 0
        self._keys_ok = 0
        self._keys_fail = 0
        self._key_seq = itertools.count(1)
        self._worker_seq = itertools.count(1)
        self._payload = b"x" * chunk
        self.seed_key = None
        self.phases = []                    # [{name,tag,start,end,seconds}]
        self._phase = "init"
        self._phase_tag = None
        self._phase_seq = 0
        self._phase_start = None            # wall
        self._phase_start_m = None          # monotonic
        # tag -> group -> list of (ts, lat_ms, status, code, retries)
        self._agg_buf = collections.defaultdict(
            lambda: collections.defaultdict(list))

    # ---------- phases ----------
    def set_phase(self, name):
        """Close the current phase and open `name`. Backward compatible."""
        self.start_phase(name)

    def start_phase(self, name):
        with self._lock:
            self._close_phase_locked(time.time(), time.monotonic())
            self._phase_seq += 1
            self._phase = name
            self._phase_tag = f"{name}#{self._phase_seq}"
            self._phase_start = time.time()
            self._phase_start_m = time.monotonic()

    def _close_phase_locked(self, wall, mono):
        if self._phase_tag is not None and self._phase_start is not None:
            self.phases.append({"name": self._phase, "tag": self._phase_tag,
                                "start": self._phase_start, "end": wall,
                                "seconds": mono - self._phase_start_m})

    def end_phase(self):
        with self._lock:
            self._close_phase_locked(time.time(), time.monotonic())
            self._phase, self._phase_tag = "idle", None
            self._phase_start = self._phase_start_m = None

    @property
    def phase(self):
        with self._lock:
            return self._phase

    @property
    def _cur_tag(self):
        with self._lock:
            return self._phase_tag

    # ---------- metrics ----------
    def _rec(self, tenant, op, ts, latency_ms, status, code="", retries=0,
             tag=None):
        ev = Event(ts, tenant, op, latency_ms, status, code, retries,
                   tag or self._phase or "init")
        self.events.append(ev)
        with self._lock:
            self._agg_buf[ev.phase][f"{tenant} {op}"].append(
                (ts, latency_ms, status, code, retries))

    def _events(self):
        for _ in range(10):
            try:
                return list(self.events)
            except RuntimeError:
                continue
        return list(self.events)

    @staticmethod
    def _agg(evs, span, phase_end=None):
        """Aggregate [(ts,lat,status,code,retries)] or Event tuples."""
        lats, ok_lats, fail_lats = [], [], []
        codes = collections.Counter()
        n_ok = n_thr = n_err = 0
        r429 = r503s = r503o = 0
        ret_ok = ret_fail = boundary = 0
        for e in evs:
            ts, lat, status, code, retries = e
            cls = classify(status, code)
            lats.append(lat)
            codes[code or str(status)] += 1
            if cls == "ok":
                n_ok += 1; ok_lats.append(lat); ret_ok += retries
            else:
                fail_lats.append(lat); ret_fail += retries
                if cls in THROTTLED:
                    n_thr += 1
                    if cls == "r429": r429 += 1
                    else: r503s += 1
                else:
                    n_err += 1
                    if cls == "r503o": r503o += 1
            if phase_end and ts + lat / 1000.0 > phase_end:
                boundary += 1
        n = len(evs)
        return {
            "n": n, "tps": n / max(1e-9, span),
            "p50": pctile(lats, 50), "p95": pctile(lats, 95),
            "p99": pctile(lats, 99),
            "mean_ms": round(statistics.fmean(lats), 1) if lats else 0,
            "ok": n_ok, "throttled": n_thr, "errors": n_err,
            "r429": r429, "r503_slowdown": r503s, "r503_other": r503o,
            "ok_p50": pctile(ok_lats, 50), "ok_p95": pctile(ok_lats, 95),
            "ok_p99": pctile(ok_lats, 99),
            "fail_p50": pctile(fail_lats, 50),
            "fail_p95": pctile(fail_lats, 95),
            "fail_p99": pctile(fail_lats, 99),
            "retries": ret_ok + ret_fail,
            "retries_ok": ret_ok, "retries_fail": ret_fail,
            "boundary": boundary, "codes": dict(codes),
        }

    def snapshot(self, since=None):
        """Metrics for the recent window: {tenant: {op: {tps,p50,p95,p99,...}}}."""
        now = time.time()
        win = [e for e in self._events() if e.ts >= (since if since else now - 10)]
        out = {}
        for e in win:
            out.setdefault(e.tenant, {}).setdefault(e.op, []).append(e)
        span = max(1.0, now - (since if since else now - 10))
        return {t: {o: self._agg(
                    [(e.ts, e.lat_ms, e.status, e.code, e.retries) for e in evs],
                    span) for o, evs in ops.items()}
                for t, ops in out.items()}

    def phase_stats(self):
        """Per-phase aggregates keyed by unique phase name (reused names get
        a '#2' suffix). Data comes from the per-phase accumulator, so deque
        overflow cannot lose the final phase. The currently open phase is
        included using 'now' as its end."""
        entries = list(self.phases)
        with self._lock:
            if self._phase_tag is not None and self._phase_start is not None:
                entries.append({"name": self._phase, "tag": self._phase_tag,
                                "start": self._phase_start, "end": time.time(),
                                "seconds": time.monotonic()
                                           - self._phase_start_m})
        out, seen = {}, collections.Counter()
        for ph in entries:
            seen[ph["name"]] += 1
            key = ph["name"] if seen[ph["name"]] == 1 \
                else f"{ph['name']}#{seen[ph['name']]}"
            groups = {}
            for grp, evs in self._agg_buf.get(ph["tag"], {}).items():
                groups[grp] = self._agg(evs, ph["seconds"], ph["end"])
            out[key] = {"_window": {"start": ph["start"], "end": ph["end"],
                                    "seconds": round(ph["seconds"], 1)},
                        **groups}
        return out

    def totals(self):
        th = err = ok = 0
        for e in self._events():
            c = classify(e.status, e.code)
            if c in THROTTLED: th += 1
            elif c == "ok": ok += 1
            else: err += 1
        return {"events": len(self.events), "ok": ok, "throttled": th,
                "errors": err,
                "noisy_objects": (min(self._keys_reserved, self.max_keys)
                                  if self.max_keys else self._keys_reserved),
                "keys_reserved": self._keys_reserved,
                "keys_ok": self._keys_ok,
                "keys_failed": self._keys_fail}

    def sustained(self, window_s=10, min_tps=0.5, tenant="tenant-noisy"):
        """False when the tenant produced < min_tps in the last window_s —
        catches a silently dead or exhausted load during long phases."""
        snap = self.snapshot(since=time.time() - window_s)
        ops = snap.get(tenant, {})
        return any(v["tps"] >= min_tps for v in ops.values())

    # ---------- key reservation (atomic) ----------
    def _next_key_seq(self):
        """Reserve a key slot atomically. Once max_keys objects exist, the
        ring recycles — PUTs overwrite existing keys so the offered rate is
        sustained without unbounded object growth."""
        with self._lock:
            self._keys_reserved += 1
            seq = next(self._key_seq)
            if self.max_keys:
                seq = (seq - 1) % self.max_keys + 1
            return seq

    def _key_done(self, ok):
        with self._lock:
            if ok:
                self._keys_ok += 1
            else:
                self._keys_fail += 1

    # ---------- workers ----------
    def _stopped(self, tenant):
        return (self.stop_flag.is_set()
                or self._tenant_stop[tenant].is_set())

    def _paused(self, tenant):
        return self.paused.is_set() or self._tenant_pause[tenant].is_set()

    def _noisy_put(self, worker_id):
        cli = s3util.s3("noisy", timeout_s=self.timeout_s, max_attempts=1)
        rid = results.run_id()
        while not self._stopped("tenant-noisy"):
            if self._paused("tenant-noisy"):
                time.sleep(0.25); continue
            seq = self._next_key_seq()
            key = f"{rid}/noisy/n{seq}.bin"
            tag, ts, t0 = self._cur_tag, time.time(), time.monotonic()
            try:
                r = cli.put_object(Bucket=self.bucket, Key=key,
                                   Body=self._payload,
                                   ContentLength=self.chunk)
                self._rec("tenant-noisy", "PUT", ts,
                          (time.monotonic()-t0)*1000, STATUS_OK,
                          retries=_ok_retries(r), tag=tag)
                self._key_done(True)
            except Exception as e:
                self._rec("tenant-noisy", "PUT", ts,
                          (time.monotonic()-t0)*1000,
                          _status(e), _code(e), _retries(e), tag=tag)
                self._key_done(False)
                time.sleep(0.05)  # brief backoff on throttle/errors

    def _critical_get(self, key, interval=0):
        cli = s3util.s3("critical", timeout_s=self.timeout_s, max_attempts=1)
        while not self._stopped("tenant-critical"):
            if self._paused("tenant-critical"):
                time.sleep(0.25); continue
            tag, ts, t0 = self._cur_tag, time.time(), time.monotonic()
            try:
                r = cli.get_object(Bucket=self.bucket, Key=key)
                with contextlib.closing(r["Body"]) as body:
                    body.read()   # consume + close: true latency, reuse conn
                self._rec("tenant-critical", "GET", ts,
                          (time.monotonic()-t0)*1000, STATUS_OK,
                          retries=_ok_retries(r), tag=tag)
            except Exception as e:
                self._rec("tenant-critical", "GET", ts,
                          (time.monotonic()-t0)*1000,
                          _status(e), _code(e), _retries(e), tag=tag)
                time.sleep(0.05)
            if interval:
                time.sleep(interval)

    # ---------- control ----------
    def running(self, tenant=None):
        if tenant:
            return any(t.is_alive() for t in self.threads[tenant])
        return any(t.is_alive() for ts in self.threads.values() for t in ts)

    def lingering(self, tenant=None):
        """Workers still alive — should be 0 after a successful stop."""
        if tenant:
            return sum(1 for t in self.threads[tenant] if t.is_alive())
        return sum(1 for ts in self.threads.values()
                   for t in ts if t.is_alive())

    def start(self, seed_key=None, noisy=True, critical=True,
              noisy_threads=6, critical_threads=2, critical_interval=0):
        """Seed one critical object if needed; launch workers.
        Re-calling on a running generator is a no-op returning the seed."""
        if self.running():
            return self.seed_key
        if seed_key is None:
            seed_key = f"{results.run_id()}/critical-seed.bin"
            s3util.s3("critical").put_object(
                Bucket=self.bucket, Key=seed_key, Body=self._payload)
        self.seed_key = seed_key
        self.stop_flag.clear(); self.paused.clear()
        for e in self._tenant_stop.values():
            e.clear()
        if noisy:
            self.add_noisy(noisy_threads)
        if critical:
            self.add_critical(critical_threads, seed_key, critical_interval)
        return seed_key

    def add_noisy(self, threads=6):
        """Launch noisy PUT workers; no-op if any are already alive."""
        if self.running("tenant-noisy"):
            return 0
        self._tenant_stop["tenant-noisy"].clear()
        self._tenant_pause["tenant-noisy"].clear()
        for _ in range(threads):
            t = threading.Thread(target=self._noisy_put,
                                 args=(next(self._worker_seq),), daemon=True)
            t.start(); self.threads["tenant-noisy"].append(t)
        return threads

    def add_critical(self, threads=2, seed_key=None, interval=0):
        """Launch critical GET workers; no-op if any are already alive."""
        if self.running("tenant-critical"):
            return 0
        seed_key = seed_key or self.seed_key
        if seed_key is None:
            seed_key = self.start(noisy=False, critical=False)
        self._tenant_stop["tenant-critical"].clear()
        self._tenant_pause["tenant-critical"].clear()
        for _ in range(threads):
            t = threading.Thread(target=self._critical_get,
                                 args=(seed_key, interval), daemon=True)
            t.start(); self.threads["tenant-critical"].append(t)
        return threads

    def stop_tenant(self, tenant):
        """Stop one tenant's workers; returns the count still alive after
        join timeout (kept in threads[] so callers can retry/observe)."""
        self._tenant_stop[tenant].set()
        self._tenant_pause[tenant].clear()   # paused workers must see stop
        still = []
        for t in self.threads[tenant]:
            t.join(timeout=self.timeout_s[1] + 2)
            if t.is_alive():
                still.append(t)
        self.threads[tenant] = still
        return len(still)

    def stop_noisy(self):
        return self.stop_tenant("tenant-noisy")

    def stop_critical(self):
        return self.stop_tenant("tenant-critical")

    def pause(self):
        self.paused.set()
        for e in self._tenant_pause.values():
            e.set()

    def resume(self):
        self.paused.clear()
        for e in self._tenant_pause.values():
            e.clear()

    def pause_noisy(self):      self._tenant_pause["tenant-noisy"].set()
    def resume_noisy(self):     self._tenant_pause["tenant-noisy"].clear()
    def pause_critical(self):   self._tenant_pause["tenant-critical"].set()
    def resume_critical(self):  self._tenant_pause["tenant-critical"].clear()

    def stop(self):
        """Stop all workers. Returns total lingering count."""
        self.stop_flag.set()
        self.paused.clear()
        for e in self._tenant_pause.values():
            e.clear()
        self.end_phase()
        return (self.stop_tenant("tenant-noisy")
                + self.stop_tenant("tenant-critical"))

    # ---------- tenant isolation ----------
    def verify_tenants(self):
        """Prove noisy/critical are distinct IAM principals that each get
        working S3 access in the shared namespace bucket."""
        rid = results.run_id()
        rows = []
        for label, user in (("tenant-noisy", "noisy"),
                            ("tenant-critical", "critical")):
            ak, _ = config.USERS[user]
            cli = s3util.s3(user)
            key = f"{rid}/probe/{user}.bin"
            cli.put_object(Bucket=self.bucket, Key=key, Body=b"probe")
            cli.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            rows.append({"tenant": label, "access_key": ak[:4] + "…",
                         "own_object_rw": "OK"})
        distinct = len({config.USERS["noisy"][0],
                        config.USERS["critical"][0]}) == 2
        return rows, distinct
