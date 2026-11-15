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
import json, urllib.request, urllib.error
from . import s3util, config, results

MAX_KEYS = 20000           # cap noisy objects per run (0 = unlimited)
CHUNK = 64 * 1024          # 64 KiB payload per PUT
STATUS_OK = 200
TENANTS = ("tenant-noisy", "tenant-critical")
TENANT_OP = {"tenant-noisy": "PUT", "tenant-critical": "GET"}

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


def cpu_times():
    """(busy, total) jiffies of the whole client host from /proc/stat, or
    None where that is unavailable."""
    try:
        with open("/proc/stat") as f:
            v = [int(x) for x in f.readline().split()[1:]]
        idle = v[3] + (v[4] if len(v) > 4 else 0)
        return sum(v) - idle, sum(v)
    except Exception:
        return None


def cpu_pct(start, end):
    """Client-host CPU utilisation (0-100, all cores) between two cpu_times()."""
    if not start or not end or end[1] <= start[1]:
        return None
    return round(100.0 * (end[0] - start[0]) / (end[1] - start[1]), 1)


def pctile(lats, p):
    """Linear-interpolation percentile (0-100) without numpy."""
    if not lats:
        return 0.0
    s = sorted(lats)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    return s[f] if f == k else s[f] + (s[f + 1] - s[f]) * (k - f)


class Pacer:
    """Open-loop rate limiter shared by a tenant's worker threads. Request k
    is due at t0 + k/rate; workers sleep until their slot, so offered load is
    fixed even when the client is slow. Latency is measured from the
    scheduled slot, and sends more than 50 ms late are counted as missed."""

    def __init__(self, rate):
        self.rate = rate
        self._lock = threading.Lock()
        self._seq = 0
        self._t0 = time.monotonic()
        self.sent = 0
        self.missed = 0

    def acquire(self):
        """Wait for this thread's slot; returns the scheduled monotonic time
        (None when pacing is disabled)."""
        if not self.rate:
            return None
        with self._lock:
            k = self._seq
            self._seq += 1
        due = self._t0 + k / self.rate
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        sent = time.monotonic()
        if sent > due + 0.05:
            with self._lock:
                self.missed += 1
        with self._lock:
            self.sent += 1
        return due

    def counts(self):
        with self._lock:
            return self.sent, self.missed


class LoadGen:
    def __init__(self, bucket=config.OBS_BUCKET_THROTTLE, chunk=CHUNK,
                 max_keys=MAX_KEYS, timeout_s=(3, 10), run_id=None):
        self.bucket = bucket
        self.chunk = chunk
        self.max_keys = max_keys
        self.timeout_s = timeout_s
        self.run_id = run_id or results.run_id()
        self.remote_noisy = None          # RemoteNoisy when NOISY_HOST is set
        self.events = collections.deque(maxlen=250000)   # live-chart buffer
        self.events_total = 0               # events ever recorded (chart cursor)
        self._totals = collections.Counter()
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
        self.event_sink = None          # optional per-event callback (agent)
        self.last_settle_error = None
        self._inflight = {}             # token -> (tenant, request-start ts)
        self._inflight_seq = itertools.count(1)
        self._pacers = {"tenant-noisy": None, "tenant-critical": None}
        self._phase_pacer_start = {}
        self.phases = []                    # [{name,tag,start,end,seconds}]
        self._phase = "init"
        self._phase_tag = None
        self._phase_seq = 0
        self._phase_start = None            # wall
        self._phase_start_m = None          # monotonic
        self._phase_cpu = None              # client-host CPU counters at start
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
            self._phase_cpu = cpu_times()
            self._phase_pacer_start = {
                t: (p.counts() if p is not None else None)
                for t, p in self._pacers.items()}

    def _close_phase_locked(self, wall, mono):
        if self._phase_tag is not None and self._phase_start is not None:
            entry = {"name": self._phase, "tag": self._phase_tag,
                     "start": self._phase_start, "end": wall,
                     "seconds": mono - self._phase_start_m,
                     "client_cpu_pct": cpu_pct(self._phase_cpu,
                                               cpu_times())}
            for t, p in self._pacers.items():
                if p is None:
                    continue
                before = self._phase_pacer_start.get(t)
                after = p.counts()
                if before:
                    op = TENANT_OP.get(t, t)
                    entry[f"{t} {op}_paced"] = after[0] - before[0]
                    entry[f"{t} {op}_missed"] = after[1] - before[1]
            self.phases.append(entry)

    def end_phase(self):
        """Close the open phase; returns its wall-clock end timestamp."""
        with self._lock:
            wall = time.time()
            self._close_phase_locked(wall, time.monotonic())
            self._phase, self._phase_tag = "idle", None
            self._phase_start = self._phase_start_m = None
            return wall

    @property
    def phase(self):
        with self._lock:
            return self._phase

    @property
    def _cur_tag(self):
        with self._lock:
            return self._phase_tag

    # ---------- metrics ----------
    def _emit(self, ev):
        sink = self.event_sink
        if sink is not None:
            try:
                sink(ev)
            except Exception:
                pass

    def _rec(self, tenant, op, ts, latency_ms, status, code="", retries=0,
             tag=None):
        ev = Event(ts, tenant, op, latency_ms, status, code, retries,
                   tag or self._phase or "init")
        with self._lock:
            self.events.append(ev)
            self.events_total += 1
            cls = classify(status, code)
            self._totals["ok" if cls == "ok" else
                         "throttled" if cls in THROTTLED else "errors"] += 1
            self._agg_buf[ev.phase][f"{tenant} {op}"].append(
                (ts, latency_ms, status, code, retries))
        self._emit(ev)

    def _phase_tag_for_ts(self, ts):
        """Phase tag for a request that STARTED at wall time `ts`: closed
        phases first, then the currently open phase. Remote noisy events are
        ingested after the fact, so the tag must come from the timestamp —
        not from whichever phase happens to be open when the HTTP response
        arrives."""
        with self._lock:
            entries = list(self.phases)
            if self._phase_tag is not None and self._phase_start is not None:
                entries.append({"name": self._phase, "tag": self._phase_tag,
                                "start": self._phase_start, "end": None})
            cur = self._phase_tag or "init"
        for ph in entries:
            end = ph.get("end")
            if ts >= ph["start"] and (end is None or ts < end):
                return ph["tag"]
        return cur if ts >= (entries[-1]["start"] if entries else 0) else "init"

    def ingest_event(self, tenant, op, ts, latency_ms, status, code="",
                     retries=0):
        """Record one remotely generated request into the local metrics.

        Used by RemoteNoisy: the remote agent owns the S3 call, U01 owns the
        phase timeline and the verdicts. `ts` is the remote request-start
        wall time; phase attribution is by timestamp so late-arriving events
        still land in the phase they belonged to."""
        tag = self._phase_tag_for_ts(ts)
        ev = Event(ts, tenant, op, latency_ms, status, code, retries, tag)
        with self._lock:
            self.events.append(ev)
            self.events_total += 1
            cls = classify(status, code)
            self._totals["ok" if cls == "ok" else
                         "throttled" if cls in THROTTLED else "errors"] += 1
            self._agg_buf[tag][f"{tenant} {op}"].append(
                (ts, latency_ms, status, code, retries))
        self._emit(ev)

    def attach_remote(self, remote):
        """Delegate noisy-tenant control/events to a RemoteNoisy client."""
        self.remote_noisy = remote

    def _events(self):
        with self._lock:
            return list(self.events)

    def events_since(self, cursor):
        """(new events, new cursor): events recorded after `cursor` (a value
        previously returned here, 0 for all). Lets the live chart process
        each event once instead of rescanning the whole buffer."""
        with self._lock:
            fresh = min(self.events_total - cursor, len(self.events))
            if fresh <= 0:
                return [], self.events_total
            evs = list(itertools.islice(
                self.events, len(self.events) - fresh, len(self.events)))
            return evs, self.events_total

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
                                           - self._phase_start_m,
                                "client_cpu_pct": cpu_pct(self._phase_cpu,
                                                          cpu_times())})
        out, seen = {}, collections.Counter()
        for ph in entries:
            seen[ph["name"]] += 1
            key = ph["name"] if seen[ph["name"]] == 1 \
                else f"{ph['name']}#{seen[ph['name']]}"
            groups = {}
            for grp, evs in self._agg_buf.get(ph["tag"], {}).items():
                groups[grp] = self._agg(evs, ph["seconds"], ph["end"])
            out[key] = {"_window": {"start": ph["start"], "end": ph["end"],
                                    "seconds": round(ph["seconds"], 1),
                                    "client_cpu_pct": ph.get("client_cpu_pct")},
                        **{k: v for k, v in ph.items()
                           if k.endswith("_paced") or k.endswith("_missed")},
                        **groups}
        return out

    def phase_window(self, name):
        """(start, end) of the most recent phase called `name`; end is now
        for the still-open phase."""
        entries = list(self.phases)
        with self._lock:
            if self._phase == name and self._phase_start is not None:
                entries.append({"name": name, "start": self._phase_start,
                                "end": time.time()})
        for ph in reversed(entries):
            if ph["name"] == name:
                return ph["start"], ph["end"]
        return None

    # ---------- in-flight tracking / settle ----------
    def _inflight_start(self, tenant, ts):
        tok = next(self._inflight_seq)
        with self._lock:
            self._inflight[tok] = (tenant, ts)
        return tok

    def _inflight_end(self, tok):
        with self._lock:
            self._inflight.pop(tok, None)

    def _inflight_before(self, until_ts):
        with self._lock:
            return sum(1 for _, ts in self._inflight.values() if ts < until_ts)

    def _wait_local_inflight(self, until_ts, timeout_s=30):
        deadline = time.time() + timeout_s
        while self._inflight_before(until_ts):
            if time.time() > deadline:
                raise TimeoutError("local in-flight requests did not settle")
            time.sleep(0.05)

    def settle(self, phase_end_ts, timeout_s=30):
        """Wait until every request that STARTED before `phase_end_ts` has
        been recorded, so phase_stats() cannot miss the slow tail. Local
        in-flight requests (critical in remote mode, both tenants locally)
        are awaited directly; remote noisy events go through
        RemoteNoisy.flush()."""
        try:
            self._wait_local_inflight(phase_end_ts, timeout_s=timeout_s)
            if self.remote_noisy is not None \
                    and not getattr(self.remote_noisy, "closed", False):
                self.remote_noisy.flush(phase_end_ts, timeout_s=timeout_s)
            self.last_settle_error = None
        except Exception as e:
            self.last_settle_error = str(e)
            raise

    def totals(self):
        """Running totals since start (O(1): kept by _rec, not rescanned)."""
        with self._lock:
            ok, th, err = (self._totals[k] for k in ("ok", "throttled", "errors"))
        return {"events": self.events_total, "ok": ok, "throttled": th,
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
        rid = self.run_id
        while not self._stopped("tenant-noisy"):
            if self._paused("tenant-noisy"):
                time.sleep(0.25); continue
            seq = self._next_key_seq()
            key = f"{rid}/noisy/n{seq}.bin"
            pacer = self._pacers["tenant-noisy"]
            due = pacer.acquire() if pacer is not None else None
            tag, ts = self._cur_tag, time.time()
            t0 = due if due is not None else time.monotonic()
            tok = self._inflight_start("tenant-noisy", ts)
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
            finally:
                self._inflight_end(tok)

    def _critical_get(self, key, interval=0):
        cli = s3util.s3("critical", timeout_s=self.timeout_s, max_attempts=1)
        while not self._stopped("tenant-critical"):
            if self._paused("tenant-critical"):
                time.sleep(0.25); continue
            pacer = self._pacers["tenant-critical"]
            due = pacer.acquire() if pacer is not None else None
            tag, ts = self._cur_tag, time.time()
            t0 = due if due is not None else time.monotonic()
            tok = self._inflight_start("tenant-critical", ts)
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
            finally:
                self._inflight_end(tok)
            if interval:
                time.sleep(interval)

    # ---------- control ----------
    def running(self, tenant=None):
        if tenant == "tenant-noisy" and self.remote_noisy is not None:
            return self.remote_noisy.running("tenant-noisy")
        if tenant:
            return any(t.is_alive() for t in self.threads[tenant])
        local = any(t.is_alive() for ts in self.threads.values() for t in ts)
        if self.remote_noisy is not None:
            return local or self.remote_noisy.running("tenant-noisy")
        return local

    def lingering(self, tenant=None):
        """Workers still alive — should be 0 after a successful stop."""
        if tenant == "tenant-noisy" and self.remote_noisy is not None:
            return self.remote_noisy.lingering("tenant-noisy")
        if tenant:
            return sum(1 for t in self.threads[tenant] if t.is_alive())
        local = sum(1 for ts in self.threads.values()
                    for t in ts if t.is_alive())
        if self.remote_noisy is not None:
            return local + self.remote_noisy.lingering("tenant-noisy")
        return local

    def start(self, seed_key=None, noisy=True, critical=True,
              noisy_threads=6, critical_threads=2, critical_interval=0,
              noisy_rate=None, critical_rate=None):
        """Seed one critical object if needed; launch only the requested
        tenants that are not already running. A running noisy tenant must
        never prevent critical workers from starting (and vice versa)."""
        if seed_key is None:
            seed_key = self.seed_key or f"{self.run_id}/critical-seed.bin"
            if not self.running("tenant-critical"):
                s3util.s3("critical").put_object(
                    Bucket=self.bucket, Key=seed_key, Body=self._payload)
        self.seed_key = seed_key
        self.stop_flag.clear(); self.paused.clear()
        if noisy:
            self._tenant_stop["tenant-noisy"].clear()
            self._tenant_pause["tenant-noisy"].clear()
        if critical:
            self._tenant_stop["tenant-critical"].clear()
            self._tenant_pause["tenant-critical"].clear()
        if noisy:
            self.add_noisy(noisy_threads, rate=noisy_rate)
        if critical:
            self.add_critical(critical_threads, seed_key, critical_interval,
                              rate=critical_rate)
        return seed_key

    def add_noisy(self, threads=6, rate=None):
        """Launch noisy PUT workers; no-op if any are already alive."""
        if self.remote_noisy is not None:
            return self.remote_noisy.add_noisy(threads, rate=rate)
        if self.running("tenant-noisy"):
            return 0
        self._tenant_stop["tenant-noisy"].clear()
        self._tenant_pause["tenant-noisy"].clear()
        self._pacers["tenant-noisy"] = Pacer(rate) if rate else None
        for _ in range(threads):
            t = threading.Thread(target=self._noisy_put,
                                 args=(next(self._worker_seq),), daemon=True)
            t.start(); self.threads["tenant-noisy"].append(t)
        return threads

    def add_critical(self, threads=2, seed_key=None, interval=0, rate=None):
        """Launch critical GET workers; no-op if any are already alive."""
        if self.running("tenant-critical"):
            return 0
        seed_key = seed_key or self.seed_key
        if seed_key is None:
            seed_key = self.start(noisy=False, critical=False)
        self._tenant_stop["tenant-critical"].clear()
        self._tenant_pause["tenant-critical"].clear()
        self._pacers["tenant-critical"] = Pacer(rate) if rate else None
        for _ in range(threads):
            t = threading.Thread(target=self._critical_get,
                                 args=(seed_key, interval), daemon=True)
            t.start(); self.threads["tenant-critical"].append(t)
        return threads

    def stop_tenant(self, tenant):
        """Stop one tenant's workers; returns the count still alive after
        join timeout (kept in threads[] so callers can retry/observe)."""
        if tenant == "tenant-noisy" and self.remote_noisy is not None:
            return self.remote_noisy.stop_noisy()
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

    def pause_noisy(self):
        if self.remote_noisy is not None:
            self.remote_noisy.pause_noisy(); return
        self._tenant_pause["tenant-noisy"].set()

    def resume_noisy(self):
        if self.remote_noisy is not None:
            self.remote_noisy.resume_noisy(); return
        self._tenant_pause["tenant-noisy"].clear()

    def pause_critical(self):   self._tenant_pause["tenant-critical"].set()
    def resume_critical(self):  self._tenant_pause["tenant-critical"].clear()

    def set_noisy_rate(self, rate):
        if self.remote_noisy is not None:
            self.remote_noisy.set_noisy_rate(rate); return
        self._pacers["tenant-noisy"] = Pacer(rate) if rate else None

    def set_critical_rate(self, rate):
        self._pacers["tenant-critical"] = Pacer(rate) if rate else None

    def stop(self):
        """Stop all workers. Returns total lingering count."""
        self.stop_flag.set()
        self.paused.clear()
        for e in self._tenant_pause.values():
            e.clear()
        self.end_phase()
        leftover = (self.stop_tenant("tenant-noisy")
                    + self.stop_tenant("tenant-critical"))
        try:
            self.settle(time.time())       # final pull: do not drop tail events
        except Exception as e:
            self.last_settle_error = str(e)
        if self.remote_noisy is not None:
            self.remote_noisy.close()
        return leftover

    # ---------- tenant isolation ----------
    _TENANT_LABEL = {"noisy": "tenant-noisy", "critical": "tenant-critical"}

    def verify_tenant(self, user, label=None):
        """Prove one PoC principal can write+read an object in the shared
        namespace bucket. Returns the display row."""
        label = label or self._TENANT_LABEL.get(user, user)
        ak, _ = config.USERS[user]
        cli = s3util.s3(user)
        key = f"{self.run_id}/probe/{user}.bin"
        cli.put_object(Bucket=self.bucket, Key=key, Body=b"probe")
        cli.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return {"tenant": label, "access_key": ak[:4] + "…",
                "own_object_rw": "OK"}

    def verify_tenants(self):
        """Prove noisy/critical are distinct IAM principals that each get
        working S3 access in the shared namespace bucket."""
        rows = [self.verify_tenant("noisy", "tenant-noisy"),
                self.verify_tenant("critical", "tenant-critical")]
        distinct = len({config.USERS["noisy"][0],
                        config.USERS["critical"][0]}) == 2
        return rows, distinct

    def noisy_burst(self, bucket, prefix, seconds=20, threads=4, size=1024,
                    keys_per_thread=50):
        """Short tenant-noisy PUT burst under one key prefix — local or on
        the remote noisy host when NOISY_HOST is configured."""
        if self.remote_noisy is not None:
            return self.remote_noisy.burst(bucket, prefix, seconds=seconds,
                                           threads=threads, size=size,
                                           keys_per_thread=keys_per_thread)
        return put_burst("noisy", bucket, prefix, seconds=seconds,
                         threads=threads, size=size,
                         keys_per_thread=keys_per_thread)


def put_burst(user, bucket, prefix, seconds=20, threads=4, size=1024,
              keys_per_thread=50):
    """Short PUT burst by one PoC user under one key prefix, client retries
    off. Used to compare a rule-covered prefix with a control prefix.
    Returns the same aggregate as phase_stats() plus ok_tps / seconds."""
    events, lock, stop = [], threading.Lock(), threading.Event()
    body = b"x" * size
    prefix = prefix.rstrip("/") + "/"

    def worker(wid):
        cli = s3util.s3(user, max_attempts=1)
        i = 0
        while not stop.is_set():
            i += 1
            ts, t0 = time.time(), time.monotonic()
            try:
                r = cli.put_object(Bucket=bucket, Body=body,
                                   Key=f"{prefix}k{wid}-{i % keys_per_thread}.bin")
                ev = (ts, (time.monotonic() - t0) * 1000, STATUS_OK, "",
                      _ok_retries(r))
            except Exception as e:
                ev = (ts, (time.monotonic() - t0) * 1000, _status(e), _code(e),
                      _retries(e))
                time.sleep(0.05)
            with lock:
                events.append(ev)

    ths = [threading.Thread(target=worker, args=(w,), daemon=True)
           for w in range(threads)]
    t0 = time.monotonic()
    for t in ths:
        t.start()
    stop.wait(seconds)
    stop.set()
    for t in ths:
        t.join(timeout=15)
    span = time.monotonic() - t0
    with lock:
        agg = LoadGen._agg(list(events), span)
    agg.update(seconds=round(span, 1), ok_tps=agg["ok"] / max(1e-9, span),
               prefix=prefix, user=user)
    return agg


class RemoteNoisyError(RuntimeError):
    pass


class RemoteNoisy:
    """U01-side client for poc/noisy_agent.py on the noisy PUT host.

    The notebook keeps using LoadGen for phases, critical GETs, verdicts and
    the chart; only tenant-noisy control and event ingestion are delegated
    here. Events are pulled continuously and re-ingested into the local
    LoadGen by request-start timestamp, so phase_stats()/evaluate() need no
    remote-specific logic."""

    def __init__(self, lg, base_url, token="", bucket=config.OBS_BUCKET_THROTTLE,
                 run_id=None, threads=6, max_keys=MAX_KEYS, chunk=CHUNK,
                 timeout_s=(3, 10), poll_s=1.0):
        self.lg = lg
        self.base_url = base_url if "://" in base_url else f"http://{base_url}"
        self.token = token
        self.bucket = bucket
        self.run_id = run_id or lg.run_id
        self.threads = threads
        self.max_keys = max_keys
        self.chunk = chunk
        self.timeout_s = timeout_s
        self.poll_s = poll_s
        self.cursor = 0
        self.ingested = 0
        self.events_pulled = 0
        self.poll_errors = 0
        self.max_gap_s = 0.0
        self.last_error = None
        self.last_error_ts = None
        self.last_ok = None
        self._last_poll_ok = None
        self._bad_since = None
        self.outages = []
        self.closed = False
        self._stop = threading.Event()
        self._poll_lock = threading.Lock()
        lg.attach_remote(self)
        # Start AFTER the agent's current event cursor so a new client does
        # not replay the previous run's history into the chart/phase stats.
        try:
            st = self.status(timeout=5)
            self.cursor = int(st.get("event_cursor", st.get("events_total", 0)) or 0)
            self.last_ok = time.time()
        except Exception as e:
            self.last_error = str(e)
            self.last_error_ts = time.time()
        self._poller = threading.Thread(target=self._poll_loop, daemon=True)
        self._poller.start()

    # ---------- HTTP ----------
    def _headers(self, ts=None):
        return {"X-POC-Token": self.token,
                "X-POC-Time": f"{(ts if ts is not None else time.time()):.6f}",
                "Content-Type": "application/json"}

    def _request(self, method, path, body=None, timeout=10, ts=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base_url + path, data=data,
                                     headers=self._headers(ts), method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            detail = e.read()[:300]
            raise RemoteNoisyError(f"{method} {path} -> HTTP {e.code}: {detail!r}")
        except Exception as e:
            raise RemoteNoisyError(f"{method} {path} failed: {e}")

    def health(self, timeout=10):
        return self._request("GET", "/health", timeout=timeout)

    def measure_clock(self, timeout=10):
        """Round-trip clock measurement: offset = agent_time - midpoint of
        the client send/receive wall times, keeping the RTT."""
        t_send = time.time()
        h = self._request("GET", "/health", timeout=timeout, ts=t_send)
        t_recv = time.time()
        agent_time = h.get("agent_time")
        if agent_time is None:
            return {"offset_ms": None, "rtt_ms": (t_recv - t_send) * 1000,
                    "health": h}
        mid = (t_send + t_recv) / 2.0
        return {"offset_ms": (agent_time - mid) * 1000,
                "rtt_ms": (t_recv - t_send) * 1000,
                "agent_time": agent_time, "health": h}

    def check_clock(self, limit_ms=50.0, timeout=10):
        """Return (offset_ms, health) or raise when the U01/U02 clocks are
        not synchronized closely enough for timestamp phase attribution."""
        m = self.measure_clock(timeout=timeout)
        off = m.get("offset_ms")
        if off is None or abs(off) > limit_ms:
            raise RemoteNoisyError(
                f"U01/U02 clock offset {off} ms exceeds {limit_ms:.0f} ms — "
                "sync chrony on both hosts before running notebook 02")
        return off, m["health"]

    def recheck_clock(self, limit_ms=50.0, timeout=10):
        """Measure at a phase boundary; returns the measurement dict and
        never raises — the caller decides FAIL/INCONCLUSIVE."""
        return self.measure_clock(timeout=timeout)

    def status(self, timeout=5):
        return self._request("GET", "/status", timeout=timeout)

    # ---------- preflight / tenant proof ----------
    def preflight(self, run_id=None):
        return self._request("POST", "/preflight",
                             {"run_id": run_id or self.run_id}, timeout=30)

    def verify_tenants(self):
        """Noisy proof comes from the remote agent; critical proof stays local."""
        pf = self.preflight()
        row = pf.get("row") or {"tenant": "tenant-noisy", "access_key": "…",
                                "own_object_rw": "FAIL",
                                "error": pf.get("error", "no row")}
        crit = self.lg.verify_tenant("critical", "tenant-critical")
        distinct = len({config.USERS["noisy"][0],
                        config.USERS["critical"][0]}) == 2
        ok = bool(pf.get("ok")) and row.get("own_object_rw") == "OK" \
            and crit.get("own_object_rw") == "OK" and distinct
        return [row, crit], ok

    # ---------- noisy controls (same shape as LoadGen) ----------
    def add_noisy(self, threads=6, rate=None):
        body = {"threads": int(threads), "run_id": self.run_id,
                "bucket": self.bucket, "max_keys": self.max_keys,
                "chunk": self.chunk, "timeout_s": list(self.timeout_s),
                "rate": rate}
        res = self._request("POST", "/noisy/start", body, timeout=15)
        return int(res.get("launched", 0))

    def set_noisy_rate(self, rate):
        self._request("POST", "/noisy/rate", {"rate": rate}, timeout=10)

    def pause_noisy(self):
        self._request("POST", "/noisy/pause", {}, timeout=10)

    def resume_noisy(self):
        self._request("POST", "/noisy/resume", {}, timeout=10)

    def stop_noisy(self):
        res = self._request("POST", "/noisy/stop", {}, timeout=30)
        return int(res.get("lingering", 0))

    def burst(self, bucket, prefix, seconds=20, threads=4, size=1024,
              keys_per_thread=50):
        return self._request("POST", "/burst", {
            "bucket": bucket, "prefix": prefix, "seconds": seconds,
            "threads": threads, "size": size,
            "keys_per_thread": keys_per_thread}, timeout=float(seconds) + 30)

    def running(self, tenant="tenant-noisy"):
        if tenant == "tenant-critical":
            return self.lg.running("tenant-critical")
        if tenant not in (None, "tenant-noisy"):
            return False
        return bool(self.status().get("noisy", {}).get("running"))

    def lingering(self, tenant="tenant-noisy"):
        if tenant == "tenant-critical":
            return self.lg.lingering("tenant-critical")
        if tenant not in (None, "tenant-noisy"):
            return 0
        return int(self.status().get("noisy", {}).get("lingering", 0))

    # ---------- event pull ----------
    def poll(self):
        # The background poller and settle()/flush() share the cursor; without
        # this lock two pulls can request the same `since` and double-ingest.
        with self._poll_lock:
            res = self._request("GET", f"/events?since={self.cursor}",
                                timeout=15)
            n = 0
            for ev in res.get("events", []):
                if ev.get("tenant") != "tenant-noisy" or ev.get("op") != "PUT":
                    continue
                self.lg.ingest_event("tenant-noisy", "PUT", ev["ts"],
                                     ev.get("lat_ms", 0), ev.get("status", 599),
                                     ev.get("code", ""), ev.get("retries", 0))
                n += 1
            self.cursor = int(res.get("cursor", self.cursor))
            self.ingested += n
            self.events_pulled += len(res.get("events", []))
            self._last_inflight = res.get("inflight")
            self._last_oldest = res.get("oldest_inflight_ts")
            now = time.time()
            if self._last_poll_ok is not None:
                self.max_gap_s = max(self.max_gap_s, now - self._last_poll_ok)
            self._last_poll_ok = now
            self.last_ok = now
            if self._bad_since is not None:
                self.outages.append((self._bad_since, now))
                self._bad_since = None
            self.last_error = None
            return n

    def _poll_loop(self):
        while not self._stop.is_set():
            try:
                self.poll()
                self._stop.wait(self.poll_s)
            except Exception as e:
                self.poll_errors += 1
                self.last_error = str(e)
                self.last_error_ts = time.time()
                if self._bad_since is None:
                    self._bad_since = self.last_error_ts
                self._stop.wait(min(10.0, self.poll_s * 5))

    def contact_ok(self, max_age_s=5.0):
        return self.last_ok is not None and (time.time() - self.last_ok) <= max_age_s

    def max_outage_between(self, t0, t1):
        """Longest interval inside [t0,t1] with no successful event pull."""
        spans = list(self.outages)
        if self._bad_since is not None:
            spans.append((self._bad_since, time.time()))
        best = 0.0
        for a, b in spans:
            lo, hi = max(a, t0), min(b, t1)
            if hi > lo:
                best = max(best, hi - lo)
        if self.last_ok is not None and self.last_ok < t1:
            # still failing right now: measure from last success to t1
            best = max(best, t1 - max(self.last_ok, t0))
        return best

    def stats(self):
        now = time.time()
        return {"events_pulled": self.events_pulled,
                "events_ingested": self.ingested,
                "poll_errors": self.poll_errors,
                "max_gap_s": round(self.max_gap_s, 3),
                "last_error": self.last_error,
                "last_error_ts": self.last_error_ts,
                "last_ok": self.last_ok,
                "contact_age_s": round(now - self.last_ok, 3)
                if self.last_ok else None,
                "contact_ok": self.contact_ok()}

    def flush(self, until_ts, timeout_s=15):
        """Pull until every request that started before until_ts has been
        recorded on U01 (agent reports oldest in-flight start time). Two
        consecutive clean polls are required so an event appended between
        the status snapshot and the cursor pull cannot slip through."""
        deadline = time.time() + timeout_s
        clean = 0
        while True:
            self.poll()
            oldest = self._last_oldest
            if oldest is None or oldest >= until_ts:
                clean += 1
                if clean >= 2:
                    return
            else:
                clean = 0
            if time.time() > deadline:
                raise RemoteNoisyError(
                    f"remote noisy flush timed out: oldest in-flight "
                    f"{oldest} < {until_ts}")
            time.sleep(0.1)

    def close(self):
        self.closed = True
        self._stop.set()
        self._poller.join(timeout=2)
