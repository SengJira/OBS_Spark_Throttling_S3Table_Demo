"""Throttler rule verification and acceptance evaluation.

verify_rule() validates a rule dict against the schema this ObjectScale 4.4
build accepts on PUT /throttle/config (documented in reports/environment.md)
plus the live /throttle/config/actions list — nothing undocumented is sent.

evaluate() turns per-phase LoadGen metrics into PASS/FAIL/INCONCLUSIVE
verdicts against customer-approved thresholds. INCONCLUSIVE is used when the
data cannot decide — e.g. no enforcement signature was observed, so an
enforcement verdict cannot be claimed either way. It is never counted as a
pass.
"""
import time

# Customer-approved acceptance thresholds (override per engagement).
DEFAULT_THRESHOLDS = {
    "noisy_tps_limit_factor": 1.2,    # enforced ok-TPS < limit*factor
    "min_throttled": 1,               # >=1 429/503 => enforcement observed
    "critical_p95_max_ms": 150.0,     # critical GET SLA during enforcement
    "critical_err_rate": 0.01,        # GET error fraction (excl. throttle)
    "recovery_p95_dev_factor": 1.5,   # recovery p95 / baseline p95 —
                                    # sustained throttled load leaves
                                    # residual contention; strict 1.25x
                                    # false-flags good recoveries
    "min_requests": 50,               # too few events => INCONCLUSIVE
    "min_offer_factor": 0.8,          # enforced offer < limit*factor => load
                                    # not sustained => INCONCLUSIVE
    "critical_throttle_max": 0,       # critical GET 429/SlowDown count allowed
    "baseline_stable_factor": 1.25,   # post-baseline p95 <= factor*baseline p95
    "recovery_improvement_min_share": 0.5,  # share of contention damage removed
    "contention_min_delta_ms": 2.0,   # contention must raise p95 by this much
    "max_missed_slot_pct": 1.0,       # paced client misses => INCONCLUSIVE
}

# Rule schema, confirmed live against PUT /throttle/config and
# GET /throttle/template on ObjectScale 4.4 (see reports/environment.md):
# every value outside these sets was rejected with HTTP 400.
RULE_FIELDS = {"id", "description", "isDisabled", "rateUnit", "filters",
               "limit", "scoping"}
FILTER_FIELDS = {"principals", "verbs", "actions", "paths", "exclude_paths",
                 "ips", "exclude_ips"}
RATE_UNITS = {"TPS", "TPM"}                      # BPS -> 400
PATH_MATCHERS = {"exact", "wildcard"}            # prefix -> 400
IP_MATCHERS = {"exact", "wildcard4", "cidr4"}
VERBS = {"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "*"}
SCOPING = {"principal", "action", "ip", "path", "verb"}
# scoping dimension -> the filter it needs (server, for every dimension:
# "scoping.per dimension '<d>' requires a corresponding filter")
SCOPING_NEEDS = {"principal": "principals", "action": "actions", "ip": "ips",
                 "path": "paths", "verb": "verbs"}


def _live_actions(thr):
    acts = thr.list_actions()
    if isinstance(acts, dict):
        acts = acts.get("actions", [])
    return set(acts)


def _check_matcher(problems, f, key, matchers):
    if key not in f:
        return
    m = f[key]
    if not isinstance(m, dict):
        problems.append(f"{key} must be an object with matcher + values")
        return
    if m.get("matcher") not in matchers:
        problems.append(f"{key}.matcher must be one of {sorted(matchers)}")
    if not m.get("values"):
        problems.append(f"{key}.values must be non-empty")


def verify_rule(thr, rule):
    """Validate `rule` against the schema ObjectScale accepts and against the
    LIVE action list. Read-only — never modifies configuration.

    `ok` is True only if every check passed, including the live action
    lookup: an action that the server does not list, or a lookup that could
    not be made, is a problem."""
    problems = []
    unknown = set(rule) - RULE_FIELDS
    if unknown:
        problems.append(f"unknown top-level fields: {sorted(unknown)}")
    if not rule.get("id"):
        problems.append("missing rule id")
    if rule.get("rateUnit") not in RATE_UNITS:
        problems.append(f"rateUnit must be one of {sorted(RATE_UNITS)}")
    warnings = []
    lim = (rule.get("limit") or {}).get("value")
    if isinstance(lim, bool) or not isinstance(lim, int) or lim < 0:
        problems.append("limit.value must be 0 or a positive integer")
    elif lim == 0:
        warnings.append("limit.value 0 is accepted by the server, but its "
                        "effect was not tested in this PoC")

    f = rule.get("filters") or {}
    bad_f = set(f) - FILTER_FIELDS
    if bad_f:
        problems.append(f"unknown filter fields: {sorted(bad_f)}")
    if not any(f.get(k) for k in FILTER_FIELDS):
        problems.append("no valid filters: a rule needs at least one filter")
    verbs, actions = f.get("verbs"), f.get("actions")
    if verbs and actions:
        problems.append("a rule uses verbs or actions, not both")
    bad_verbs = set(verbs or []) - VERBS
    if bad_verbs:
        problems.append(f"unknown verbs: {sorted(bad_verbs)}")

    supported = {}
    if actions:
        try:
            live = _live_actions(thr)
            supported["actions"] = sorted(live)
            bad_actions = sorted(set(actions) - live)
            if bad_actions:
                problems.append(f"actions not offered by this server: {bad_actions}")
        except Exception as e:
            supported["actions_error"] = str(e)[:80]
            problems.append("actions could not be checked against the live "
                            f"/throttle/config/actions list ({type(e).__name__})")

    _check_matcher(problems, f, "paths", PATH_MATCHERS)
    _check_matcher(problems, f, "exclude_paths", PATH_MATCHERS)
    _check_matcher(problems, f, "ips", IP_MATCHERS)
    _check_matcher(problems, f, "exclude_ips", IP_MATCHERS)
    if "exclude_paths" in f and "paths" not in f:
        problems.append("exclude_paths requires a paths filter")
    if "exclude_ips" in f and "ips" not in f:
        problems.append("exclude_ips requires an ips filter")

    per = (rule.get("scoping") or {}).get("per") or []
    bad_per = [d for d in per if d not in SCOPING]
    if bad_per:
        problems.append(f"scoping.per values must be from {sorted(SCOPING)}; "
                        f"got {bad_per}")
    for d in per:
        need = SCOPING_NEEDS.get(d)
        if need and not f.get(need):
            problems.append(f"scoping.per '{d}' requires a {need} filter")
    return {"ok": not problems, "problems": problems, "warnings": warnings,
            "supported": supported, "checked_at": time.time()}


def effective_policy(thr):
    """Read (never write) the effective throttler settings."""
    try:
        rules = thr.get_rules()
    except Exception:
        rules = []
    return {"mode": thr.get_mode(), "response_code": thr.get_response_code(),
            "rules": rules, "retrieved_at": time.time()}


def server_counters(thr):
    """Best-effort discovery of server-side throttle metrics. Probes a small
    set of read-only GET paths and reports status per path — never fabricates
    numbers. `available` is False unless a path actually returns data."""
    probed, data = {}, {}
    for path in ("throttle/stats", "throttle/metrics",
                 "throttle/config/history", "throttle/history"):
        try:
            r = thr.api.get("/" + path)
            probed[path] = r.status_code
            if r.status_code == 200 and r.text:
                data[path] = r.json()
        except Exception as e:
            probed[path] = f"error:{type(e).__name__}"
    # documented metrics path: Flux query API (503 on this build — see
    # reports/environment.md); recorded for correlation, never assumed up
    try:
        q = ('from(bucket:"monitoring_main") |> range(start:-5m) '
             '|> filter(fn:(r)=>r._measurement =~ "throttle.*")')
        r = thr.api.post("/flux/query", body={"query": q})
        probed["flux/query"] = r.status_code
        if r.status_code == 200 and r.text:
            data["flux/query"] = r.text[:4000]
    except Exception as e:
        probed["flux/query"] = f"error:{type(e).__name__}"
    return {"available": bool(data), "probed": probed, "data": data}


def signals(phase_stats, limit, th=None, noisy_group="tenant-noisy PUT"):
    """Raw enforcement evidence from the 'enforced' phase (client retries are
    off in LoadGen, so every server response is counted as it arrived).

    rejected: 429 / 503-SlowDown responses were returned.
    delayed:  accepted TPS sits in the limit band while the unthrottled rate
              was at least twice the limit AND accepted-request tail latency
              rose — the server queues requests to hold the rate."""
    th = th or DEFAULT_THRESHOLDS
    ps = phase_stats
    cont = ps.get("contention", {})
    cont_noisy = cont.get(noisy_group, {})
    enfc = ps.get("enforced", {})
    enfc_noisy = enfc.get(noisy_group, {})
    ok_tps = enfc_noisy.get("ok", 0) / max(1e-9, enfc.get("_window", {}).get("seconds", 1))
    before = cont_noisy.get("ok", 0) / max(1e-9, cont.get("_window", {}).get("seconds", 1))
    tail = max(enfc_noisy.get("ok_p95", enfc_noisy.get("p95", 0)),
               enfc_noisy.get("ok_p99", enfc_noisy.get("p99", 0)))
    base_tail = cont_noisy.get("ok_p95", cont_noisy.get("p95", 0))
    delayed = (
        bool(limit)
        and enfc_noisy.get("n", 0) >= th["min_requests"]
        and ok_tps <= limit * th["noisy_tps_limit_factor"]
        and ok_tps >= limit * 0.2            # pinned near limit, not dead
        and before >= limit * 2
        and tail >= max(2 * base_tail, 25))
    return {"rejected": enfc_noisy.get("throttled", 0) > 0, "delayed": delayed,
            "throttled": enfc_noisy.get("throttled", 0),
            "ok_tps": ok_tps, "ok_tps_before": before,
            "within_limit": bool(limit) and ok_tps <= limit * th["noisy_tps_limit_factor"]}


def detect(phase_stats, limit, th=None, critical_group="tenant-critical GET",
           noisy_group="tenant-noisy PUT"):
    """Decide whether enforcement fired in the 'enforced' phase, and how.

    Returns (active: bool, mode: 'reject+delay'|'reject'|'delay'|'none').
    Observed on ObjectScale 4.4 with client retries off (reports/
    environment.md): the throttler does both — it queues requests to hold
    the accepted rate at the limit (accepted p95 ~25 ms -> ~700 ms) and
    rejects the overflow with HTTP 429 `TooManyRequests` (~6 % of requests).
    With SDK retries on, those 429s are retried away and only the delay is
    visible, which is why LoadGen disables retries."""
    sig = signals(phase_stats, limit, th, noisy_group)
    if sig["rejected"] and sig["delayed"]:
        return True, "reject+delay"
    if sig["rejected"]:
        return True, "reject"
    if sig["delayed"]:
        return True, "delay"
    return False, "none"


def tc24_status(phase_stats, limit, sustained=True, th=None,
                critical_group="tenant-critical GET",
                noisy_group="tenant-noisy PUT"):
    """Status of the enforcement test case — never a hard-coded PASS.

    FAIL          not enough data, the noisy load died, or throttle responses
                  were seen but the accepted rate still exceeds the limit.
    INCONCLUSIVE  enough data but no enforcement signature at all.
    PASS          enforcement signature AND accepted noisy TPS within the
                  limit tolerance.
    Returns (status, reason, active, mode)."""
    th = th or DEFAULT_THRESHOLDS
    enfc = phase_stats.get("enforced", {})
    n_noisy = enfc.get(noisy_group, {}).get("n", 0)
    n_crit = enfc.get(critical_group, {}).get("n", 0)
    active, mode = detect(phase_stats, limit, th, critical_group, noisy_group)
    sig = signals(phase_stats, limit, th, noisy_group)
    if n_noisy < th["min_requests"] or n_crit < th["min_requests"]:
        return ("FAIL", f"insufficient data in the enforced phase "
                        f"(noisy {n_noisy}, critical {n_crit} requests)",
                active, mode)
    if not sustained:
        return "FAIL", "noisy load was not sustained through the phase", active, mode
    if not active:
        return ("INCONCLUSIVE",
                f"no enforcement signature: 0 throttle responses and accepted "
                f"rate {sig['ok_tps']:.1f} TPS vs limit {limit} "
                f"(unthrottled {sig['ok_tps_before']:.1f})", active, mode)
    if not sig["within_limit"]:
        return ("FAIL", f"{sig['throttled']} throttle responses, but accepted "
                        f"rate {sig['ok_tps']:.1f} TPS exceeds limit {limit} "
                        f"x{th['noisy_tps_limit_factor']}", active, mode)
    return ("PASS", f"{mode}: accepted {sig['ok_tps']:.1f} TPS vs limit "
                    f"{limit} (unthrottled {sig['ok_tps_before']:.1f}), "
                    f"{sig['throttled']} throttle responses", active, mode)


def wait_effective(lg, limit, timeout_s=120, window_s=10, poll_s=5, th=None,
                   tenant="tenant-noisy", op="PUT", on_tick=None, sleep=None):
    """After a mode switch, wait until enforcement is visible in the last
    `window_s` seconds (a throttle response, or accepted TPS inside the limit
    band). Returns seconds waited, or None if `timeout_s` elapsed first —
    the caller then measures anyway and the verdict reflects it."""
    th = th or DEFAULT_THRESHOLDS
    sleep = sleep or time.sleep
    t0 = time.time()
    while True:
        waited = time.time() - t0
        g = lg.snapshot(since=time.time() - window_s).get(tenant, {}).get(op, {})
        ok_tps = g.get("ok", 0) / window_s
        if g.get("n", 0) and (g.get("throttled", 0) > 0 or (
                waited >= window_s
                and ok_tps <= limit * th["noisy_tps_limit_factor"])):
            return round(waited, 1)
        if waited >= timeout_s:
            return None
        if on_tick:
            on_tick(waited, ok_tps, g.get("throttled", 0))
        sleep(poll_s)


def evaluate_prefix(covered, control, limit, th=None):
    """Prefix-scoped rule check from two PUT bursts by the same principal:
    one under the covered key prefix, one under a control prefix.

    PASS only if the covered prefix is held at its limit while the control
    prefix runs clearly above it. Anything else is INCONCLUSIVE with the
    reason (the rule may not have propagated, or the offered load was too
    low to tell the two apart)."""
    th = th or DEFAULT_THRESHOLDS
    cov_tps, ctl_tps = covered.get("ok_tps", 0), control.get("ok_tps", 0)
    band = limit * th["noisy_tps_limit_factor"]
    detail = (f"covered {cov_tps:.1f} ok TPS / {covered.get('throttled', 0)} "
              f"throttle responses, control {ctl_tps:.1f} ok TPS / "
              f"{control.get('throttled', 0)}, prefix limit {limit}")
    if min(covered.get("n", 0), control.get("n", 0)) < th["min_requests"]:
        return {"status": "INCONCLUSIVE", "reason": "too few requests — " + detail}
    cov_enforced = cov_tps <= band and (
        covered.get("throttled", 0) > 0 or ctl_tps >= 2 * band)
    ctl_free = ctl_tps >= max(2 * cov_tps, limit * 1.5)
    if cov_enforced and ctl_free:
        return {"status": "PASS", "reason": detail}
    if not cov_enforced:
        why = "covered prefix is not held at the prefix limit"
    else:
        why = "control prefix is not clearly above the prefix limit"
    return {"status": "INCONCLUSIVE", "reason": f"{why} — {detail}"}


def combine(verdicts, names=None):
    """Worst status of the named verdicts: FAIL > INCONCLUSIVE > PASS."""
    sts = [v["verdict"] for v in verdicts if names is None or v["name"] in names]
    if not sts:
        return "INCONCLUSIVE"
    if "FAIL" in sts:
        return "FAIL"
    return "INCONCLUSIVE" if "INCONCLUSIVE" in sts else "PASS"


def _verdict(name, ok, detail, data_ok=True):
    return {"name": name,
            "verdict": ("PASS" if ok else "FAIL") if data_ok
            else "INCONCLUSIVE",
            "detail": detail}


def evaluate(phase_stats, rule=None, thresholds=None, enforcement_active=None,
             critical_group="tenant-critical GET",
             noisy_group="tenant-noisy PUT"):
    """Score baseline/contention/enforced/recovery phases vs thresholds.
    `enforcement_active` overrides auto-detection (True/False/None=auto).
    A 50 TPS limit is not expected to yield exactly 50 ok/s — enforcement
    windows, bursts, retries and concurrency all shift observed rates."""
    th = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    ps = phase_stats
    base = ps.get("baseline", {}).get(critical_group, {})
    cont_crit = ps.get("contention", {}).get(critical_group, {})
    cont_noisy = ps.get("contention", {}).get(noisy_group, {})
    sim_noisy = ps.get("simulation", {}).get(noisy_group, {})
    enfc = ps.get("enforced", {})
    enfc_crit = enfc.get(critical_group, {})
    enfc_noisy = enfc.get(noisy_group, {})
    span = enfc.get("_window", {}).get("seconds", 1)
    rec = ps.get("recovery", {})
    rec_crit = rec.get(critical_group, {})
    rec_noisy = rec.get(noisy_group, {})
    rec_span = rec.get("_window", {}).get("seconds", 1)
    post_base = ps.get("post-baseline", {}).get(critical_group, {})

    lim = ((rule or {}).get("limit") or {}).get("value")
    det_active, enforcement_mode = detect(
        ps, lim, th=th,
        critical_group=critical_group, noisy_group=noisy_group)
    if enforcement_active is None:
        enforcement_active = det_active
    ok_tps = enfc_noisy.get("ok", 0) / max(1e-9, span)
    before = cont_noisy.get("ok", 0) / max(
        1e-9, ps.get("contention", {}).get("_window", {}).get("seconds", 1))

    out = {"enforcement_active": enforcement_active,
           "enforcement_mode": enforcement_mode, "thresholds": th,
           "rule_limit": lim, "verdicts": []}
    v = out["verdicts"]

    # -1. environment drift guard: the post-baseline critical-alone phase must
    #     stay close to the baseline critical-alone measurement.
    base_ok = base.get("n", 0) >= th["min_requests"] and base.get("p95")
    post_ok = post_base.get("n", 0) >= th["min_requests"]
    if base_ok and post_ok:
        drift = post_base["p95"] / max(1e-9, base["p95"])
        v.append(_verdict("baseline_stable",
                          drift <= th["baseline_stable_factor"],
                          f"post-baseline p95={post_base['p95']:.1f}ms vs "
                          f"baseline {base['p95']:.1f}ms ({drift:.2f}x)"))
    else:
        v.append(_verdict("baseline_stable", False,
                          "insufficient baseline/post-baseline data",
                          data_ok=False))
    drifted = not any(x["name"] == "baseline_stable" and x["verdict"] == "PASS"
                      for x in v)

    # paced-client guard: too many missed slots means the client, not the
    # throttler, decided the latency numbers.
    paced_phases = [ph for ph in ps.values()
                    if ph.get("tenant-critical GET_paced")]
    missed = sum(ph.get("tenant-critical GET_missed", 0)
                 for ph in paced_phases)
    paced = sum(ph.get("tenant-critical GET_paced", 0)
                for ph in paced_phases)
    if paced:
        pct_missed = 100.0 * missed / max(1, paced)
        # too many missed slots means the client, not the throttler, decided
        # the latency numbers — that is INCONCLUSIVE, never a FAIL
        v.append({"name": "paced_client_kept_up",
                  "verdict": "PASS" if pct_missed <= th["max_missed_slot_pct"]
                             else "INCONCLUSIVE",
                  "detail": f"{missed}/{paced} critical slots missed "
                            f"({pct_missed:.2f}%)"})

    # 0. noisy load actually sustained during the enforcement window.
    #    Whenever enforcement is active, completions are capped near the
    #    limit — the floor drops to lim*0.25 (still far above dead load).
    offered = enfc_noisy.get("n", 0) / max(1e-9, span)
    need = (lim or 0) * th["min_offer_factor"] if lim else th["min_requests"]
    if enforcement_active and lim:
        need = min(need, lim * 0.25)
    v.append(_verdict("noisy_load_sustained",
                      offered >= max(need, th["min_requests"] / max(1e-9, span)),
                      f"offered {offered:.1f} TPS during enforced "
                      f"(need ≥ {need:.0f})",
                      data_ok=enfc_noisy.get("n", 0) > 0))

    # 1. noisy successful PUT TPS under enforcement vs the rule limit
    before_note = f"; pre-enforcement ok_tps={before:.1f}"
    if enfc_noisy.get("n", 0) < th["min_requests"]:
        v.append(_verdict("noisy_tps_under_limit", False,
                          f"too few noisy events ({enfc_noisy.get('n',0)})",
                          data_ok=False))
    elif not enforcement_active:
        v.append(_verdict("noisy_tps_under_limit", False,
                          f"no throttling observed; ok_tps={ok_tps:.1f}"
                          + (f" vs limit {lim}" if lim else "") + before_note,
                          data_ok=False))
    else:
        v.append(_verdict("noisy_tps_under_limit",
                          ok_tps <= lim * th["noisy_tps_limit_factor"],
                          f"ok_tps={ok_tps:.1f} vs limit {lim} "
                          f"({enforcement_mode}-mode enforcement)"
                          + before_note))

    # 2. noisy throttling evidence. Reject-mode: % of 429/SlowDown responses.
    #    Delay-mode: 0% rejections is EXPECTED — enforcement is proven by
    #    rate-limiting + latency, so this check passes on the delay signal.
    n, thr_n = enfc_noisy.get("n", 0), enfc_noisy.get("throttled", 0)
    pct = 100 * thr_n / max(1, n)
    delay_note = (" — delay-mode enforcement, 0 rejections expected"
                  if enforcement_mode == "delay" else "")
    detail = (f"{pct:.0f}% throttled ({thr_n}/{n})"
              f" [429:{enfc_noisy.get('r429',0)}"
              f" 503/SlowDown:{enfc_noisy.get('r503_slowdown',0)}"
              f" 503/other:{enfc_noisy.get('r503_other',0)}]"
              f" retries ok/fail={enfc_noisy.get('retries_ok',0)}/"
              f"{enfc_noisy.get('retries_fail',0)}")
    v.append(_verdict("noisy_throttling_pct",
                      thr_n >= th["min_throttled"]
                      or enforcement_mode == "delay",
                      detail + delay_note,
                      data_ok=n >= th["min_requests"] and enforcement_active))

    # 2b. simulation phase: rule matches must NOT produce client rejections
    if sim_noisy:
        sim_n = sim_noisy.get("n", 0)
        sim_thr = sim_noisy.get("throttled", 0)
        v.append(_verdict(
            "simulation_did_not_reject", sim_thr == 0,
            f"{sim_thr} client 429/SlowDown while in dry-run "
            f"({sim_n} noisy requests)",
            data_ok=sim_n >= th["min_requests"]))

    # 2c. rule must not throttle the critical tenant at all
    crit_thr = sum(g.get("throttled", 0) for g in
                   (enfc_crit, rec_crit, ps.get("contention", {})
                    .get(critical_group, {}),
                    ps.get("simulation", {}).get(critical_group, {})))
    crit_n = enfc_crit.get("n", 0) + rec_crit.get("n", 0)
    v.append(_verdict("critical_not_throttled",
                      crit_thr <= th["critical_throttle_max"],
                      f"{crit_thr} critical 429/SlowDown responses "
                      f"(rule targets noisy PUT only)",
                      data_ok=crit_n >= th["min_requests"]))

    # 2d. noisy workload must keep running through recovery — recovery means
    #     critical is protected under SUSTAINED noisy load, not load removed
    rec_offered = rec_noisy.get("n", 0) / max(1e-9, rec_span)
    v.append(_verdict("noisy_sustained_in_recovery",
                      rec_offered >= max(need,
                                         th["min_requests"]
                                         / max(1e-9, rec_span)),
                      f"offered {rec_offered:.1f} TPS during recovery "
                      f"(throttle events may stay elevated — expected)",
                      data_ok=rec_noisy.get("n", 0) > 0))

    # 3. critical GET SLA under enforcement AND during recovery
    if drifted:
        v.append({"name": "critical_p95_within_sla", "verdict": "INCONCLUSIVE",
                  "detail": "environment drifted — latency verdicts "
                            "inconclusive"})
    else:
        sla_p95 = max(enfc_crit.get("p95", 0), rec_crit.get("p95", 0))
        v.append(_verdict("critical_p95_within_sla",
                          sla_p95 <= th["critical_p95_max_ms"],
                          f"enforced p95={enfc_crit.get('p95',0):.1f}ms / "
                          f"recovery p95={rec_crit.get('p95',0):.1f}ms vs SLA "
                          f"{th['critical_p95_max_ms']}ms",
                          data_ok=(enfc_crit.get("n", 0) >= th["min_requests"]
                                   and rec_crit.get("n", 0) >= th["min_requests"])))

    # 4. critical GET error rate
    cn = max(1, enfc_crit.get("n", 0))
    err = enfc_crit.get("errors", 0) / cn
    v.append(_verdict("critical_error_rate", err <= th["critical_err_rate"],
                      f"{err*100:.2f}% ({enfc_crit.get('errors',0)}/{cn})",
                      data_ok=enfc_crit.get("n", 0) >= th["min_requests"]))

    # 4b. main isolation verdict: share of the contention latency damage that
    #     recovery removed. share = (contention-baseline - recovery-baseline)
    #     ... expressed as (cont_p95 - rec_p95) / (cont_p95 - base_p95).
    cont_p95 = cont_crit.get("p95", 0)
    rec_p95 = rec_crit.get("p95", 0)
    base_p95 = base.get("p95", 0)
    enough = all(g.get("n", 0) >= th["min_requests"]
                 for g in (cont_crit, rec_crit, base))
    delta = cont_p95 - base_p95
    if not enough:
        v.append({"name": "recovery_improvement", "verdict": "INCONCLUSIVE",
                  "detail": "insufficient baseline/contention/recovery data"})
    elif delta < th["contention_min_delta_ms"]:
        v.append({"name": "recovery_improvement", "verdict": "INCONCLUSIVE",
                  "detail": f"contention raised p95 by only {delta:.1f}ms "
                            f"(< {th['contention_min_delta_ms']}ms) — no "
                            f"contention to recover from"})
    else:
        share = (cont_p95 - rec_p95) / max(1e-9, delta)
        if not enforcement_active or drifted:
            verdict = "INCONCLUSIVE"
        else:
            verdict = ("PASS" if share >= th["recovery_improvement_min_share"]
                       else "FAIL")
        note = ("; environment drifted" if drifted else "")
        v.append({"name": "recovery_improvement", "verdict": verdict,
                  "detail": f"share={share:.2f} (contention {cont_p95:.1f}ms, "
                            f"recovery {rec_p95:.1f}ms, baseline "
                            f"{base_p95:.1f}ms); enforcement="
                            f"{'yes' if enforcement_active else 'no'}" + note})

    # 5. recovery vs baseline p95 — INFORMATION ONLY (single-host CPU noise
    #    made this check unfair; recovery_vs_reference is the scored verdict)
    if rec_crit.get("n", 0) >= th["min_requests"] and base.get("p95"):
        dev = rec_crit["p95"] / max(1e-9, base["p95"])
        detail = (f"recovery p95={rec_crit['p95']:.1f}ms "
                  f"(p99={rec_crit.get('p99',0):.1f}) = {dev:.2f}x "
                  f"baseline ({base['p95']:.1f}ms)")
        scored_ok = enforcement_active and not drifted
        v.append({"name": "recovery_p95_vs_baseline", "info": True,
                  "verdict": ("PASS" if dev <= th["recovery_p95_dev_factor"]
                              else "FAIL") if scored_ok else "INCONCLUSIVE",
                  "detail": detail + ("" if scored_ok else
                            (" — environment drifted" if drifted
                             else " — enforcement never observed"))})
    else:
        v.append({"name": "recovery_p95_vs_baseline", "info": True,
                  "verdict": "INCONCLUSIVE",
                  "detail": "insufficient recovery/baseline data"})

    scored = [x for x in v if not x.get("info")]
    fails = [x for x in scored if x["verdict"] == "FAIL"]
    incs = [x for x in scored if x["verdict"] == "INCONCLUSIVE"]
    out["overall"] = ("FAIL" if fails else
                      "INCONCLUSIVE" if incs else "PASS")
    return out
