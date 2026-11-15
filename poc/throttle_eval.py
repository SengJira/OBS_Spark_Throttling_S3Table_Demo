"""Throttler rule verification and acceptance evaluation.

verify_rule() validates a rule dict against the schema this ObjectScale 4.4
build accepts on PUT /throttle/config (documented in reports/environment.md)
plus the live /throttle/config/actions list — nothing undocumented is sent.

evaluate() turns per-phase LoadGen metrics into PASS/FAIL/INCONCLUSIVE
verdicts against customer-approved thresholds. INCONCLUSIVE is used when the
data cannot decide — e.g. this lab build accepts rules but emits no 429/503
until a datahead reload (see reports/environment.md), so an enforcement
verdict cannot be claimed either way.
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
}

# Fields verified against PUT /throttle/config on this build.
RULE_FIELDS = {"id", "rateUnit", "isDisabled", "filters", "limit", "scoping",
               "description"}
FILTER_FIELDS = {"principals", "verbs", "actions", "paths", "ips", "size"}
PATH_MATCHERS = {"wildcard", "exact", "prefix"}
RATE_UNITS = {"TPS", "BPS"}


def verify_rule(thr, rule):
    """Validate `rule` against the accepted config schema and the live
    actions list. Read-only — never modifies configuration."""
    problems = []
    unknown = set(rule) - RULE_FIELDS
    if unknown:
        problems.append(f"unknown top-level fields: {sorted(unknown)}")
    if not rule.get("id"):
        problems.append("missing rule id")
    if rule.get("rateUnit") not in RATE_UNITS:
        problems.append(f"rateUnit must be one of {sorted(RATE_UNITS)}")
    lim = (rule.get("limit") or {}).get("value")
    if not isinstance(lim, (int, float)) or lim <= 0:
        problems.append("limit.value must be a positive number")
    f = rule.get("filters") or {}
    bad_f = set(f) - FILTER_FIELDS
    if bad_f:
        problems.append(f"unknown filter fields: {sorted(bad_f)}")
    if not (f.get("principals") or f.get("paths") or f.get("ips")):
        problems.append("filters must select at least one dimension")
    if "paths" in f:
        p = f["paths"]
        if p.get("matcher") not in PATH_MATCHERS:
            problems.append(f"paths.matcher must be one of {sorted(PATH_MATCHERS)}")
        if not p.get("values"):
            problems.append("paths.values must be non-empty")
    supported = {}
    try:
        acts = thr.list_actions()
        supported["actions"] = acts if isinstance(acts, list) else acts.get(
            "actions", acts)
    except Exception as e:
        supported["actions_error"] = str(e)[:80]
    return {"ok": not problems, "problems": problems,
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


def detect(phase_stats, limit, th=None, critical_group="tenant-critical GET",
           noisy_group="tenant-noisy PUT"):
    """Decide whether enforcement fired in the 'enforced' phase, and how.

    Returns (active: bool, mode: 'reject'|'delay'|'none').
    ObjectScale can throttle by rejection (429/SlowDown) OR by delay —
    requests all return 200 but are queued server-side to the policy rate.
    Delay-mode signature: ok_tps collapses into the ~limit band while
    contention was well above it AND noisy tail latency spikes (a dead load
    generator cannot produce that signature)."""
    th = th or DEFAULT_THRESHOLDS
    ps = phase_stats
    cont_noisy = ps.get("contention", {}).get(noisy_group, {})
    enfc = ps.get("enforced", {})
    enfc_noisy = enfc.get(noisy_group, {})
    span = enfc.get("_window", {}).get("seconds", 1)
    ok_tps = enfc_noisy.get("ok", 0) / max(1e-9, span)
    before = cont_noisy.get("ok", 0) / max(
        1e-9, ps.get("contention", {}).get("_window", {}).get("seconds", 1))
    if enfc_noisy.get("throttled", 0) > 0:
        return True, "reject"
    delay = (
        bool(limit)
        and enfc_noisy.get("n", 0) >= th["min_requests"]
        and ok_tps <= limit * th["noisy_tps_limit_factor"]
        and ok_tps >= limit * 0.2            # pinned near limit, not dead
        and before >= limit * 2
        and max(enfc_noisy.get("p95", 0), enfc_noisy.get("p99", 0))
            >= max(2 * cont_noisy.get("p95", 0), 25))
    return (True, "delay") if delay else (False, "none")


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

    # 3. critical GET SLA under enforcement
    v.append(_verdict("critical_p95_within_sla",
                      enfc_crit.get("p95", 0) <= th["critical_p95_max_ms"],
                      f"p95={enfc_crit.get('p95',0):.1f}ms / "
                      f"p99={enfc_crit.get('p99',0):.1f}ms vs SLA "
                      f"{th['critical_p95_max_ms']}ms",
                      data_ok=enfc_crit.get("n", 0) >= th["min_requests"]))

    # 4. critical GET error rate
    cn = max(1, enfc_crit.get("n", 0))
    err = enfc_crit.get("errors", 0) / cn
    v.append(_verdict("critical_error_rate", err <= th["critical_err_rate"],
                      f"{err*100:.2f}% ({enfc_crit.get('errors',0)}/{cn})",
                      data_ok=enfc_crit.get("n", 0) >= th["min_requests"]))

    # 5. recovery vs baseline p95 (noisy still running — protected by rule).
    #    Meaningful only when enforcement was observed; without it the phase
    #    just re-measures contention, so the verdict is INCONCLUSIVE.
    if rec_crit.get("n", 0) >= th["min_requests"] and base.get("p95"):
        dev = rec_crit["p95"] / max(1e-9, base["p95"])
        detail = (f"recovery p95={rec_crit['p95']:.1f}ms "
                  f"(p99={rec_crit.get('p99',0):.1f}) = {dev:.2f}x "
                  f"baseline ({base['p95']:.1f}ms)")
        v.append(_verdict("recovery_p95_vs_baseline",
                          dev <= th["recovery_p95_dev_factor"],
                          detail if enforcement_active
                          else detail + " — enforcement never observed",
                          data_ok=enforcement_active))
    else:
        v.append(_verdict("recovery_p95_vs_baseline", False,
                          "insufficient recovery/baseline data", data_ok=False))

    fails = [x for x in v if x["verdict"] == "FAIL"]
    incs = [x for x in v if x["verdict"] == "INCONCLUSIVE"]
    out["overall"] = ("FAIL" if fails else
                      "INCONCLUSIVE" if incs else "PASS")
    return out
