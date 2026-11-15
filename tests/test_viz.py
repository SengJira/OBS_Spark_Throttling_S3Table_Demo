"""Live-chart series: contiguous bins, stalls visible, throttled separate (#15)."""
import pathlib, sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import viz
from poc.loadgen import Event


def ev(ts, tenant="tenant-noisy", status=200, code=""):
    return Event(ts, tenant, "PUT", 5.0, status, code, 0, "p")


def test_empty_bins_are_zero_so_a_stall_is_visible():
    # 10 events in bin 0, nothing for 15 s, 5 events in bin 4
    events = [ev(100 + i * 0.1) for i in range(10)] + \
             [ev(120 + i * 0.1) for i in range(5)]
    xs, tps, thr = viz.tps_series(events, "tenant-noisy", 100, 121)
    assert xs == [0, 5, 10, 15, 20]
    assert tps == [2.0, 0, 0, 0, 1.0]
    assert thr == [0, 0, 0, 0, 0]


def test_values_follow_time_order_not_insertion_order():
    # late bin inserted first: the old code paired sorted keys with
    # insertion-ordered values and drew the wrong rate at each x
    events = [ev(111)] * 5 + [ev(101)] * 10
    xs, tps, _ = viz.tps_series(events, "tenant-noisy", 100, 112)
    assert list(zip(xs, tps)) == [(0, 2.0), (5, 0.0), (10, 1.0)]


def test_throttled_responses_are_a_separate_series():
    events = [ev(100), ev(101, status=429, code="TooManyRequests"),
              ev(102, status=503, code="SlowDown"),
              ev(103, status=503, code="ServiceUnavailable")]   # not a throttle
    _, tps, thr = viz.tps_series(events, "tenant-noisy", 100, 104)
    assert tps == [4 / 5] and thr == [2 / 5]


def test_series_covers_the_global_range_for_a_tenant_that_stopped():
    events = [ev(100, "tenant-noisy"), ev(100, "tenant-critical"),
              ev(118, "tenant-critical")]
    xs, tps, _ = viz.tps_series(events, "tenant-noisy", 100, 118)
    assert xs == [0, 5, 10, 15] and tps == [0.2, 0, 0, 0]


# ---- incremental chart state: same series, bounded redraw cost ----

def test_chart_state_matches_full_rescan_when_fed_in_chunks():
    import random
    rnd = random.Random(7)
    events = sorted((ev(100 + rnd.random() * 60,
                        rnd.choice(["tenant-noisy", "tenant-critical"]),
                        rnd.choice([200, 200, 200, 429]), "") for _ in range(2000)),
                    key=lambda e: e.ts)
    st = viz.ChartState()
    for i in range(0, len(events), 137):          # arbitrary chunking
        st.add(events[i:i + 137])
    for tenant in ("tenant-noisy", "tenant-critical"):
        assert st.series(tenant) == viz.tps_series(events, tenant, st.t0, st.t_end)


def test_chart_state_p95_per_bin():
    st = viz.ChartState()
    st.add([Event(100 + i * 0.01, "tenant-critical", "GET", float(i), 200, "", 0, "p")
            for i in range(100)])                  # one bin, latencies 0..99
    xs, ys = st.p95_series("tenant-critical")
    assert xs == [0] and 94 <= ys[0] <= 95


def test_live_chart_reads_each_event_once():
    from poc import loadgen

    class LG:                                   # the two calls draw() makes
        def __init__(self): self.calls = []; self.batches = [[ev(100)], [ev(101)], []]
        def events_since(self, cursor):
            self.calls.append(cursor)
            b = self.batches.pop(0)
            return b, cursor + len(b)
    lg, st = LG(), viz.ChartState()
    cursor = 0
    for _ in range(3):
        fresh, cursor = lg.events_since(cursor); st.add(fresh)
    assert lg.calls == [0, 1, 2] and st.bins["tenant-noisy"][0][0] == 2
    # real LoadGen cursor: nothing is returned twice, nothing is skipped
    g = loadgen.LoadGen(bucket="b", max_keys=0)
    for i in range(5):
        g._rec("tenant-noisy", "PUT", 100 + i, 1.0, 200)
    a, c1 = g.events_since(0)
    g._rec("tenant-noisy", "PUT", 200, 1.0, 429, "TooManyRequests")
    b, c2 = g.events_since(c1)
    assert len(a) == 5 and len(b) == 1 and b[0].status == 429 and c2 == 6
    assert g.events_since(c2) == ([], 6)
    assert g.totals()["throttled"] == 1 and g.totals()["ok"] == 5
