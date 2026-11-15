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
