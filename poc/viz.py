"""Display helpers for notebooks: styled tables, banners, live throttle chart."""
from IPython.display import HTML, display, clear_output
import pandas as pd

from . import results
from .loadgen import classify, THROTTLED


def banner(status, title, msg=""):
    """Status banner. `status` is PASS | FAIL | INCONCLUSIVE | INCOMPLETE, or
    a bool (True -> PASS, False -> FAIL). Amber = INCONCLUSIVE: the evidence
    does not decide, which is never shown as a pass."""
    label = status_label(status)
    color = results.STATUS_COLORS.get(label, results.STATUS_COLORS["FAIL"])
    display(HTML(
        f'<div style="background:{color};color:#fff;padding:10px 16px;border-radius:6px;'
        f'font-size:17px;font-family:sans-serif;margin:8px 0">'
        f'<b>{label}</b> — {title} <span style="opacity:.85">{msg}</span></div>'))


def status_label(status):
    if isinstance(status, str):
        return status.upper()
    return "PASS" if status else "FAIL"


def status_style(row, column="status"):
    """pandas Styler row function colouring a row by its status column."""
    color = results.STATUS_COLORS.get(str(row[column]).upper(), "#b71c1c")
    return [f"background-color:{color};color:#fff"] * len(row)


def table(df, caption=None):
    st = df.style.set_table_attributes('style="font-size:15px"')
    if caption:
        st = st.set_caption(caption)
    display(st)


def object_table(objects):
    df = pd.DataFrame(objects, columns=["Key", "Bytes"])
    df["MiB"] = (df["Bytes"] / 1024 / 1024).round(2)
    return table(df[["Key", "MiB"]])


BIN_S = 5


def tps_series(events, tenant, t0, t_end, bin_s=BIN_S):
    """Per-bin request rates for one tenant over a CONTIGUOUS range of bins
    from t0 to t_end. Empty bins are 0, so a stalled workload shows as a
    drop to zero instead of a line joining its neighbours.
    Returns (bin_start_seconds, total_tps, throttled_tps)."""
    n_bins = int(max(0.0, t_end - t0) // bin_s) + 1
    total, throttled = [0] * n_bins, [0] * n_bins
    for e in events:
        if e.tenant != tenant:
            continue
        i = int((e.ts - t0) // bin_s)
        if 0 <= i < n_bins:
            total[i] += 1
            if classify(e.status, e.code) in THROTTLED:
                throttled[i] += 1
    xs = [i * bin_s for i in range(n_bins)]
    return xs, [v / bin_s for v in total], [v / bin_s for v in throttled]


class LiveChart:
    """Two-panel live chart (TPS + p95 latency per tenant) + throttled series."""

    def __init__(self, loadgen):
        self.lg = loadgen
        self.marks = []  # (epoch, label)

    def mark(self, label):
        import time
        self.marks.append((time.time(), label))

    def draw(self):
        import matplotlib.pyplot as plt
        import statistics
        ev = self.lg._events()
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4))
        if not ev:
            ax1.set_title("waiting for load…")
            display(fig); plt.close(fig); return
        t0, t_end = ev[0].ts, max(e.ts for e in ev)
        for tenant, color in (("tenant-noisy", "#e65100"), ("tenant-critical", "#1565c0")):
            xs, tps, thr = tps_series(ev, tenant, t0, t_end)
            ax1.plot(xs, tps, color=color, label=f"{tenant} TPS")
            if any(thr):
                ax1.plot(xs, thr, color=color, ls=":", lw=2,
                         label=f"{tenant} throttled (429/SlowDown)")
            pts = sorted((e.ts - t0, e.lat_ms) for e in ev if e.tenant == tenant)
            if pts:
                w = 10
                ys = [statistics.quantiles([b for _, b in pts[i:i+w]], n=20)[-1]
                      if len(pts[i:i+w]) >= w else max(b for _, b in pts[i:i+w])
                      for i in range(0, len(pts), w)]
                ax2.plot([x for x, _ in pts[::w][:len(ys)]], ys,
                         color=color, label=f"{tenant} p95")
        # closed phase boundaries (baseline/contention/…) as shaded bands
        for i, ph in enumerate(getattr(self.lg, "phases", [])):
            for ax in (ax1, ax2):
                ax.axvspan(ph["start"] - t0, ph["end"] - t0,
                           color="#dddddd" if i % 2 else "none",
                           alpha=0.5, lw=0)
            ax1.text(ph["start"] - t0, ax1.get_ylim()[1] * .95, ph["name"],
                     rotation=90, fontsize=8, va="top", color="#555")
        for ts, lab in self.marks:
            for ax in (ax1, ax2):
                ax.axvline(ts - t0, color="gray", ls="--", lw=0.8)
            ax1.text(ts - t0, ax1.get_ylim()[1] * .95, lab,
                     rotation=90, fontsize=9, va="top")
        ax1.set_title(f"TPS per tenant ({BIN_S}s bins)"); ax1.legend(fontsize=9)
        ax2.set_title("p95 latency (ms)"); ax2.legend(fontsize=9)
        tot = self.lg.totals()
        fig.suptitle(f"throttled (429/SlowDown): {tot['throttled']}   "
                     f"errors: {tot['errors']}   requests: {tot['events']}",
                     fontsize=12, fontweight="bold")
        display(fig); plt.close(fig)

    def refresh(self, out, seconds=30, step=2.0):
        """Refresh the chart inside an ipywidgets Output for `seconds`."""
        import time
        end = time.time() + seconds
        with out:
            clear_output(wait=True); self.draw()
        while time.time() < end:
            time.sleep(step)
            with out:
                clear_output(wait=True); self.draw()
