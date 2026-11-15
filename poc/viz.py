"""Display helpers for notebooks: styled tables, banners, live throttle chart."""
from IPython.display import HTML, display, clear_output
import pandas as pd

from . import results


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


class LiveChart:
    """Two-panel live chart (TPS + p95 latency per tenant) + throttled counter."""

    def __init__(self, loadgen):
        self.lg = loadgen
        self.marks = []  # (epoch, label)

    def mark(self, label):
        import time
        self.marks.append((time.time(), label))

    def draw(self):
        import matplotlib.pyplot as plt
        import time as _t
        ev = self.lg._events()
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4))
        now = _t.time()
        if not ev:
            ax1.set_title("waiting for load…")
            display(fig); plt.close(fig); return
        t0 = ev[0].ts
        for tenant, color in (("tenant-noisy", "#e65100"), ("tenant-critical", "#1565c0")):
            pts = [e for e in ev if e.tenant == tenant]
            xs, tp, lat = [], [], []
            for e in pts:
                xs.append(e.ts - t0)
                lat.append(e.lat_ms)
                tp.append(429 if e.status in (429, 503) else 200)
            # rolling tps over 5s bins
            bins = {}
            for x in xs:
                bins[int(x // 5)] = bins.get(int(x // 5), 0) + 1
            ax1.plot(sorted(bins), [v / 5 for v in bins.values()],
                     color=color, label=f"{tenant} TPS")
            if lat:
                import statistics
                w = 10
                xs_l = sorted(zip(xs, lat))
                ys = [statistics.quantiles([b for _, b in xs_l[i:i+w]], n=20)[-1]
                      if len(xs_l[i:i+w]) >= w else max(b for _, b in xs_l[i:i+w])
                      for i in range(0, len(xs_l), w)]
                ax2.plot([x for x, _ in xs_l[::w][:len(ys)]], ys,
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
        ax1.set_title("TPS per tenant"); ax1.legend(fontsize=9)
        ax2.set_title("p95 latency (ms)"); ax2.legend(fontsize=9)
        tot = self.lg.totals()
        fig.suptitle(f"429/503 throttled: {tot['throttled']}   "
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
