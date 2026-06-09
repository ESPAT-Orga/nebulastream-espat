"""Reproduce the probe-latency variance graph using probe_duration_s as the y-metric.

The original graph used probe_latency_listener (μs) from the LatencyListener.
In the fixed code, per-buffer pipeline latency is sub-microsecond, making the
listener metric indistinguishable across conditions. probe_duration_s (total
wall-clock time for the probe query to finish) captures the full effect of the
O(log N + k) vs O(N) store lookup, so we use that instead. Both metrics tell
the same story: high cross-condition variance before the fix, tight grouping after.

This script mirrors the notebook structure exactly:
  rows  = build_windows_per_probe_window  (1 and 100)
  cols  = statistic_type                  (CountMin only here)
  x     = numberOfWorkerThreads           (16)
  hue   = statisticStoreType             (DEFAULT / WINDOW / SUB_STORES)
  y     = probe_duration_s (seconds)
"""

import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os, sys

csv_path = os.path.join(os.path.dirname(__file__), "results_statistic_probe.csv")
df = pd.read_csv(csv_path)
df = df[df["issue"] == "ok"].copy()
df["probe_duration_s"] = pd.to_numeric(df["probe_duration_s"], errors="coerce")
df = df.dropna(subset=["probe_duration_s"])

dataset = "ClusterMonitoring"
df_ds = df[df["dataset"] == dataset]

row_values = sorted(df_ds["build_windows_per_probe_window"].unique())
col_values = sorted(df_ds["statistic_type"].unique())
n_rows, n_cols = len(row_values), len(col_values)

fig, axes = plt.subplots(
    n_rows, n_cols,
    figsize=(7 * n_cols, 5 * n_rows),
    sharey=False,
    squeeze=False,
)

store_order = ["DEFAULT", "WINDOW", "SUB_STORES"]
palette = sns.color_palette("tab10", 3)

for ri, bwpw in enumerate(row_values):
    for ci, stat_type in enumerate(col_values):
        ax = axes[ri][ci]
        subset = df_ds[
            (df_ds["build_windows_per_probe_window"] == bwpw)
            & (df_ds["statistic_type"] == stat_type)
        ]
        if subset.empty:
            ax.set_visible(False)
            continue

        sns.boxplot(
            data=subset,
            x="numberOfWorkerThreads",
            y="probe_duration_s",
            hue="statisticStoreType",
            hue_order=[s for s in store_order if s in subset["statisticStoreType"].unique()],
            palette=palette,
            showfliers=True,
            ax=ax,
        )

        # Annotate each box with n= count
        ax.set_title(
            f"{stat_type}  |  windows={bwpw}",
            fontsize=13, fontweight="bold",
        )
        ax.set_xlabel("Worker threads", fontsize=11)
        ax.set_ylabel("Probe query duration (s)" if ci == 0 else "", fontsize=11)
        ax.legend(title="Store type", fontsize=9, loc="best")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.2f}s"))

fig.suptitle(
    f"Probe latency — Dataset: {dataset} (AFTER fix)\n"
    "rows=windows, x=threads, hue=store type",
    fontsize=14, y=1.01,
)
plt.tight_layout()

out = os.path.join(os.path.dirname(__file__), "probe_variance_after_fix.png")
fig.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved: {out}")

# Also print the numeric summary the graph is built from
print("\n=== Numeric summary (probe_duration_s) ===")
print(f"{'windows':>8} {'store':>12} {'n':>4}  {'mean':>8}  {'std':>7}  {'CV%':>6}  {'min':>6}  {'max':>6}")
for bwpw in row_values:
    for store in store_order:
        sub = df_ds[
            (df_ds["build_windows_per_probe_window"] == bwpw)
            & (df_ds["statisticStoreType"] == store)
        ]["probe_duration_s"]
        if sub.empty:
            continue
        m, s = sub.mean(), sub.std()
        cv = s / m * 100 if m > 0 else float("nan")
        print(f"  {bwpw:6d}  {store:>12}  {len(sub):3d}  {m:8.3f}  {s:7.3f}  {cv:6.1f}%  {sub.min():6.3f}  {sub.max():6.3f}")
    print()
