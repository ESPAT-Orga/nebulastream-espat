"""Before vs. after fix comparison using probe_duration_s from the result CSVs.

probe_duration_s = wall-clock seconds from probe-query submission to completion.
This is the metric that exposes the fix: before fix, getSingleStatistic always
missed when build_windows_per_probe_window=100, so every run was ~0.56 s
(dominated by source-startup overhead with 0 output records).
After the fix, getStatistics finds N build-window statistics, so CountMin and
EquiWidthHistogram at large memory budgets take noticeably longer.

LatencyListener events cannot be used here: 1000 probe tuples x 32 bytes = 32 KB
fits in one 100 KB TupleBuffer, so there is exactly one LatencyListener event
per probe run and its value is always 0-2 µs regardless of fix state.

Layout: 2 rows (windows=1, windows=100) x 2 cols (Before fix, After fix)
x-axis: memory_budget
hue:    statistic_type
y-axis: probe_duration_s
"""

import os
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))

df_before = pd.read_csv(os.path.join(HERE, "before_fix", "results_statistic_probe.csv"))
df_after  = pd.read_csv(os.path.join(HERE, "reproduction", "results_statistic_probe.csv"))
df_before["phase"] = "Before fix"
df_after["phase"]  = "After fix"

for label, df in [("Before", df_before), ("After", df_after)]:
    sub = df[(df["issue"] == "ok") & (df["build_window_size_sec"] == 1)]
    print(f"\n=== {label} fix --- probe_duration_s (ws=1s) ===")
    print(
        sub.groupby(["statistic_type", "memory_budget", "build_windows_per_probe_window"])
           ["probe_duration_s"]
           .agg(["mean", "std", "min", "max"])
           .round(3)
           .to_string()
    )

TYPE_ORDER = ["Reservoir", "EquiWidthHistogram", "CountMin"]
PALETTE    = {"Reservoir": "#1f77b4", "EquiWidthHistogram": "#ff7f0e", "CountMin": "#2ca02c"}
MB_ORDER   = [1024, 10240]
BWPW_VALS  = [1, 100]

fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=False)

for row_idx, bwpw in enumerate(BWPW_VALS):
    for col_idx, (phase_label, df) in enumerate(
        [("Before fix", df_before), ("After fix", df_after)]
    ):
        ax = axes[row_idx][col_idx]
        subset = df[
            (df["issue"] == "ok")
            & (df["build_window_size_sec"] == 1)
            & (df["build_windows_per_probe_window"] == bwpw)
        ]
        present = [t for t in TYPE_ORDER if t in subset["statistic_type"].unique()]
        sns.boxplot(
            data=subset,
            x="memory_budget",
            y="probe_duration_s",
            hue="statistic_type",
            hue_order=present,
            palette=PALETTE,
            order=MB_ORDER,
            showfliers=True,
            ax=ax,
        )
        ax.set_title(
            f"{phase_label}  |  windows={bwpw}  |  no. statistic windows=1",
            fontsize=11,
            fontweight="bold",
        )
        ax.set_xlabel("Memory Budget (bytes)", fontsize=10)
        ax.set_ylabel("Probe duration (s)" if col_idx == 0 else "", fontsize=10)
        ax.legend(title="statistic_type", fontsize=8)

fig.suptitle(
    "Dataset: ClusterMonitoring | numWorkerThreads=16\n"
    "Y: probe_duration_s  (higher = more statistics found = fix is working)",
    fontsize=12,
    y=1.02,
)
plt.tight_layout()
out = os.path.join(HERE, "probe_latency_before_after.png")
fig.savefig(out, dpi=150, bbox_inches="tight")
print(f"\nSaved: {out}")
