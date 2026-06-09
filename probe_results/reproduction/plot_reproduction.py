"""Reproduce the original probe-latency-variance graph structure after the fix.

Extracts individual LatencyListener events from worker log files, identifies
the probe query (the minority query by event count), and plots:
  x = memory_budget, hue = statistic_type, facets = build_windows_per_probe_window
  y = probe_latency_listener (µs)

This directly mirrors the original graph structure.
"""

import os
import re
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPRO_DIR = os.path.dirname(__file__)
LATENCY_RE = re.compile(
    r"Latency for queryId QueryId\([^)]*distributed=(\S+)\)"
    r".*?is ([\d.]+) (us|ns|ms)"
)


def parse_probe_latencies_us(log_path):
    """Return list of per-event latencies in µs for the probe query in log_path.

    The build query always dominates event count; the probe query is the
    minority (smallest event count among queries seen in the file).
    """
    by_qid: dict[str, list[float]] = {}
    with open(log_path) as f:
        for line in f:
            m = LATENCY_RE.search(line)
            if not m:
                continue
            qid, value, unit = m.group(1), float(m.group(2)), m.group(3)
            if unit == "ns":
                value /= 1000.0
            elif unit == "ms":
                value *= 1000.0
            by_qid.setdefault(qid, []).append(value)
    if not by_qid:
        return []
    probe_qid = min(by_qid, key=lambda q: len(by_qid[q]))
    return by_qid[probe_qid]


rows = []
for trial_dir in sorted(os.listdir(REPRO_DIR)):
    if not trial_dir.startswith("probe_"):
        continue
    parts = trial_dir.split("_")
    try:
        dataset    = parts[2]
        stat_type  = parts[3]
        mb         = int(parts[4].replace("mb", ""))
        ws_sec     = int(parts[5].replace("ws", "").replace("sec", ""))
        threads    = int(parts[6].replace("t", ""))
        store      = parts[7]
        n_ids      = int(parts[8].replace("ids", ""))
        bwpw       = int(parts[9].replace("xwindow", ""))
    except (IndexError, ValueError):
        continue

    trial_path = os.path.join(REPRO_DIR, trial_dir)
    log_files = [f for f in os.listdir(trial_path) if f.startswith("SingleNodeStdout_")]
    if not log_files:
        continue
    log_path = os.path.join(trial_path, log_files[0])
    latencies = parse_probe_latencies_us(log_path)
    for lat in latencies:
        rows.append({
            "statistic_type": stat_type,
            "memory_budget": mb,
            "build_windows_per_probe_window": bwpw,
            "build_window_size_sec": ws_sec,
            "numberOfWorkerThreads": threads,
            "statisticStoreType": store,
            "probe_latency_us": lat,
        })

df = pd.DataFrame(rows)
print(f"Total events extracted: {len(df)}")
print(df.groupby(["statistic_type","memory_budget","build_windows_per_probe_window"])["probe_latency_us"].agg(["count","mean","std","max"]).to_string())

df_plot = df[df["build_window_size_sec"] == 1].copy()

type_order = ["Reservoir", "EquiWidthHistogram", "CountMin"]
bwpw_vals  = sorted(df_plot["build_windows_per_probe_window"].unique())
mb_vals    = sorted(df_plot["memory_budget"].unique())
palette    = {"Reservoir": "#1f77b4", "EquiWidthHistogram": "#ff7f0e", "CountMin": "#2ca02c"}

fig, axes = plt.subplots(1, len(bwpw_vals), figsize=(7 * len(bwpw_vals), 6), sharey=False)
if len(bwpw_vals) == 1:
    axes = [axes]

for col_idx, bwpw in enumerate(bwpw_vals):
    ax = axes[col_idx]
    subset = df_plot[df_plot["build_windows_per_probe_window"] == bwpw]
    sns.boxplot(
        data=subset,
        x="memory_budget",
        y="probe_latency_us",
        hue="statistic_type",
        hue_order=[t for t in type_order if t in subset["statistic_type"].unique()],
        palette=palette,
        order=mb_vals,
        showfliers=True,
        ax=ax,
    )
    ax.set_title(f"windows={bwpw} | no. statistic windows=1", fontsize=13, fontweight="bold")
    ax.set_xlabel("Memory Budget (bytes)", fontsize=11)
    ax.set_ylabel("Probe Latency (µs)" if col_idx == 0 else "", fontsize=11)
    ax.legend(title="statistic_type", fontsize=9)

fig.suptitle(
    "Dataset: ClusterMonitoring | numWorkerThreads=16\n(AFTER fix — getSingleStatistic → getStatistics range query)",
    fontsize=13, y=1.02,
)
plt.tight_layout()
out = os.path.join(REPRO_DIR, "probe_latency_after_fix.png")
fig.savefig(out, dpi=150, bbox_inches="tight")
print(f"\nSaved: {out}")
