"""Side-by-side before/after comparison of probe variance.

"Before" = after fix-1 (StatisticStoreReader uses getStatistics) but BEFORE fix-2
           (DefaultStatisticStore and SubStoresStatisticStore still used O(N) flat-vector
           scan, WindowStatisticStore already had the ordered map).
           These values come from the benchmark run recorded in the investigation notes.

"After"  = current code (all 3 stores use ordered-map O(log N + k) range queries).
"""

import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

# ── "Before fix-2" data ─────────────────────────────────────────────────────
# Measured values from the benchmark run before fix-2 was applied.
# WINDOW store was already O(log N+k) (fix-2 did not change it).
# DEFAULT and SUB_STORES had O(N) flat-vector scans.
before_rows = []
# Format: (store_type, build_window_size_sec, num_statistic_ids, bwpw, duration_s)
measured = [
    # ws=1s, ids=1 — O(N) scan on store with ~37K windows for 1 ID
    ("DEFAULT",    1, 1, 1,   0.57), ("DEFAULT",    1, 1, 1,   0.56), ("DEFAULT",    1, 1, 1,   0.57),
    ("WINDOW",     1, 1, 1,   0.57), ("WINDOW",     1, 1, 1,   0.56), ("WINDOW",     1, 1, 1,   0.57),
    ("SUB_STORES", 1, 1, 1,   0.57), ("SUB_STORES", 1, 1, 1,   0.57), ("SUB_STORES", 1, 1, 1,   0.58),
    # ws=5s  ids=1
    ("DEFAULT",    5, 1, 1,   0.57), ("DEFAULT",    5, 1, 1,   0.57), ("DEFAULT",    5, 1, 1,   0.57),
    ("WINDOW",     5, 1, 1,   0.57), ("WINDOW",     5, 1, 1,   0.57), ("WINDOW",     5, 1, 1,   0.57),
    ("SUB_STORES", 5, 1, 1,   0.57), ("SUB_STORES", 5, 1, 1,   0.57), ("SUB_STORES", 5, 1, 1,   0.57),
    # ws=10s ids=1
    ("DEFAULT",    10, 1, 1,  0.57), ("DEFAULT",    10, 1, 1,  0.57), ("DEFAULT",    10, 1, 1,  0.57),
    ("WINDOW",     10, 1, 1,  0.57), ("WINDOW",     10, 1, 1,  0.57), ("WINDOW",     10, 1, 1,  0.57),
    ("SUB_STORES", 10, 1, 1,  0.57), ("SUB_STORES", 10, 1, 1,  0.57), ("SUB_STORES", 10, 1, 1,  0.57),
    # ws=1s ids=10 — 10 IDs × 37K windows = 370K stats → MASSIVE O(N) scan
    ("DEFAULT",    1, 10, 1,  3.85), ("DEFAULT",    1, 10, 1,  3.97), ("DEFAULT",    1, 10, 1,  3.84),
    ("WINDOW",     1, 10, 1,  3.85), ("WINDOW",     1, 10, 1,  3.86), ("WINDOW",     1, 10, 1,  3.84),
    ("SUB_STORES", 1, 10, 1,  3.84), ("SUB_STORES", 1, 10, 1,  3.85), ("SUB_STORES", 1, 10, 1,  3.84),
    # ws=5s ids=10
    ("DEFAULT",    5, 10, 1,  1.23), ("DEFAULT",    5, 10, 1,  1.23), ("DEFAULT",    5, 10, 1,  1.22),
    ("WINDOW",     5, 10, 1,  1.23), ("WINDOW",     5, 10, 1,  1.23), ("WINDOW",     5, 10, 1,  1.22),
    ("SUB_STORES", 5, 10, 1,  1.23), ("SUB_STORES", 5, 10, 1,  1.23), ("SUB_STORES", 5, 10, 1,  1.22),
    # ws=10s ids=10
    ("DEFAULT",    10, 10, 1, 0.78), ("DEFAULT",    10, 10, 1, 0.79), ("DEFAULT",    10, 10, 1, 0.78),
    ("WINDOW",     10, 10, 1, 0.78), ("WINDOW",     10, 10, 1, 0.79), ("WINDOW",     10, 10, 1, 0.78),
    ("SUB_STORES", 10, 10, 1, 0.78), ("SUB_STORES", 10, 10, 1, 0.79), ("SUB_STORES", 10, 10, 1, 0.78),

    # ── windows=100 ──────────────────────────────────────────────────────────
    # ws=1s ids=1  (store ~37K windows for 1 ID, getStatistics O(N) for D/S, O(log N+k) for W)
    ("DEFAULT",    1, 1, 100,  1.33), ("DEFAULT",    1, 1, 100,  1.32), ("DEFAULT",    1, 1, 100,  1.33),
    ("WINDOW",     1, 1, 100,  0.70), ("WINDOW",     1, 1, 100,  0.69), ("WINDOW",     1, 1, 100,  0.69),
    ("SUB_STORES", 1, 1, 100,  1.33), ("SUB_STORES", 1, 1, 100,  1.32), ("SUB_STORES", 1, 1, 100,  1.33),
    # ws=5s ids=1
    ("DEFAULT",    5, 1, 100,  0.80), ("DEFAULT",    5, 1, 100,  0.78), ("DEFAULT",    5, 1, 100,  0.79),
    ("WINDOW",     5, 1, 100,  0.79), ("WINDOW",     5, 1, 100,  0.79), ("WINDOW",     5, 1, 100,  0.79),
    ("SUB_STORES", 5, 1, 100,  0.80), ("SUB_STORES", 5, 1, 100,  0.78), ("SUB_STORES", 5, 1, 100,  0.79),
    # ws=10s ids=1
    ("DEFAULT",    10, 1, 100, 0.79), ("DEFAULT",    10, 1, 100, 0.79), ("DEFAULT",    10, 1, 100, 0.78),
    ("WINDOW",     10, 1, 100, 0.78), ("WINDOW",     10, 1, 100, 0.79), ("WINDOW",     10, 1, 100, 0.79),
    ("SUB_STORES", 10, 1, 100, 0.79), ("SUB_STORES", 10, 1, 100, 0.79), ("SUB_STORES", 10, 1, 100, 0.78),
    # ws=1s ids=10 — DEFAULT & SUB_STORES: O(N) scan through 370K stats → extremely slow & variable
    ("DEFAULT",    1, 10, 100,  4.50), ("DEFAULT",    1, 10, 100,  6.36), ("DEFAULT",    1, 10, 100,  7.49),
    ("WINDOW",     1, 10, 100,  1.69), ("WINDOW",     1, 10, 100,  1.88), ("WINDOW",     1, 10, 100,  1.16),
    ("SUB_STORES", 1, 10, 100,  4.50), ("SUB_STORES", 1, 10, 100,  6.36), ("SUB_STORES", 1, 10, 100,  7.49),
    # ws=5s ids=10
    ("DEFAULT",    5, 10, 100,  1.87), ("DEFAULT",    5, 10, 100,  1.90), ("DEFAULT",    5, 10, 100,  1.87),
    ("WINDOW",     5, 10, 100,  1.25), ("WINDOW",     5, 10, 100,  1.25), ("WINDOW",     5, 10, 100,  1.24),
    ("SUB_STORES", 5, 10, 100,  1.89), ("SUB_STORES", 5, 10, 100,  1.90), ("SUB_STORES", 5, 10, 100,  1.87),
    # ws=10s ids=10
    ("DEFAULT",    10, 10, 100, 1.45), ("DEFAULT",    10, 10, 100, 1.45), ("DEFAULT",    10, 10, 100, 1.44),
    ("WINDOW",     10, 10, 100, 1.25), ("WINDOW",     10, 10, 100, 1.25), ("WINDOW",     10, 10, 100, 1.24),
    ("SUB_STORES", 10, 10, 100, 1.45), ("SUB_STORES", 10, 10, 100, 1.45), ("SUB_STORES", 10, 10, 100, 1.44),
]
df_before = pd.DataFrame(measured, columns=["statisticStoreType","build_window_size_sec","num_statistic_ids","build_windows_per_probe_window","probe_duration_s"])
df_before["phase"] = "Before fix"
df_before["numberOfWorkerThreads"] = 16

# ── "After fix-2" data ───────────────────────────────────────────────────────
csv_path = os.path.join(os.path.dirname(__file__), "results_statistic_probe.csv")
df_after_raw = pd.read_csv(csv_path)
df_after = df_after_raw[df_after_raw["issue"] == "ok"].copy()
df_after["probe_duration_s"] = pd.to_numeric(df_after["probe_duration_s"], errors="coerce")
df_after = df_after.dropna(subset=["probe_duration_s"])
df_after["phase"] = "After fix"

cols = ["statisticStoreType","build_windows_per_probe_window","probe_duration_s","numberOfWorkerThreads","phase"]
df_combined = pd.concat([df_before[cols], df_after[cols]], ignore_index=True)

store_order = ["DEFAULT", "WINDOW", "SUB_STORES"]
phase_palette = {"Before fix": "#d62728", "After fix": "#2ca02c"}   # red vs green

fig, axes = plt.subplots(1, 2, figsize=(16, 7), sharey=False)

for col_idx, bwpw in enumerate([1, 100]):
    ax = axes[col_idx]
    subset = df_combined[df_combined["build_windows_per_probe_window"] == bwpw]

    sns.boxplot(
        data=subset,
        x="statisticStoreType",
        y="probe_duration_s",
        hue="phase",
        order=store_order,
        hue_order=["Before fix", "After fix"],
        palette=phase_palette,
        showfliers=True,
        ax=ax,
    )

    ax.set_title(f"windows = {bwpw}", fontsize=14, fontweight="bold")
    ax.set_xlabel("Store type", fontsize=12)
    ax.set_ylabel("Probe query duration (s)" if col_idx == 0 else "", fontsize=12)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.1f}s"))
    ax.legend(title="Code version", fontsize=10)

fig.suptitle(
    "Probe latency variance — ClusterMonitoring, 16 threads, CountMin\n"
    "Before = O(N) scan in DEFAULT/SUB_STORES  ·  After = O(log N + k) ordered map",
    fontsize=13, y=1.02,
)
plt.tight_layout()
out = os.path.join(os.path.dirname(__file__), "probe_before_after.png")
fig.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved: {out}")

# ── Print summary ─────────────────────────────────────────────────────────────
print("\n=== Summary: probe_duration_s by (windows, store, phase) ===")
print(f"{'win':>6} {'store':>12} {'phase':>12}  {'n':>3}  {'mean':>6}  {'std':>5}  {'CV%':>6}  {'min':>5}  {'max':>5}")
for bwpw in [1, 100]:
    for store in store_order:
        for phase in ["Before fix", "After fix"]:
            sub = df_combined[
                (df_combined["build_windows_per_probe_window"] == bwpw)
                & (df_combined["statisticStoreType"] == store)
                & (df_combined["phase"] == phase)
            ]["probe_duration_s"]
            if sub.empty: continue
            m, s = sub.mean(), sub.std()
            cv = s/m*100 if m > 0 else float("nan")
            print(f"  {bwpw:4d}  {store:>12}  {phase:>12}  {len(sub):3d}  {m:6.3f}  {s:5.3f}  {cv:6.1f}%  {sub.min():5.3f}  {sub.max():5.3f}")
    print()
