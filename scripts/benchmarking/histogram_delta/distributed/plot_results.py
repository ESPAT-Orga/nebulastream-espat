#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Plot the distributed statistic-collection (wire bytes) benchmark results.

Reads a results directory containing ``summary.csv`` and ``<topology>/<variant>_traffic.csv`` and
writes PNGs, one pair per topology:

  - wire_bytes[_<topology>].png       : total bytes reaching the root per variant, with the saving
                                        factor against the uncompressed `split` baseline
  - throughput[_<topology>].png       : mean ingest throughput per variant (only for an unthrottled
                                        run -- see plot_throughput)
  - traffic_over_time[_<topology>].png: root eth0 RX over the hold, per variant

Both charts default to the four payload-representation variants (`--exclude prometheus,local`).
`prometheus` and `local` are the bracket, not the comparison: one is the raw stream that crosses when
no synopsis is built, the other the floor when nothing variable-sized crosses at all. Their numbers
stay in summary.csv and in the table this script prints.

The figures carry NO title or subtitle: they are meant to sit under a paper caption, which is where
the run parameters belong. What stays on the canvas is what a caption cannot replace -- axis labels,
category labels, per-bar values, and the legend.

Styling follows scripts/benchmarking/histogram_delta/plot_results.py: Okabe-Ito colorblind-safe
palette in a fixed order, one y-axis, thin lines, recessive grid.

Palette check (adjacent-pair list, which is the one bars and lines use), computed not eyeballed --
lightness band PASS, chroma floor PASS, CVD separation PASS (worst 13.1 dE deutan on the default four
variants, 9.6 with prometheus+local added; target 8.0), normal-vision separation PASS (worst 15.6 dE,
floor 15.0), contrast WARN for the lightest marks. The WARN is discharged by never encoding identity
in color alone: every bar carries its category as a tick label and its value as a direct label, every
line is named in the legend, and the table this script prints carries every number.

Needs matplotlib + pandas, which the sibling suite's venv already has:
  scripts/benchmarking/histogram_delta/.venv/bin/python \\
      scripts/benchmarking/histogram_delta/distributed/plot_results.py <results-dir>
"""

import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

# Okabe-Ito CVD-safe categorical palette (fixed order).
BLACK, ORANGE, SKY, GREEN, YELLOW, BLUE, VERMILLION, PURPLE = (
    "#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
GRID = "#dddddd"
INK = "#222222"
MUTED = "#666666"

# Fixed variant order: what crosses the network, from the whole raw stream down to four scalars. Any
# variant not listed still plots, appended in the order it appears in the summary.
VARIANT_ORDER = ["prometheus", "split", "split_zstd", "delta", "delta_zstd", "local"]
VARIANT_COLOR = {
    "prometheus": PURPLE,
    "split": BLUE,
    "split_zstd": SKY,
    "delta": VERMILLION,
    "delta_zstd": ORANGE,
    "local": GREEN,
}
VARIANT_LABEL = {
    "prometheus": "prometheus\nraw stream",
    "split": "split\nfull synopsis",
    "split_zstd": "split + zstd\ncompressed synopsis",
    "delta": "delta\nsparse delta",
    "delta_zstd": "delta + zstd\ncompressed delta",
    "local": "local\n4 scalars",
}
BASELINE = "split"  # the uncompressed synopsis the other variants are measured against


def _style(ax):
    ax.grid(True, color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(MUTED)
    ax.tick_params(colors=MUTED)


def _ordered(variants):
    """Known variants first, in VARIANT_ORDER; anything else appended in encounter order."""
    known = [v for v in VARIANT_ORDER if v in variants]
    return known + [v for v in variants if v not in set(known)]


def _color(variant):
    return VARIANT_COLOR.get(variant, MUTED)


def _label(variant):
    return VARIANT_LABEL.get(variant, variant)


def plot_wire_bytes(df, topology, variants, out):
    """Vertical bars of total root-ingress bytes per variant, annotated with the saving vs `split`.

    Linear y on purpose: bar height has to stay proportional to the value it encodes, and a log axis
    would flatter the small variants by giving them a bar they did not earn. Every bar is
    direct-labelled with its value and its factor against `split`, so nothing depends on reading a
    height off the axis.
    """
    rows = df[df["topology"] == topology]
    mb = {v: rows[rows["variant"] == v]["total_bytes"].iloc[0] / 1e6 for v in variants}
    base = mb.get(BASELINE)

    fig, ax = plt.subplots(figsize=(1.55 * len(variants) + 1.6, 5.2))
    _style(ax)
    xpos = list(range(len(variants)))
    ax.bar(xpos, [mb[v] for v in variants], color=[_color(v) for v in variants], width=0.62, zorder=3)

    for x, v in zip(xpos, variants):
        note = f"{mb[v]:,.1f} MB"
        if base and mb[v] > 0 and v != BASELINE:
            ### Say which direction: a variant can sit ABOVE the baseline (prometheus does), and
            ### phrasing that as a fraction of a "saving" would read as a win.
            note += (f"\n{base / mb[v]:.1f}x less" if mb[v] < base else f"\n{mb[v] / base:.1f}x more")
        ax.annotate(note, (x, mb[v]), color=INK, fontsize=9, ha="center", va="bottom",
                    xytext=(0, 5), textcoords="offset points", linespacing=1.35)

    ax.set_xticks(xpos)
    ax.set_xticklabels([_label(v) for v in variants], fontsize=9, color=INK)
    ax.set_ylim(0, max(mb.values()) * 1.22)  # room for the two-line annotations
    ax.set_ylabel("bytes received (MB)", color=INK)
    ax.grid(axis="x", visible=False)  # vertical gridlines say nothing about nominal categories
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


def plot_throughput(df, topology, variants, out):
    """Vertical bars of mean ingest throughput per variant, annotated with the cost against `split`.

    The number is `mean_tps` from summary.csv: the mean of the engine's own throughput-listener samples
    over the busiest stream, i.e. tuples/s pulled through the source-side pipeline. It only says
    something about the VARIANTS when the run was unthrottled (GENERATOR_RATE=0); a rate-limited run
    pins every variant to the limiter, so this refuses to plot that case rather than draw four
    identical bars.
    """
    rows = df[df["topology"] == topology]
    if "mean_tps" not in rows.columns:
        print(f"summary.csv has no mean_tps column (pre-throughput run), skipping {out}")
        return
    mtps = {v: rows[rows["variant"] == v]["mean_tps"].iloc[0] / 1e6 for v in variants}
    if max(mtps.values()) <= 0:
        print(f"no throughput samples recorded, skipping {out}")
        return
    ### A spread this tight across variants means the sources were rate-limited, not that the variants
    ### perform identically. Plotting it would present the limiter as a result.
    spread = (max(mtps.values()) - min(mtps.values())) / max(mtps.values())
    if spread < 0.01:
        print(f"throughput spread is {spread * 100:.2f}% across variants -- the sources were almost "
              f"certainly rate-limited (re-run with GENERATOR_RATE=0); skipping {out}")
        return
    base = mtps.get(BASELINE)

    fig, ax = plt.subplots(figsize=(1.55 * len(variants) + 1.6, 5.2))
    _style(ax)
    xpos = list(range(len(variants)))
    ax.bar(xpos, [mtps[v] for v in variants], color=[_color(v) for v in variants], width=0.62, zorder=3)

    for x, v in zip(xpos, variants):
        note = f"{mtps[v]:.2f} M/s"
        if base and v != BASELINE:
            pct = (mtps[v] / base - 1) * 100
            note += f"\n{abs(pct):.0f}% {'faster' if pct >= 0 else 'slower'}"
        ax.annotate(note, (x, mtps[v]), color=INK, fontsize=9, ha="center", va="bottom",
                    xytext=(0, 5), textcoords="offset points", linespacing=1.35)

    ax.set_xticks(xpos)
    ax.set_xticklabels([_label(v) for v in variants], fontsize=9, color=INK)
    ax.set_ylim(0, max(mtps.values()) * 1.22)
    ax.set_ylabel("ingest throughput (M tuples/s)", color=INK)
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


def plot_traffic_over_time(results_dir, topology, variants, out):
    """Root eth0 RX over the hold. The ramp at the start is the queries deploying and the sources
    connecting."""
    topo_dir = os.path.join(results_dir, topology.replace("/", "-"))
    fig, ax = plt.subplots(figsize=(8.4, 4.6))
    _style(ax)
    plotted = 0
    for v in variants:
        path = os.path.join(topo_dir, f"{v}_traffic.csv")
        if not os.path.exists(path):
            continue
        t = pd.read_csv(path)
        ax.plot(t["elapsed_s"], t["root_rx_bps"] / 1e6, color=_color(v), lw=1.6,
                label=_label(v).replace("\n", " — "), zorder=4)
        plotted += 1
    if not plotted:
        plt.close(fig)
        print(f"no traffic CSVs under {topo_dir}, skipping {out}")
        return

    ### Log y is for a RATE spanning decades, not for bar length: the marks here are lines, whose
    ### position (not extent) carries the value, so a log axis does not overstate anything.
    ax.set_yscale("log")
    ax.set_xlabel("elapsed (s)", color=INK)
    ax.set_ylabel("root eth0 RX (MB/s, log scale)", color=INK)
    ### The legend stays: it is identity, not chrome. Without it the series are distinguishable only
    ### by color, which is the one thing a figure may never rely on.
    ax.legend(frameon=False, fontsize=8.5, loc="center right")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


def _merge_runs(runs):
    """Reduce repetitions of one experiment to a median row per (topology, variant).

    Median rather than mean: a run that hit a stall or a slow compile is an outlier, and one of three
    should not drag the reported figure. Returns the merged frame and, per (topology, variant), the
    relative spread of `mean_tps` across runs -- printed so the reader can see how much of a
    variant-to-variant difference is real.
    """
    if len(runs) == 1:
        return runs[0], {}
    combined = pd.concat(runs, ignore_index=True)
    merged = combined.groupby(["topology", "variant"], as_index=False).median(numeric_only=True)
    spread = {}
    if "mean_tps" in combined.columns:
        for (topo, variant), g in combined.groupby(["topology", "variant"]):
            lo, hi = g["mean_tps"].min(), g["mean_tps"].max()
            spread[(topo, variant)] = (hi - lo) / hi if hi else 0.0
    return merged, spread


def main():
    ap = argparse.ArgumentParser(description="Plot distributed statistic-collection wire-bytes results.")
    ap.add_argument("results_dir", nargs="+",
                    help="directory holding summary.csv and <topology>/<variant>_traffic.csv. Pass several "
                         "to merge repetitions of the same experiment: each numeric column is reduced to "
                         "its MEDIAN across runs and the observed spread is printed.")
    ap.add_argument("--out-dir", default=None, help="where to write PNGs (default: the results dir)")
    ap.add_argument(
        "--charts", default="wire_bytes,throughput,traffic",
        help="comma-separated subset of charts to draw. A throughput run (GENERATOR_RATE=0) should pass "
             "--charts throughput: with the sources unthrottled each variant ingests a different amount, "
             "so its byte counts are NOT comparable across variants and must not be plotted.",
    )
    ap.add_argument(
        "--exclude", default="prometheus,local",
        help="comma-separated variants to leave out of BOTH charts (default: prometheus,local -- the raw "
             "stream and the no-wire floor, which bracket the comparison rather than take part in it). "
             "Pass an empty string to plot everything the summary holds.",
    )
    args = ap.parse_args()

    out_dir = args.out_dir or args.results_dir[0]
    os.makedirs(out_dir, exist_ok=True)
    runs = [pd.read_csv(os.path.join(d, "summary.csv")) for d in args.results_dir]
    df, spread = _merge_runs(runs)
    topologies = list(dict.fromkeys(df["topology"]))
    if len(runs) > 1:
        ### Persist what was actually plotted: the medians live nowhere else, and without this the
        ### figures could not be reproduced from the per-run summaries without redoing the merge.
        merged_path = os.path.join(out_dir, "summary_merged.csv")
        df.to_csv(merged_path, index=False)
        print(f"wrote {merged_path} (median of {len(runs)} runs)")
    excluded = {v.strip() for v in args.exclude.split(",") if v.strip()}
    charts = {c.strip() for c in args.charts.split(",") if c.strip()}
    unknown = charts - {"wire_bytes", "throughput", "traffic"}
    if unknown:
        ap.error(f"unknown chart(s): {', '.join(sorted(unknown))}")

    ### The table twin of the charts. It lists EVERY variant, excluded ones included: dropping a series
    ### from a chart is a framing choice, and the numbers behind it should stay one place away.
    has_tps = "mean_tps" in df.columns
    for topology in topologies:
        rows = df[df["topology"] == topology]
        base = rows[rows["variant"] == BASELINE]["total_bytes"]
        base = base.iloc[0] if len(base) else None
        base_tps = rows[rows["variant"] == BASELINE]["mean_tps"] if has_tps else []
        base_tps = base_tps.iloc[0] if len(base_tps) else None
        print(f"\ntopology {topology}"
              + (f"  (median of {len(runs)} runs)" if len(runs) > 1 else ""))
        header = f"  {'variant':<12} {'MB':>10} {'vs split':>10}"
        if has_tps:
            header += f" {'MTup/s':>9} {'vs split':>10} {'spread':>8}"
        print(header + "   plotted")
        for v in _ordered(list(rows["variant"])):
            b = rows[rows["variant"] == v]["total_bytes"].iloc[0]
            if not (base and b):
                factor = "-"
            else:
                factor = f"{base / b:.1f}x less" if b < base else (f"{b / base:.1f}x more" if b > base else "1.0x")
            line = f"  {v:<12} {b / 1e6:>10,.2f} {factor:>10}"
            if has_tps:
                t = rows[rows["variant"] == v]["mean_tps"].iloc[0] / 1e6
                rel = "-" if not (base_tps and t) else f"{(t / (base_tps / 1e6) - 1) * 100:+.0f}%"
                sp = spread.get((topology, v))
                line += f" {t:>9.2f} {rel:>10} {'-' if sp is None else f'{sp * 100:.0f}%':>8}"
            print(line + f"   {'no' if v in excluded else 'yes'}")
    print()

    for topology in topologies:
        # Only suffix the filenames when a run swept more than one topology, so the common
        # single-topology case keeps stable names.
        suffix = "" if len(topologies) == 1 else f"_{topology.replace('/', '-')}"
        variants = [v for v in _ordered(list(df[df["topology"] == topology]["variant"])) if v not in excluded]
        if not variants:
            print(f"every variant of topology {topology} is excluded, skipping")
            continue
        if "wire_bytes" in charts:
            plot_wire_bytes(df, topology, variants, os.path.join(out_dir, f"wire_bytes{suffix}.png"))
        if "throughput" in charts:
            plot_throughput(df, topology, variants, os.path.join(out_dir, f"throughput{suffix}.png"))
        if "traffic" in charts:
            plot_traffic_over_time(
                args.results_dir, topology, variants, os.path.join(out_dir, f"traffic_over_time{suffix}.png")
            )


if __name__ == "__main__":
    main()
