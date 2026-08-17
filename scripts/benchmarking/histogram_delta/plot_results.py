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

"""Plot the histogram-delta throughput benchmark results.

Reads results_histogram_delta_throughput.csv and writes PNGs:
  - throughput_vs_threads.png : throughput scaling, plain vs delta@{N}
  - throughput_vs_keyframe.png: throughput vs keyframe interval N (plain = flat reference)
  - overhead_vs_threads.png   : delta throughput overhead (%) vs plain, per N

Colors use the Okabe-Ito colorblind-safe categorical palette (fixed order); one y-axis; a legend for
>=2 series; thin lines; recessive grid.
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
DELTA_N_COLORS = [ORANGE, GREEN, VERMILLION, PURPLE, SKY]  # assigned to N values in ascending order


def _style(ax):
    ax.grid(True, color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(MUTED)
    ax.tick_params(colors=MUTED)


def _median(df, variant, threads=None, N=None):
    q = df[df["variant"] == variant]
    if threads is not None:
        q = q[q["threads"] == threads]
    if N is not None:
        q = q[q["keyframe_interval"] == N]
    return q["tuples_per_second"].median() / 1e6 if len(q) else None


def _representative_N(df):
    Ns = sorted(df[df["variant"] == "delta"]["keyframe_interval"].unique())
    return 10 if 10 in Ns else Ns[len(Ns) // 2]


def plot_vs_threads(df, out):
    """Plain vs delta throughput across threads. N barely affects throughput (see the keyframe plot),
    so we draw a single representative delta line to keep the scaling + overhead story uncluttered."""
    threads = sorted(df["threads"].unique())
    n = _representative_N(df)

    fig, ax = plt.subplots(figsize=(7.6, 4.6))
    _style(ax)
    yp = [_median(df, "plain", threads=t) for t in threads]
    ax.plot(threads, yp, color=BLUE, lw=2, marker="o", ms=7, label="plain histogram", zorder=5)
    ax.annotate("plain", (threads[-1], yp[-1]), color=BLUE, fontsize=9, weight="bold",
                xytext=(6, 0), textcoords="offset points", va="center")
    yd = [_median(df, "delta", threads=t, N=n) for t in threads]
    ax.plot(threads, yd, color=VERMILLION, lw=2, marker="s", ms=6, label=f"delta (N={n})", zorder=4)
    ax.annotate(f"delta", (threads[-1], yd[-1]), color=VERMILLION, fontsize=9, weight="bold",
                xytext=(6, 0), textcoords="offset points", va="center")

    ax.set_xscale("log", base=2)
    ax.set_xticks(threads)
    ax.set_xticklabels([str(t) for t in threads])
    ax.set_xlabel("worker threads", color=INK)
    ax.set_ylabel("throughput (M tuples/s)", color=INK)
    ax.set_ylim(bottom=0)
    ax.set_xlim(right=threads[-1] * 1.25)  # room for the end labels
    ax.set_title("Delta throughput scales with threads, at a small constant overhead vs plain",
                 color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, loc="upper left", fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


def plot_vs_keyframe(df, out, thread_choices=(4, 16)):
    Ns = sorted(df[df["variant"] == "delta"]["keyframe_interval"].unique())
    thread_choices = [t for t in thread_choices if t in set(df["threads"])]
    fig, ax = plt.subplots(figsize=(7, 4.5))
    _style(ax)
    colors = [GREEN, VERMILLION, ORANGE, PURPLE]
    for i, t in enumerate(thread_choices):
        c = colors[i % len(colors)]
        yd = [_median(df, "delta", threads=t, N=n) for n in Ns]
        ax.plot(Ns, yd, color=c, lw=2, marker="s", ms=6, label=f"delta @ {t} threads", zorder=5)
        ax.annotate(f"{t} thr", (Ns[-1], yd[-1]), color=c, fontsize=9,
                    xytext=(6, 0), textcoords="offset points", va="center")
        # plain reference (flat dashed) at same thread count
        p = _median(df, "plain", threads=t)
        if p is not None:
            ax.axhline(p, color=c, lw=1, ls=":", alpha=0.7, zorder=3)
            ax.annotate(f"plain {t} thr", (Ns[0], p), color=c, fontsize=8, alpha=0.9,
                        xytext=(0, 4), textcoords="offset points", va="bottom")

    ax.set_xscale("log")
    ax.set_xticks(Ns)
    ax.set_xticklabels([str(n) for n in Ns])
    ax.set_xlabel("keyframe interval N", color=INK)
    ax.set_ylabel("throughput (M tuples/s)", color=INK)
    ax.set_ylim(bottom=0)
    ax.set_xlim(right=Ns[-1] * 1.6)  # room for end labels
    ax.set_title("Keyframe interval N barely affects throughput (dotted = plain)",
                 color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, loc="lower right", fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


def plot_overhead(df, out):
    threads = sorted(df["threads"].unique())
    Ns = sorted(df[df["variant"] == "delta"]["keyframe_interval"].unique())
    fig, ax = plt.subplots(figsize=(7, 4.5))
    _style(ax)
    for i, n in enumerate(Ns):
        c = DELTA_N_COLORS[i % len(DELTA_N_COLORS)]
        y = []
        for t in threads:
            p = _median(df, "plain", threads=t)
            d = _median(df, "delta", threads=t, N=n)
            y.append((1 - d / p) * 100 if (p and d) else None)
        ax.plot(threads, y, color=c, lw=2, marker="s", ms=6, label=f"N={n}", zorder=4)
    ax.set_xscale("log", base=2)
    ax.set_xticks(threads)
    ax.set_xticklabels([str(t) for t in threads])
    ax.set_xlabel("worker threads", color=INK)
    ax.set_ylabel("delta throughput overhead vs plain (%)", color=INK)
    ax.set_ylim(bottom=0)
    ax.set_title("Delta throughput overhead vs plain (roughly constant)",
                 color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, loc="upper right", fontsize=9, title="keyframe N", title_fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


def main():
    ap = argparse.ArgumentParser(description="Plot histogram-delta throughput benchmark results.")
    ap.add_argument("csv", help="results_histogram_delta_throughput.csv")
    ap.add_argument("--out-dir", default=None, help="where to write PNGs (default: alongside the csv)")
    args = ap.parse_args()
    out_dir = args.out_dir or os.path.dirname(os.path.abspath(args.csv))
    df = pd.read_csv(args.csv)
    plot_vs_threads(df, os.path.join(out_dir, "throughput_vs_threads.png"))
    plot_vs_keyframe(df, os.path.join(out_dir, "throughput_vs_keyframe.png"))
    plot_overhead(df, os.path.join(out_dir, "overhead_vs_threads.png"))


if __name__ == "__main__":
    main()
