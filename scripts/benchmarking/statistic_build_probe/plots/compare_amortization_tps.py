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

"""Compare the per-run-averaged tuplesPerSecond between two amortization result CSVs.

Both files share the same parameter schema. For every parameter combination we
average the metric across its runs (run_idx) in each file, then report the
difference of those per-run averages (current - baseline).

Default: baseline = results_synopsis_amortization.csv.bak
         current  = results_synopsis_amortization.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent

# Columns that identify a single measurement run rather than a parameter, plus
# the per-run outputs. Everything else is treated as a grouping parameter.
NON_PARAM_COLS = {"run_idx", "build_duration_s", "issue"}


def resolve_metric(columns, requested):
    """Return the actual column for `requested`, allowing a partial/prefix match."""
    if requested in columns:
        return requested
    matches = [c for c in columns if requested in c]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        sys.exit(f"error: no column matching '{requested}' in {list(columns)}")
    sys.exit(f"error: '{requested}' is ambiguous, matches {matches}; pass --metric explicitly")


def per_group_mean(path, metric, group_cols):
    """Average `metric` across runs for each parameter group in one file."""
    df = pd.read_csv(path)
    df[metric] = pd.to_numeric(df[metric], errors="coerce")
    # Drop rows without a valid measurement (e.g. exception/timeout/crashed runs).
    df = df.dropna(subset=[metric])
    grouped = df.groupby(group_cols, dropna=False)[metric]
    out = grouped.agg(mean="mean", runs="count").reset_index()
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--current", type=Path, default=HERE / "results_synopsis_amortization.csv",
                        help="newer results file (default: results_synopsis_amortization.csv)")
    parser.add_argument("--baseline", type=Path, default=HERE / "results_synopsis_amortization.csv.bak",
                        help="baseline results file (default: .csv.bak)")
    parser.add_argument("--metric", default="tuplesPerSecond",
                        help="metric column (partial match ok; default: tuplesPerSecond -> tuplesPerSecond_listener)")
    parser.add_argument("--out", type=Path, default=None,
                        help="optional path to write the per-group diff table as CSV")
    parser.add_argument("--top", type=int, default=20,
                        help="how many largest-magnitude diffs to print (default: 20)")
    args = parser.parse_args()

    cur_cols = pd.read_csv(args.current, nrows=0).columns
    bak_cols = pd.read_csv(args.baseline, nrows=0).columns
    if list(cur_cols) != list(bak_cols):
        sys.exit(f"error: column schemas differ\n  current : {list(cur_cols)}\n  baseline: {list(bak_cols)}")

    metric = resolve_metric(cur_cols, args.metric)
    group_cols = [c for c in cur_cols if c not in NON_PARAM_COLS and c != metric]
    if args.metric != metric:
        print(f"Using metric column '{metric}' for requested '{args.metric}'.")
    print(f"Grouping parameters: {group_cols}\n")

    cur = per_group_mean(args.current, metric, group_cols)
    bak = per_group_mean(args.baseline, metric, group_cols)

    merged = cur.merge(bak, on=group_cols, how="outer", suffixes=("_current", "_baseline"), indicator=True)

    only_current = merged[merged["_merge"] == "left_only"]
    only_baseline = merged[merged["_merge"] == "right_only"]
    common = merged[merged["_merge"] == "both"].copy()

    common["diff"] = common["mean_current"] - common["mean_baseline"]
    common["pct_diff"] = common["diff"] / common["mean_baseline"] * 100.0

    # Report. ----------------------------------------------------------------
    print(f"Parameter groups: {len(common)} common, "
          f"{len(only_current)} only in current, {len(only_baseline)} only in baseline\n")

    if not common.empty:
        print("=== Per-group difference of per-run averages (current - baseline) ===")
        label_col = "query_name" if "query_name" in group_cols else None
        show = common.copy()
        show = show.reindex(show["diff"].abs().sort_values(ascending=False).index)
        cols = ([label_col] if label_col else group_cols) + [
            "mean_baseline", "mean_current", "diff", "pct_diff", "runs_baseline", "runs_current"]
        with pd.option_context("display.max_rows", None, "display.width", 200,
                               "display.float_format", lambda v: f"{v:,.2f}"):
            print(show.head(args.top)[cols].to_string(index=False))
        if len(show) > args.top:
            print(f"... ({len(show) - args.top} more groups; use --top to show more or --out to write all)")

        print("\n=== Summary across common parameter groups ===")
        print(f"  groups compared          : {len(common)}")
        print(f"  mean diff (current-base) : {common['diff'].mean():,.2f} tuples/s")
        print(f"  median diff              : {common['diff'].median():,.2f} tuples/s")
        print(f"  mean |diff|              : {common['diff'].abs().mean():,.2f} tuples/s")
        print(f"  mean pct diff            : {common['pct_diff'].mean():+.2f} %")
        print(f"  median pct diff          : {common['pct_diff'].median():+.2f} %")
        print(f"  groups current > baseline: {(common['diff'] > 0).sum()}")
        print(f"  groups current < baseline: {(common['diff'] < 0).sum()}")

    if not only_baseline.empty or not only_current.empty:
        print("\n=== Parameter groups present in only one file ===")
        lbl = "query_name" if "query_name" in group_cols else group_cols[0]
        if not only_current.empty:
            print(f"  only in current ({len(only_current)}): "
                  f"{sorted(only_current[lbl].dropna().unique().tolist())[:10]}{' ...' if len(only_current) > 10 else ''}")
        if not only_baseline.empty:
            print(f"  only in baseline ({len(only_baseline)}): "
                  f"{sorted(only_baseline[lbl].dropna().unique().tolist())[:10]}{' ...' if len(only_baseline) > 10 else ''}")

    if args.out is not None:
        common.drop(columns="_merge").to_csv(args.out, index=False)
        print(f"\nWrote per-group diff table to {args.out}")


if __name__ == "__main__":
    main()
