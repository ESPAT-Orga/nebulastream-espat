#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# statistic_grid pipeline: k ClusterMonitoring Q2 aggregations alongside j independent equi-width
# histogram queries, swept over both, at every configured ingestion rate. Writes
# results_statistic_grid.csv — the file StatisticGrid.ipynb reads.
#
# THREE sweeps land in that one csv: the grid itself, and two all-analytical reference arms at j=0 that
# bracket it — Q2 window aggregations (the lower limit, same query the grid runs) and ingest-only queries
# whose predicate matches nothing (the upper limit, the floor of per-query cost). See the BASELINE_* block at the bottom; set either
# variable to "" to skip that arm.
#
# NOTHING IS SPLICED here. Every query, analytical and statistic, owns its own TCP source, so j is
# not capped by k and the statistic queries pay for their own ingestion. That is a different
# measurement from run_statistic_overhead.sh (which splices and holds ingestion constant) — the two
# belong side by side in the paper, never as one series.
#
# `grep -c '^accept' <run>/generator.log` should read k+j at every point; anything less means a
# connection was refused, which shows up as clean-looking overhead rather than as an error.
#
# Must be run from the repository root, with NES_BUILD_DIR pointing at the Release build.
# Any arguments are forwarded to the runner.
#
#   # full sweep at the configured rates and grid, ~100 min with NUM_RUNS=3
#   NUM_RUNS=3 NES_BUILD_DIR=/home/nschubert/nes-build-statistic-overhead \
#     scripts/benchmarking/statistic_overhead/run_statistic_grid.sh \
#       --output-dir /home/nschubert/bench-grid-full --skip-build
#
#   # smoke: the two extreme corners at one rate, ~4 min
#   NUM_RUNS=1 NES_BUILD_DIR=/home/nschubert/nes-build-statistic-overhead \
#     scripts/benchmarking/statistic_overhead/run_statistic_grid.sh \
#       --analytical-counts 1 10 --statistic-counts 0 100 --tuples-per-sec 100000 \
#       --output-dir /home/nschubert/bench-grid-smoke2 --skip-build
#
#   # one rate only (the default sweeps every rate in GRID_TUPLES_PER_SEC_PER_QUERY)
#   ... run_statistic_grid.sh --tuples-per-sec 100000 --output-dir ... --skip-build
#
# Keep --output-dir OUTSIDE the synced source tree: CLion's rsync mirrors it with --delete and will
# remove results mid-run. Drop --skip-build to (re)build NebulaStream first.

set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-benchmark_run_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"

# Idempotent — docker reuses cached layers when the crate has not changed.
docker build -t "${GENERATOR_IMAGE:-nes-bench-gen:local}" rust-benchmark-generator

# Create a Python virtual environment and install the required libraries (runner + notebook execution).
python3 -m venv myenv
source myenv/bin/activate
trap 'deactivate 2>/dev/null || true; rm -rf myenv' EXIT
pip3 install --quiet argparse requests pandas pyyaml matplotlib seaborn nbconvert nbformat ipykernel

# --- bash -> python -> result csv -------------------------------------------------------------------------
myenv/bin/python3 -m scripts.benchmarking.statistic_overhead.run_statistic_grid \
    --output-dir "$OUTPUT_DIR" "$@" 2>&1 | tee "$OUTPUT_DIR/run.log"

# --- the all-analytical reference arm ---------------------------------------------------------------------
# k analytical queries and NO statistic query, which is the red line in the sustained figure. It answers the
# objection the grid alone cannot: is the drop at large j what a STATISTIC query costs, or what any extra
# query costs? Appended to the SAME csv rather than a second file — the keys are disjoint (this arm is j=0 at
# k values the grid never runs), so the sweep stays a one-file download and the notebook needs no merge step.
#
# The flags come AFTER "$@" on purpose: argparse keeps the last occurrence, so a smoke run's own
# --analytical-counts / --statistic-counts cannot leak into the arm. --skip-build likewise, because the first
# invocation has already built. Set BASELINE_ANALYTICAL_COUNTS="" to skip the arm entirely.
#
# k=1,5,10 are deliberately absent from the WINDOW arm: the grid's own j=0 cells ARE those points, so
# measuring them again would only add duplicate keys. The arm starts at k=2 (k=0 has no analytical query and
# therefore no throughput). The filter arm below gets no such freebie — the grid's j=0 cells are window
# queries — so it carries 1, 5, 10 itself.
BASELINE_ANALYTICAL_COUNTS="${BASELINE_ANALYTICAL_COUNTS:-2 50 60 70 80 90 100 150 200}"
if [ -n "$BASELINE_ANALYTICAL_COUNTS" ]; then
    echo
    echo "=== all-analytical reference arm: k = $BASELINE_ANALYTICAL_COUNTS, j = 0 ==="
    # Unquoted on purpose: the counts are a word-split list of ints.
    # shellcheck disable=SC2086
    myenv/bin/python3 -m scripts.benchmarking.statistic_overhead.run_statistic_grid \
        --output-dir "$OUTPUT_DIR" "$@" \
        --analytical-counts $BASELINE_ANALYTICAL_COUNTS --statistic-counts 0 \
        --analytical-query window --append --skip-build 2>&1 | tee -a "$OUTPUT_DIR/run.log"
fi

# --- the ingest reference arm -----------------------------------------------------------------------------
# The same arm with the window and GROUP BY removed AND a predicate nothing matches, so not one tuple is
# ever materialised: TCP receive, CSV parse, compare, drop. That is the floor of what a query costs here and
# therefore the UPPER limit of the band; the window arm above is the lower one.
#
# NOT a passthrough filter, which is what this arm used to be. A Void sink discards, but the pipeline still
# has to materialise every surviving tuple into an output buffer first, so SELECT * at 25% selectivity was
# costing ~7.5 Mtup/s of copying at k=150 and came out BELOW the grid — a histogram build is cheaper per
# tuple than a projection. Pass --analytical-query filter by hand to measure that mid-point arm.
#
# Defaults to the window arm's k values PLUS 1, 5 and 10, which the window arm gets free from the grid; set
# BASELINE_INGEST_COUNTS="" to skip it.
BASELINE_INGEST_COUNTS="${BASELINE_INGEST_COUNTS:-1 2 5 10 50 60 70 80 90 100 150 200}"
if [ -n "$BASELINE_INGEST_COUNTS" ]; then
    echo
    echo "=== ingest reference arm: k = $BASELINE_INGEST_COUNTS, j = 0 ==="
    # shellcheck disable=SC2086
    myenv/bin/python3 -m scripts.benchmarking.statistic_overhead.run_statistic_grid \
        --output-dir "$OUTPUT_DIR" "$@" \
        --analytical-counts $BASELINE_INGEST_COUNTS --statistic-counts 0 \
        --analytical-query ingest --append --skip-build 2>&1 | tee -a "$OUTPUT_DIR/run.log"
fi

echo
echo "Result CSV: $(realpath "$OUTPUT_DIR")/results_statistic_grid.csv"
echo "Download it into the notebook's data folder with:"
echo "  scp <host>:$(realpath "$OUTPUT_DIR")/results_statistic_grid.csv \\"
echo "      scripts/benchmarking/statistic_overhead/plots/grid_data/"
echo "then run scripts/benchmarking/statistic_overhead/plots/StatisticGrid.ipynb."
