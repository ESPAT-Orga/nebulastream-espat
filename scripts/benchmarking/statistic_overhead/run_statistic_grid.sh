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

echo
echo "Result CSV: $(realpath "$OUTPUT_DIR")/results_statistic_grid.csv"
echo "Download it into the notebook's data folder with:"
echo "  scp <host>:$(realpath "$OUTPUT_DIR")/results_statistic_grid.csv \\"
echo "      scripts/benchmarking/statistic_overhead/plots/grid_data/"
echo "then run scripts/benchmarking/statistic_overhead/plots/StatisticGrid.ipynb."
