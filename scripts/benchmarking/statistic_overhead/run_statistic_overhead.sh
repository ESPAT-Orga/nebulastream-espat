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

# statistic_overhead pipeline: sweeps how many of a fixed set of ClusterMonitoring Q2 aggregations
# carry a spliced equi-width histogram over their GROUP BY key, and writes
# results_statistic_overhead.csv.
#
# The histograms splice onto each query's own source, so the offered load — and the TCP connection
# count — are identical at every point of the sweep. `grep -c '^accept' <run>/generator.log` should
# read NUM_ANALYTICAL_QUERIES at every N; anything more means the splice did not take.
#
# Must be run from the repository root, with NES_BUILD_DIR pointing at the Release build.
# Any arguments are forwarded to the runner.
#
#   # smoke: both extremes, ~4 min
#   NES_BUILD_DIR=/home/nschubert/nes-build-statistic-overhead NUM_RUNS=1 \
#     scripts/benchmarking/statistic_overhead/run_statistic_overhead.sh \
#       --statistic-query-counts 10 0 --output-dir /home/nschubert/bench-smoke --skip-build
#
#   # calibration: find the highest offered load the baseline still fully sustains
#   NES_BUILD_DIR=/home/nschubert/nes-build-statistic-overhead NUM_RUNS=1 \
#     scripts/benchmarking/statistic_overhead/run_statistic_overhead.sh \
#       --statistic-query-counts 0 --tuples-per-sec 100000 200000 400000 800000 \
#       --output-dir /home/nschubert/bench-calib --skip-build
#
#   # main sweep at the calibrated operating point, ~1.5 h
#   NES_BUILD_DIR=/home/nschubert/nes-build-statistic-overhead \
#     scripts/benchmarking/statistic_overhead/run_statistic_overhead.sh \
#       --output-dir /home/nschubert/bench-sweep --skip-build
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
myenv/bin/python3 -m scripts.benchmarking.statistic_overhead.run_statistic_overhead \
    --output-dir "$OUTPUT_DIR" "$@" 2>&1 | tee "$OUTPUT_DIR/run.log"
