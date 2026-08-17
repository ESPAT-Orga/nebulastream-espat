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
#
# Histogram delta-compression THROUGHPUT benchmark.
#   - Compares plain EQUIWIDTHHISTOGRAM vs the delta split (EQUIWIDTHHISTOGRAMDELTA),
#     sweeping worker threads and the keyframe interval N.
#   - Data: the real Google ClusterMonitoring 1 GB trace (histogram over taskId, windowed on the real
#     creationTS event time). The driver auto-downloads + projects it on first run (see
#     prepare_cluster_monitoring.py); pass --synthetic to fall back to the uniform-random generator.
#   - Single-node: measures the delta machinery's CPU/throughput overhead and its multi-thread scaling,
#     NOT network byte savings. Those come from scripts/benchmarking/distributed_statistic_collection.
#
# Run from the repo root. Set NES_BUILD_DIR if the build is not ./build_dir (e.g. cmake-build-debug).
set -euo pipefail

OUTPUT_DIR="benchmark_run_histogram_delta_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"

# Plotting deps live in a uv-managed venv (pyproject.toml + uv.lock in this dir). The driver itself is
# stdlib-only, so it runs under the system python3; only the plot step needs matplotlib + pandas.
PLOT_VENV="$(dirname "$0")/.venv"
uv sync --project "$(dirname "$0")" --quiet
PLOT_PY="$PLOT_VENV/bin/python"

# Throughput sweep (real ClusterMonitoring trace auto-prepared on first run into
# <build>/working_dir/cluster_monitoring.csv; 60 s windows so the real event time is gap-free).
# Tune: --threads, --keyframe-intervals, --runs, --window-size. Keep the buffer pool modest (set in the
# driver) so a stray worker cannot OOM the host.
python3 -m scripts.benchmarking.histogram_delta.run_histogram_delta_throughput \
    --output-dir "$OUTPUT_DIR" \
    --threads 1 2 4 8 16 \
    --keyframe-intervals 1 2 5 10 50 \
    --runs 3 --warmup 1

# Plots (throughput vs threads, throughput vs N, overhead vs threads).
"$PLOT_PY" -m scripts.benchmarking.histogram_delta.plot_results \
    "$OUTPUT_DIR/results_histogram_delta_throughput.csv"

echo "Done. CSV + PNGs in $(realpath "$OUTPUT_DIR")"
