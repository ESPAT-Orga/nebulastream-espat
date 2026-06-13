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

set -euo pipefail

OUTPUT_DIR="benchmark_run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"

DOCKER="docker run --rm --workdir /tmp/espat-1 -v $(pwd):/tmp/espat-1"

# Step 1: Rebuild nes-single-node-worker with the current source (incremental).
$DOCKER nebulastream/nes-development:local \
    cmake --build cmake-build-release -j --target nes-single-node-worker

# Step 2: Probe benchmark — for each trial, runs a build query to populate the
# statistic store, then a probe query and records throughput / latency.
# Parameters match the original StatisticProbe experiment:
#   dataset       : ClusterMonitoring
#   statistic types: Reservoir, EquiWidthHistogram, CountMin
#   memory budgets : 1024, 5120, 10240 bytes  (from configs.py)
#   worker threads : 16
#   window size    : 1 s  (single value, others are held constant)
#   store type     : DEFAULT
#   statistic IDs  : 1    (allNumStatisticIds = [1] in configs.py)
#   windows/probe  : 1, 100 (allBuildWindowsPerProbeWindow, always swept)
#   runs           : 3
$DOCKER \
    -e NES_BUILD_DIR=cmake-build-release \
    nebulastream/nes-development:local \
    sh -c "
        python3 -m venv /tmp/bench_venv \
        && /tmp/bench_venv/bin/pip install --quiet pyyaml requests pandas \
        && /tmp/bench_venv/bin/python3 -m scripts.benchmarking.e2e.run_statistic_probe \
            --queries ClusterMonitoring \
            --statistic-types Reservoir EquiWidthHistogram CountMin \
            --worker-threads 16 \
            --window-sizes 1 60 \
            --statistic-store-types DEFAULT \
            --output-dir $OUTPUT_DIR \
            --skip-build
    "

echo ""
echo "Results written to $OUTPUT_DIR/results_statistic_probe.csv"
echo "Generate the plot with:"
echo "  python3 scripts/benchmarking/e2e/plots/plot_statistic_probe.py $OUTPUT_DIR/results_statistic_probe.csv"
