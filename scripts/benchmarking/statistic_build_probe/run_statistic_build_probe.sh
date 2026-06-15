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

# Create a timestamped output directory for this experiment run
OUTPUT_DIR="benchmark_run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"

# Create a Python virtual environment and install the required python libraries
python3 -m venv myenv
source myenv/bin/activate
pip3 install argparse requests pandas pyyaml

# Synopsis build / amortization (throughput) — writes results_synopsis_amortization.csv.
# One build query maintaining N synopses; the num_synopses == 1 rows are the single-synopsis build
# throughputs (Sum / Passthrough / Reservoir / CountMin / EquiWidthHistogram) and num_synopses > 1 the
# CountMin / EquiWidthHistogram scaling curve. The StatisticBuild notebook reads this CSV directly.
#
# === LARGE-DATA RUN (active) ==================================================
# ClusterMonitoring (1 GB, F=10) + Manufacturing (1 GB, F=13) only — drops the 6 GB Nexmark to keep the
# wall-clock to a few hours. Both budgets {1,10}kB, both thread counts {1,16}, full N-ladder per dataset,
# one run each, window sizes 1/5/10 (~288 trials). No --skip-build, so on first run it configures + builds
# NES into ./build_dir AND the ENABLE_LARGE_TESTS=1 configure downloads the large datasets (resolving the
# .md5 placeholders); subsequent runs reuse ./build_dir. No --build-dataset → configured paths.
#myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_synopsis_amortization --queries ClusterMonitoring Manufacturing --memory-budgets 1024 10240 --window-sizes 1 10 --worker-threads 1 16 --num-runs 3 --output-dir "$OUTPUT_DIR"
myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_synopsis_amortization --memory-budgets 5120 --window-sizes 1 10 --worker-threads 1 16 --num-runs 3 --output-dir "$OUTPUT_DIR"
#myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_synopsis_amortization --memory-budgets 1024 5120 10240 --worker-threads 1 16 --num-runs 3 --output-dir "$OUTPUT_DIR"



# Accuracy — writes results_statistic_accuracy.csv.
#myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_statistic_accuracy --all --output-dir "$OUTPUT_DIR" --statistic-store-types SUB_STORES

# Probe (probe throughput) — writes results_statistic_probe.csv
#myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_statistic_probe    --all --output-dir "$OUTPUT_DIR" --statistic-store-types SUB_STORES
#myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_statistic_probe  --queries ClusterMonitoring --output-dir "$OUTPUT_DIR" --statistic-store-types WINDOW --statistic-types Reservoir EquiWidthHistogram CountMin --memory-budgets 1024 10240 --worker-threads 1 --num-runs 1
#myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_statistic_probe  --queries ClusterMonitoring --output-dir "$OUTPUT_DIR" --statistic-store-types WINDOW --worker-threads 1 16 --num-runs 3
#myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_statistic_probe  --all --output-dir "$OUTPUT_DIR" --statistic-store-types WINDOW --worker-threads 1 16 --num-runs 3

# Deactivate the virtual environment
deactivate
rm -rf myenv
