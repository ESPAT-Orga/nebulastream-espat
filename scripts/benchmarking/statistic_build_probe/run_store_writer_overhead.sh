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

# Store-writer overhead pipeline: runs the benchmark (produces results_store_writer_overhead.csv) and then
# executes the StoreWriterOverhead notebook to render the overhead plots as PDFs.
#
# Any arguments are forwarded to run_store_writer_overhead.py, e.g. for a quick smoke run:
#   scripts/benchmarking/statistic_build_probe/run_store_writer_overhead.sh \
#       -q Nexmark --build-dataset /path/to/small.csv --aggregations Sum Count CountMin \
#       --memory-budgets 1024 --window-sizes 5 --worker-threads 1 --num-runs 1 --skip-build

set -euo pipefail

OUTPUT_DIR="benchmark_run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"

# Create a Python virtual environment and install the required libraries (runner + notebook execution).
python3 -m venv myenv
source myenv/bin/activate
pip3 install --quiet argparse requests pandas pyyaml matplotlib seaborn nbconvert nbformat ipykernel

# --- bash -> python -> result csv -------------------------------------------------------------------------
myenv/bin/python3 -m scripts.benchmarking.statistic_build_probe.run_store_writer_overhead --output-dir "$OUTPUT_DIR" "$@"


deactivate
rm -rf myenv
