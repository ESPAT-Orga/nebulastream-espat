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

#myenv/bin/python3 -m scripts.benchmarking.e2e.run_statistic_build --all --output-dir "$OUTPUT_DIR"

myenv/bin/python3 -m scripts.benchmarking.e2e.run_statistic_probe --all --output-dir "$OUTPUT_DIR"

# Deactivate the virtual environment
deactivate
rm -rf myenv
