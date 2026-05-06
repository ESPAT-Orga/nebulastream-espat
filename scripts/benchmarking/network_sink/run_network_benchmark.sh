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

# Network-sink benchmark wrapper.
# Drives scripts.benchmarking.network_sink.run_network_benchmark, which spawns two single-node-workers,
# submits a HIGH-priority and a LOW-priority generator → Network-sink query, and records per-query
# throughput into results_network_sink.csv.
#
# Optional: tc qdisc throttling on the loopback interface requires CAP_NET_ADMIN. The benchmark uses
# `sudo -n tc ...`; configure passwordless sudo for `tc` if you want the throttling sweep dimension to
# take effect, otherwise the runs proceed unthrottled with a warning.

OUTPUT_DIR="${1:-network_sink_run_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"

# Ensure a Python venv exists with required packages.
if [ ! -d "myenv" ]; then
  python3 -m venv myenv
  myenv/bin/pip install --quiet pyyaml
fi

myenv/bin/python3 -m scripts.benchmarking.network_sink.run_network_benchmark \
    --output-dir "$OUTPUT_DIR" \
    "${@:2}"
