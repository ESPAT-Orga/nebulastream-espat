#!/usr/bin/env bats

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
# Smoke test for the network-sink benchmark. Runs scripts/benchmarking/network_sink/run_network_benchmark.sh
# with a minimal sweep (one trial point) to confirm the benchmark plumbing works end to end:
#   * two single-node-workers spawn,
#   * one HIGH and one LOW priority query are submitted via nes-cli,
#   * the queries run for the configured duration,
#   * results_network_sink.csv is produced with at least one row per (priority).
#
# This fixture is intentionally thin: full sweeps are run directly via the bash wrapper, not via bats.

setup_file() {
  if [ -z "${NES_DIR:-}" ]; then
    echo "ERROR: NES_DIR environment variable must be set" >&2
    exit 1
  fi
  if [ ! -x "${NES_DIR}/build_dir/nes-single-node-worker/nes-single-node-worker" ]; then
    echo "ERROR: built nes-single-node-worker not found under \$NES_DIR/build_dir; build first" >&2
    exit 1
  fi
  if [ ! -x "${NES_DIR}/build_dir/nes-frontend/apps/nes-cli" ]; then
    echo "ERROR: built nes-cli not found under \$NES_DIR/build_dir; build first" >&2
    exit 1
  fi
}

@test "network-sink benchmark smoke: ALWAYS_SEND, FIXED, no throttle" {
  cd "$NES_DIR"
  out_dir="$(mktemp -d)"
  run bash scripts/benchmarking/network_sink/run_network_benchmark.sh "$out_dir" \
    --num-runs 1 \
    --strategies ALWAYS_SEND \
    --rate-types FIXED \
    --max-network-rates none
  echo "stdout:"
  echo "$output"
  [ "$status" -eq 0 ]
  [ -f "$out_dir/results_network_sink.csv" ]
  # At least the header plus two data rows (HIGH + LOW).
  line_count=$(wc -l < "$out_dir/results_network_sink.csv")
  [ "$line_count" -ge 3 ]
}

@test "network-sink benchmark smoke: ADAPTIVE_DIFFERENT_PRIO, FIXED, no throttle" {
  cd "$NES_DIR"
  out_dir="$(mktemp -d)"
  run bash scripts/benchmarking/network_sink/run_network_benchmark.sh "$out_dir" \
    --num-runs 1 \
    --strategies ADAPTIVE_DIFFERENT_PRIO \
    --rate-types FIXED \
    --max-network-rates none
  echo "stdout:"
  echo "$output"
  [ "$status" -eq 0 ]
  [ -f "$out_dir/results_network_sink.csv" ]
  line_count=$(wc -l < "$out_dir/results_network_sink.csv")
  [ "$line_count" -ge 3 ]
}
