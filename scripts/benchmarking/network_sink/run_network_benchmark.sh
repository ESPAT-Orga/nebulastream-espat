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
#
# Drives scripts.benchmarking.network_sink.run_network_benchmark, which sweeps:
#   * caps               (config.py:EXPERIMENT_CAPS — one twin grid figure per cap)
#   * HIGH staircase     (config.py:HIGH_STEP_CONFIGS — square-wave high_frac↔low_frac)
#   * LOW emit rate      (config.py:LOW_EMIT_RATES — frac_of_cap; cooperative vs. greedy)
#   * sending strategy   (config.py:EXPERIMENT_STRATEGIES — ALWAYS_SEND, ADAPTIVE_DIFFERENT_PRIO,
#                         and HIGH_ALONE as a no-harm reference run once per (cap, HIGH config))
#
# For every cell of (cap × HIGH × LOW × strategy) the driver runs one trial: spawn worker-1 and
# worker-2 with the strategy, apply tc throttle, submit one HIGH STEP query and one LOW FIXED
# query, sleep for the trial duration, parse the worker logs for BufferIngest / Throughput /
# Latency events, and append per-priority time-series rows to the consolidated CSVs.
#
# The notebook in plots/ then renders one twin figure per cap (ADAPTIVE vs. ALWAYS_SEND) on the
# (HIGH × LOW) grid; HIGH_ALONE is overlaid faintly per row to defend the no-harm claim.
#
# Design heritage:
#   * Credit-based flow control (Apache Flink, ATM) — pending-acks credit window between sender
#     and receiver. Foundation for the per-channel rate limit consulted by ADAPTIVE_DIFFERENT_PRIO.
#   * Hierarchical Token Bucket (HTB; Linux `tc qdisc htb`, Devera) and Weighted Fair Queueing
#     (WFQ / GPS; Demers/Keshav/Shenker SIGCOMM 1989, Parekh/Gallager IEEE/ACM ToN 1993) — formal
#     models for class-weighted bandwidth share with work conservation. The next iteration of
#     ADAPTIVE replaces the strict-priority gate with an HTB-style scheduler so LOW gets a
#     guaranteed liveness floor (DiffServ AF style; RFC 2597) instead of starving when HIGH
#     continuously oversubscribes.
#   * Cameo (Xu et al., NSDI 2021) and Henge (Kalim et al., SoCC 2018) — streaming-system prior
#     art on priority/SLO-aware scheduling; Cameo propagates deadlines per message, this work
#     propagates priority per channel and uses the engine's own backpressure as the gating signal.
#
# tc qdisc throttling on the loopback interface requires CAP_NET_ADMIN. The benchmark uses
# `sudo -n tc ...`; configure passwordless sudo for `tc` if you want the throttling sweep to take
# effect, otherwise pass `--caps none` to skip throttling.

# Must run from the repository root so the relative build-dir path resolves and the
# scripts.benchmarking.* package imports work.
for marker in nes-sources nes-sql-parser nes-systests; do
  if [ ! -d "$marker" ]; then
    echo "ERROR: $0 must be executed from the repository root (missing: $marker/)." >&2
    echo "       cwd = $(pwd)" >&2
    exit 1
  fi
done

# Build artifact location. Override NES_BUILD_DIR to point at a remote-mounted or symlinked build
# (e.g. NES_BUILD_DIR=/path/to/cmake-build-debug ...). The Python driver reads the same variable and,
# unless --skip-build is passed, runs cmake configure + ninja build into it.
export NES_BUILD_DIR="${NES_BUILD_DIR:-./build_dir}"

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
