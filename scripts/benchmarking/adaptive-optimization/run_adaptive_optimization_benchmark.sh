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

# Comparison runner: deploys the native adaptive setup, the Prometheus SOTA baseline,
# and each fixed-variant baseline (bid_first, price_first) one after another, and
# collects all four throughput CSVs into a single results directory.
#   - adaptive   : native in-engine statistics drive the filter-order switch.
#   - prometheus : the SOTA baseline — a PrometheusSink builds the histogram, a real
#                  Prometheus scrapes it, and the coordinator poll loop drives the switch.
#   - bid_first / price_first : fixed orders with NO adaptation — they measure "the cost
#                  of running the wrong filter order without any system intervening".
# With --sqrts > 0 and the ALTERNATING workload, the throughput-over-time curves make the
# reaction-time difference visible directly: the slump after each regime flip is brief for
# `adaptive`, wider for `prometheus` (its scrape+rate+poll reaction), and never recovers for
# the fixed variants.
#
# All user-passed flags after the script name are forwarded to every individual run
# (so e.g. `--duration 60 --sqrts 150 --replays-per-file 120` applies uniformly).
# DO NOT pass `--fixed-variant`, `--baseline-prometheus`, `--baseline-poll-interval-ms`, or
# `--output` here — the wrapper sets those itself per run.
#
# The `prometheus` variant is swept across the poll intervals in POLL_INTERVALS (1000/5000/10000 ms)
# below, producing one CSV per interval (prometheus_<ms>.csv). It is the ONLY variant with a
# coordinator poll loop, so `adaptive` (in-engine gated probes) and the fixed variants ignore the
# interval and run exactly once.
#
# Pass `--results-dir <path>` to override the output directory. The directory
# is REMOVED and recreated on every invocation so old CSVs can't leak between
# experiments — the directory's contents always reflects exactly one run of
# this script.

set -euo pipefail

# Default results directory; override with --results-dir <path>.
RESULTS_DIR="results/adaptive_comparison"

# Strip --results-dir <path> out of "$@" before forwarding to the Python script.
FORWARDED_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --results-dir)
            RESULTS_DIR="$2"
            shift 2
            ;;
        --results-dir=*)
            RESULTS_DIR="${1#*=}"
            shift
            ;;
        --fixed-variant|--baseline-prometheus|--baseline-poll-interval-ms|--output)
            echo "Error: $1 is set per-run by this wrapper; do not pass it on the command line." >&2
            exit 1
            ;;
        *)
            FORWARDED_ARGS+=("$1")
            shift
            ;;
    esac
done

# Wipe + recreate the results dir so old artifacts can't mask a regression.
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"
echo "[wrapper] Results dir: $RESULTS_DIR (clean)"

# Create a Python virtual environment and install the required python libraries
python3 -m venv myenv
source myenv/bin/activate
pip3 install argparse requests pandas pyyaml numpy

# `python -m` rejects hyphens in module paths (`adaptive-optimization` isn't a valid identifier),
# so run the script by file path. The script appends the repo root to sys.path itself, which is
# why this works without setting PYTHONPATH.
PYBIN="myenv/bin/python3"
PYSCRIPT="scripts/benchmarking/adaptive-optimization/run_adaptive_optimization_benchmark.py"

# Poll intervals (ms) to sweep for the prometheus variant. Only the prometheus baseline has a
# coordinator poll loop, so this sweep applies to it alone.
POLL_INTERVALS=(1000 5000 10000)

# Each run also redirects its console output to a log file alongside the CSV
# so failures can be diagnosed without re-running.
# Each element is on its own line (no `\` continuation) so any variant can be
# commented out independently without breaking the list.
#
# Non-prometheus variants: run ONCE each. The poll interval is inert for them (it's only read in
# the Python script's --baseline-prometheus branch), so we don't pass it.
single_run_specs=(
    "adaptive::adaptive.csv"
    "bid_first:--fixed-variant bid_first:bid_first.csv"
    "price_first:--fixed-variant price_first:price_first.csv"
)
for variant_spec in "${single_run_specs[@]}"; do
    IFS=':' read -r label variant_flag csv_name <<<"$variant_spec"
    echo "[wrapper] --- Running variant: $label ---"
    csv_path="$RESULTS_DIR/$csv_name"
    log_path="$RESULTS_DIR/${label}.log"
    # `$variant_flag` deliberately unquoted so an empty value contributes no arg
    # and "--fixed-variant bid_first" expands to two args.
    # shellcheck disable=SC2086
    "$PYBIN" "$PYSCRIPT" --sqrts 200 --duration 120 $variant_flag --output "$csv_path" "${FORWARDED_ARGS[@]}" 2>&1 | tee "$log_path"
    echo "[wrapper] --- Done: $label (csv=$csv_path, log=$log_path) ---"
done

# Prometheus SOTA baseline: sweep the coordinator poll interval. Each interval gets its own
# CSV (prometheus_<ms>.csv) + log so the reaction-time curves can be compared across polling
# granularities.
for poll_ms in "${POLL_INTERVALS[@]}"; do
    label="prometheus_${poll_ms}ms"
    echo "[wrapper] --- Running variant: $label (poll-interval=${poll_ms}ms) ---"
    csv_path="$RESULTS_DIR/prometheus_${poll_ms}.csv"
    log_path="$RESULTS_DIR/${label}.log"
    "$PYBIN" "$PYSCRIPT" --sqrts 200 --duration 120 --baseline-prometheus --baseline-poll-interval-ms "$poll_ms" --output "$csv_path" "${FORWARDED_ARGS[@]}" 2>&1 | tee "$log_path"
    echo "[wrapper] --- Done: $label (csv=$csv_path, log=$log_path) ---"
done

# Deactivate the virtual environment
deactivate
rm -rf myenv

echo "[wrapper] All runs complete (${#single_run_specs[@]} single + ${#POLL_INTERVALS[@]} prometheus sweep). Results in: $RESULTS_DIR"
ls -la "$RESULTS_DIR"
