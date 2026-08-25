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

# Runs both figures of the histogram-delta wire experiment end to end, over the four payload
# representations: split (full synopsis), split_zstd, delta (sparse delta), delta_zstd.
#
#   1. BYTES  -- how much reaches the root per variant.
#      Sources are rate-limited (GENERATOR_RATE), which is what makes the counts comparable: every
#      variant ingests the same tuples, so a difference in bytes is a difference in representation.
#
#   2. THROUGHPUT -- how fast each variant ingests.
#      Sources are unthrottled so each variant reaches its own ceiling, and they are Memory sources so
#      the CSV parse happens before the query starts instead of on the hot path. Repeated REPS times;
#      the notebook takes the median.
#
# The two CANNOT come from one run: equalising ingest is what makes the bytes comparable, and NOT
# equalising it is what makes the throughput meaningful. Hence two experiments.
#
# Usage, from the repository root:
#   bash scripts/benchmarking/histogram_delta/distributed/run_histogram_delta_compression.sh
#
# Knobs (all optional):
#   OUT_ROOT=<dir>   where results land (default: ./benchmark_run_dsc_histogram_delta_<timestamp>)
#   REPS=<n>         throughput repetitions (default 3)
#   ONLY=bytes|throughput   run just one of the two
#   BUILD_DIR=<dir>  prebuilt worker tree; built on first use if absent
#
# Output layout, which plots/HistogramDeltaCompression.ipynb reads directly:
#   $OUT_ROOT/bytes/summary.csv              + bytes/1-1/<variant>_traffic.csv
#   $OUT_ROOT/throughput/run<N>/summary.csv

set -euo pipefail

REPO_ROOT="$(pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/run_benchmark.sh"

if [[ ! -f "$RUNNER" ]]; then
    echo "ERROR: $RUNNER not found; run this from the repository root." >&2
    exit 1
fi

OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/benchmark_run_dsc_histogram_delta_$(date +%Y%m%d_%H%M%S)}"
REPS="${REPS:-3}"
ONLY="${ONLY:-both}"
mkdir -p "$OUT_ROOT"

### Shared across both experiments so the two figures describe the same workload: the real
### ClusterMonitoring trace, 682 bins over taskId's true range, 60 s event-time windows, one leaf.
export DATASET="${DATASET:-cluster_monitoring}"
export TOPOLOGIES="${TOPOLOGIES:-1/1}"
export SOURCES_PER_LEAF="${SOURCES_PER_LEAF:-1}"
export HISTOGRAM_MEMORY_BUDGET="${HISTOGRAM_MEMORY_BUDGET:-16384}"
export HISTOGRAM_DELTA_KEYFRAME_INTERVAL="${HISTOGRAM_DELTA_KEYFRAME_INTERVAL:-10}"
export VARIANTS="${VARIANTS:-split,split_zstd,delta,delta_zstd}"
export MODES=traffic

echo "=================================================================="
echo "Output root : $OUT_ROOT"
echo "Variants    : $VARIANTS"
echo "Repetitions : $REPS (throughput)"
echo "=================================================================="

### --- 1. Bytes on the wire ------------------------------------------------------------------
### TCP sources at a fixed rate. RESULTS_COPY_DIR is pinned to OUTPUT_DIR so the runner does not also
### scatter copies into the suite's plots/ directory.
if [[ "$ONLY" == "both" || "$ONLY" == "bytes" ]]; then
    echo
    echo "### Experiment 1/2: bytes on the wire (rate-limited, equal ingest per variant)"
    OUTPUT_DIR="$OUT_ROOT/bytes" \
    RESULTS_COPY_DIR="$OUT_ROOT/bytes" \
    SOURCE_TYPE=tcp \
    DATASET_COPIES=1 \
    RUN_DURATION_SECONDS="${BYTES_DURATION:-60}" \
        bash "$RUNNER"
fi

### --- 2. Throughput -------------------------------------------------------------------------
### Memory sources, unthrottled. DATASET_COPIES=8 replays the trace 8x with each copy shifted by a
### whole number of windows: the raw trace drains in ~3.5 s, which is short enough that warm-up is a
### large share of the measured span. NUMBER_OF_BUFFERS covers the ~2.4 GB of pre-parsed tuples.
if [[ "$ONLY" == "both" || "$ONLY" == "throughput" ]]; then
    for rep in $(seq 1 "$REPS"); do
        echo
        echo "### Experiment 2/2: throughput, repetition $rep/$REPS (unthrottled, Memory source)"
        OUTPUT_DIR="$OUT_ROOT/throughput/run$rep" \
        RESULTS_COPY_DIR="$OUT_ROOT/throughput/run$rep" \
        SOURCE_TYPE=memory \
        DATASET_COPIES="${DATASET_COPIES:-8}" \
        NUMBER_OF_BUFFERS="${NUMBER_OF_BUFFERS:-600000}" \
        THROUGHPUT_LISTENER_INTERVAL_MS="${THROUGHPUT_LISTENER_INTERVAL_MS:-50}" \
        RUN_DURATION_SECONDS="${THROUGHPUT_DURATION:-120}" \
            bash "$RUNNER"
    done
fi

echo
echo "=================================================================="
echo "Done. Results in $OUT_ROOT"
echo
echo "Plot them with plots/HistogramDeltaCompression.ipynb, which reads exactly this layout:"
echo "  export RESULTS_DIR=$OUT_ROOT"
echo "  jupyter lab scripts/benchmarking/histogram_delta/distributed/plots/HistogramDeltaCompression.ipynb"
echo
echo "The notebook needs matplotlib + pandas; the sibling suite's venv already has them"
echo "(scripts/benchmarking/histogram_delta/.venv), though not jupyter itself. Either register"
echo "that venv as a kernel, or use any environment with matplotlib, pandas and jupyter."
echo "=================================================================="
