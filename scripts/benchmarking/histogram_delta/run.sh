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
# Histogram delta compression -- the two wire measurements, over the four payload representations
# (split, split_zstd, delta, delta_zstd) on a 2-node Docker topology:
#
#   1. BYTES      -- how much reaches the root per variant. The BENEFIT of delta compression.
#   2. THROUGHPUT -- how fast the worker ingests per variant, from the engine's throughput listener.
#                    The COST.
#
# The two CANNOT come from one run. Byte counts are only comparable when every variant ingests the
# SAME tuples, which is why experiment 1 rate-limits its sources -- and under a rate limit the
# throughput listener merely reports the limiter back. Experiment 2 therefore runs unthrottled, which
# in turn makes its byte counts incomparable. Hence two experiments into one output root.
#
# Experiment 2 uses a Memory source: MemorySource::setup() parses the CSV into TupleBuffers BEFORE the
# query starts and then hands them out zero-copy, so transport and text parsing leave the measurement.
# With a TCP source the number would be whole-pipeline and could be source-bound rather than telling
# you anything about the worker.
#
# Data: the real Google ClusterMonitoring 1 GB trace (histogram over taskId, windowed on its real
# creationTS event time). Downloaded and projected on first use, then cached.
#
# Usage, from the repository root:
#   bash scripts/benchmarking/histogram_delta/run.sh
#   bash scripts/benchmarking/histogram_delta/run.sh --smoke   # wiring check, unusable numbers
#
# Knobs (all optional):
#   OUT_ROOT=<dir>              where results land (default ./benchmark_run_histogram_delta_<ts>)
#   REPS=<n>                    throughput repetitions, median'd by the notebook (default 3)
#   ONLY=bytes|throughput       run just one of the two
#   BYTES_DURATION=<s>          hold for experiment 1 (default 60)
#   THROUGHPUT_DURATION=<s>     hold for experiment 2 (default 120)
#   SKIP_BUILD=1                reuse the existing in-image build
#   NES_SKIP_DEV_IMAGE_BUILD=1  reuse the existing dev image instead of rebuilding it
#
# Both experiments build NES INSIDE the development Docker image, so this script needs no native
# build and no host vcpkg toolchain.

set -euo pipefail

REPO_ROOT="$(pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/distributed/run_benchmark.sh"

if [[ ! -f "$RUNNER" ]]; then
    echo "ERROR: $RUNNER not found; run this from the repository root." >&2
    exit 1
fi

### --smoke: a wiring check, not a measurement. One repetition, short holds, and only 2 replays of
### the trace instead of 8 -- enough to prove both experiments run end to end and both charts render.
### The numbers it produces are NOT usable: with DATASET_COPIES=2 the Memory source drains in a few
### seconds, so warm-up is a large share of the measured span, which is precisely what the default 8
### exists to avoid. All four variants are kept, because a chart with fewer is not the chart.
if [[ "${1:-}" == "--smoke" ]]; then
    shift
    echo "### --smoke: wiring check only, the resulting numbers are not usable"
    REPS="${REPS:-1}"
    BYTES_DURATION="${BYTES_DURATION:-20}"
    THROUGHPUT_DURATION="${THROUGHPUT_DURATION:-30}"
    DATASET_COPIES="${DATASET_COPIES:-2}"
    NUMBER_OF_BUFFERS="${NUMBER_OF_BUFFERS:-150000}"
fi
if [[ $# -gt 0 ]]; then
    echo "ERROR: unexpected argument '$1'. This script takes only --smoke; everything else is an" >&2
    echo "       environment variable (OUT_ROOT, REPS, ONLY, BYTES_DURATION, ...)." >&2
    exit 1
fi

OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/benchmark_run_histogram_delta_$(date +%Y%m%d_%H%M%S)}"
REPS="${REPS:-3}"
ONLY="${ONLY:-both}"
mkdir -p "$OUT_ROOT"

### The two files the notebook reads, flat in the output root. Each experiment keeps its own working
### subdirectory (the runner wipes OUTPUT_DIR on start and writes logs + per-topology traffic CSVs
### there), but neither is part of the interface.
BYTES_CSV="$OUT_ROOT/results_histogram_delta_bytes.csv"
THROUGHPUT_CSV="$OUT_ROOT/results_histogram_delta_throughput.csv"

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
    ### Lift the summary to the output root. The runner needs a working directory of its own (traffic
    ### CSVs, logs, and it wipes OUTPUT_DIR on start), but the notebook should see one flat CSV.
    if [[ -f "$OUT_ROOT/bytes/summary.csv" ]]; then
        cp "$OUT_ROOT/bytes/summary.csv" "$BYTES_CSV"
    fi
fi

### --- 2. Throughput -------------------------------------------------------------------------
### Memory sources, unthrottled. DATASET_COPIES=8 replays the trace 8x with each copy shifted by a
### whole number of windows: the raw trace drains in ~3.5 s, short enough that warm-up would be a
### large share of the measured span. NUMBER_OF_BUFFERS covers the ~2.4 GB of pre-parsed tuples.
### THROUGHPUT_LISTENER_INTERVAL_MS drops to 50 because the active span is short: at the default
### 200 ms, one sample of span error is several percent -- more than the gaps between variants.
###
### A failing repetition must not discard what already succeeded, so each is wrapped.
if [[ "$ONLY" == "both" || "$ONLY" == "throughput" ]]; then
    for rep in $(seq 1 "$REPS"); do
        echo
        echo "### Experiment 2/2: throughput, repetition $rep/$REPS (unthrottled, Memory source)"
        if ! ( \
            OUTPUT_DIR="$OUT_ROOT/throughput/run$rep" \
            RESULTS_COPY_DIR="$OUT_ROOT/throughput/run$rep" \
            SOURCE_TYPE=memory \
            DATASET_COPIES="${DATASET_COPIES:-8}" \
            NUMBER_OF_BUFFERS="${NUMBER_OF_BUFFERS:-600000}" \
            THROUGHPUT_LISTENER_INTERVAL_MS="${THROUGHPUT_LISTENER_INTERVAL_MS:-50}" \
            RUN_DURATION_SECONDS="${THROUGHPUT_DURATION:-120}" \
                bash "$RUNNER" ); then
            echo "WARNING: throughput repetition $rep/$REPS failed; continuing with the rest." >&2
        fi
    done
    ### Concatenate the per-repetition summaries into ONE csv, tagging each row with its repetition.
    ### The notebook medians over `run`, so the repetitions must stay distinguishable in the file.
    python3 - "$OUT_ROOT" "$THROUGHPUT_CSV" <<'MERGE'
import csv, glob, os, re, sys

root, out = sys.argv[1], sys.argv[2]
paths = glob.glob(os.path.join(root, "throughput", "run*", "summary.csv"))
# Numeric sort: a lexicographic one puts run10 before run2. Only cosmetic, but the `run` column is
# read by a human as often as by pandas.
paths.sort(key=lambda p: int(re.search(r"run(\d+)", p).group(1)))
if not paths:
    raise SystemExit(0)
rows, header = [], None
for path in paths:
    with open(path) as f:
        reader = csv.reader(f)
        head = next(reader, None)
        if head is None:
            continue
        if header is None:
            header = head + ["run"]
        run = os.path.basename(os.path.dirname(path))
        rows += [r + [run] for r in reader if r]
if header:
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"merged {len(paths)} repetition(s), {len(rows)} rows -> {os.path.basename(out)}")
MERGE
fi

echo
echo "=================================================================="
echo "Done. Results in $OUT_ROOT"
if [[ -f "$BYTES_CSV" ]]; then
    echo "  bytes      : results_histogram_delta_bytes.csv"
else
    echo "  bytes      : (not produced -- ONLY=throughput, or the experiment failed)"
fi
if [[ -f "$THROUGHPUT_CSV" ]]; then
    echo "  throughput : results_histogram_delta_throughput.csv"
else
    echo "  throughput : (not produced -- ONLY=bytes, or every repetition failed)"
fi
echo
echo "Plot both with:"
echo "  jupyter lab scripts/benchmarking/histogram_delta/plots/HistogramDeltaCompression.ipynb"
echo "(it auto-picks the most recent benchmark_run_* directory; override with RESULTS_DIR)"
echo "The notebook needs matplotlib, pandas, seaborn and jupyter."
echo "=================================================================="
