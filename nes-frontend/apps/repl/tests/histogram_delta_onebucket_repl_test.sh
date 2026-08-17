#!/usr/bin/env bash
#
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
# ---------------------------------------------------------------------------
# End-to-end REPL test for histogram delta-compression (GEN/RESOLVER split),
# ONE-BUCKET-PER-WINDOW variant. See docs/histogram-delta-wire-compression-plan.md.
#
# Companion to histogram_delta_repl_test.sh. That test uses identical windows, so
# every per-window delta after window 0 is all-zero (numChangedBins=0). This test
# instead makes each window differ from its predecessor by exactly ONE bin: window 0
# is the baseline, then each subsequent window adds one extra tuple into the next bin,
# cumulatively. So the GEN sparse delta carries exactly one changed bin per window —
# the single-bin path the sparse encoding exists for.
#
#   window 0 : baseline, every bin = 1  (values 1..10)
#   window 1 : bin for value 1 (+1)     -> bins [2,1,1,1,1,1,1,1,1,1]
#   window 2 : bin for value 2 (+1)     -> bins [2,2,1,1,1,1,1,1,1,1]
#   ...
#   window 9 : bin for value 9 (+1)     -> bins [2,2,2,2,2,2,2,2,2,1]
#
# Each window w carries the baseline (values 1..10) plus one extra copy of each value
# in 1..w, so its actual histogram is bin v = 2 iff v <= w else 1. Delta(w) = w - (w-1)
# is therefore +1 in exactly bin w (numChangedBins=1, nTuplesDelta=1). The RESOLVER must
# reconstruct each full per-window histogram from that single-bin delta plus the baseline.
#
# For each setting of the optimizer flag `enable_histogram_delta_compression`:
#   1. deploys+runs a REQUEST STATISTIC that builds an EquiWidthHistogram — flag ON is
#      the GEN/RESOLVER delta split, flag OFF a single plain build;
#   2. checks the generated plan shape (delta ops present / absent);
#   3. PROBES the node-local statistic store (one row per 1-second window) into a File sink.
# It then asserts the reconstructed bins match the expected per-window golden AND are
# byte-identical between the delta and plain builds.
#
#   flag ON  ->  SOURCE -> StatBuild(EquiWidthHistogramDeltaGen)
#                       -> WatermarkAssigner(event time, re-window on STATISTICSTART)
#                       -> StatBuild(EquiWidthHistogramDeltaResolver)
#                       -> StatisticStoreWriter -> sink
#   flag OFF ->  SOURCE -> StatBuild(EquiWidthHistogram) -> StatisticStoreWriter -> sink
#
# NOTE — single worker thread here: this test pins number_of_worker_threads=1 for simplicity.
# Multi-threaded delta compression is covered by the MultiThread scenario in WindowAggregationHistogramDelta.test.
#
# The plan shape is read from the REPL's "DEBUG: Statistic query BEFORE optimization"
# stderr dump (ReplStarter.cpp). If that debug print is ever removed, update plan_block.
#
# Usage:
#   NES_REPL=/path/to/nes-repl-embedded ./histogram_delta_onebucket_repl_test.sh
# or, using a build dir:
#   BUILD_DIR=/path/to/cmake-build-debug ./histogram_delta_onebucket_repl_test.sh
# ---------------------------------------------------------------------------

set -uo pipefail

# --- locate the embedded REPL binary --------------------------------------
BUILD_DIR="${BUILD_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)/cmake-build-debug}"
REPL="${NES_REPL:-$BUILD_DIR/nes-frontend/apps/nes-repl-embedded}"

if [[ ! -x "$REPL" ]]; then
    echo "ERROR: embedded REPL binary not found or not executable: $REPL" >&2
    echo "       set NES_REPL=/path/to/nes-repl-embedded or BUILD_DIR=/path/to/build" >&2
    exit 2
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
CSV="$WORK/stream.csv"
KEYS="$WORK/keys.csv"

# --- static tuples: window w holds the baseline values 1..10 plus one extra copy of each
# value in 1..w (cumulative), so bin v = 2 iff v <= w else 1. Each window is 1 s wide
# (TUMBLING 1000 ms); tuples are spaced 40 ms apart so even the widest window (10+9=19
# tuples) stays inside its 1 s bound. Total 100 + (0+1+...+9) = 145 tuples.
cat > "$CSV" <<'EOF'
1,1,0
1,2,40
1,3,80
1,4,120
1,5,160
1,6,200
1,7,240
1,8,280
1,9,320
1,10,360
1,1,1000
1,2,1040
1,3,1080
1,4,1120
1,5,1160
1,6,1200
1,7,1240
1,8,1280
1,9,1320
1,10,1360
1,1,1400
1,1,2000
1,2,2040
1,3,2080
1,4,2120
1,5,2160
1,6,2200
1,7,2240
1,8,2280
1,9,2320
1,10,2360
1,1,2400
1,2,2440
1,1,3000
1,2,3040
1,3,3080
1,4,3120
1,5,3160
1,6,3200
1,7,3240
1,8,3280
1,9,3320
1,10,3360
1,1,3400
1,2,3440
1,3,3480
1,1,4000
1,2,4040
1,3,4080
1,4,4120
1,5,4160
1,6,4200
1,7,4240
1,8,4280
1,9,4320
1,10,4360
1,1,4400
1,2,4440
1,3,4480
1,4,4520
1,1,5000
1,2,5040
1,3,5080
1,4,5120
1,5,5160
1,6,5200
1,7,5240
1,8,5280
1,9,5320
1,10,5360
1,1,5400
1,2,5440
1,3,5480
1,4,5520
1,5,5560
1,1,6000
1,2,6040
1,3,6080
1,4,6120
1,5,6160
1,6,6200
1,7,6240
1,8,6280
1,9,6320
1,10,6360
1,1,6400
1,2,6440
1,3,6480
1,4,6520
1,5,6560
1,6,6600
1,1,7000
1,2,7040
1,3,7080
1,4,7120
1,5,7160
1,6,7200
1,7,7240
1,8,7280
1,9,7320
1,10,7360
1,1,7400
1,2,7440
1,3,7480
1,4,7520
1,5,7560
1,6,7600
1,7,7640
1,1,8000
1,2,8040
1,3,8080
1,4,8120
1,5,8160
1,6,8200
1,7,8240
1,8,8280
1,9,8320
1,10,8360
1,1,8400
1,2,8440
1,3,8480
1,4,8520
1,5,8560
1,6,8600
1,7,8640
1,8,8680
1,1,9000
1,2,9040
1,3,9080
1,4,9120
1,5,9160
1,6,9200
1,7,9240
1,8,9280
1,9,9320
1,10,9360
1,1,9400
1,2,9440
1,3,9480
1,4,9520
1,5,9560
1,6,9600
1,7,9640
1,8,9680
1,9,9720
EOF

# --- probe driving keys: one (statisticId, windowStart, windowEnd) row per window.
# The StatisticStoreReader reads these keys from each input record and looks the window
# up in the store, emitting one output row per bin.
cat > "$KEYS" <<'EOF'
1,0,1000
1,1000,2000
1,2000,3000
1,3000,4000
1,4000,5000
1,5000,6000
1,6000,7000
1,7000,8000
1,8000,9000
1,9000,10000
EOF

# --- expected per-window golden: 10 bins [1,11), width 1 -> value v falls in bin v.
# Bin v has counter 2 once value v got its extra copy (window v onwards), else 1.
# Row layout: start end binStart binCounter binEnd.
GOLDEN_TEXT='0 1000 1 1 2
0 1000 2 1 3
0 1000 3 1 4
0 1000 4 1 5
0 1000 5 1 6
0 1000 6 1 7
0 1000 7 1 8
0 1000 8 1 9
0 1000 9 1 10
0 1000 10 1 11
1000 2000 1 2 2
1000 2000 2 1 3
1000 2000 3 1 4
1000 2000 4 1 5
1000 2000 5 1 6
1000 2000 6 1 7
1000 2000 7 1 8
1000 2000 8 1 9
1000 2000 9 1 10
1000 2000 10 1 11
2000 3000 1 2 2
2000 3000 2 2 3
2000 3000 3 1 4
2000 3000 4 1 5
2000 3000 5 1 6
2000 3000 6 1 7
2000 3000 7 1 8
2000 3000 8 1 9
2000 3000 9 1 10
2000 3000 10 1 11
3000 4000 1 2 2
3000 4000 2 2 3
3000 4000 3 2 4
3000 4000 4 1 5
3000 4000 5 1 6
3000 4000 6 1 7
3000 4000 7 1 8
3000 4000 8 1 9
3000 4000 9 1 10
3000 4000 10 1 11
4000 5000 1 2 2
4000 5000 2 2 3
4000 5000 3 2 4
4000 5000 4 2 5
4000 5000 5 1 6
4000 5000 6 1 7
4000 5000 7 1 8
4000 5000 8 1 9
4000 5000 9 1 10
4000 5000 10 1 11
5000 6000 1 2 2
5000 6000 2 2 3
5000 6000 3 2 4
5000 6000 4 2 5
5000 6000 5 2 6
5000 6000 6 1 7
5000 6000 7 1 8
5000 6000 8 1 9
5000 6000 9 1 10
5000 6000 10 1 11
6000 7000 1 2 2
6000 7000 2 2 3
6000 7000 3 2 4
6000 7000 4 2 5
6000 7000 5 2 6
6000 7000 6 2 7
6000 7000 7 1 8
6000 7000 8 1 9
6000 7000 9 1 10
6000 7000 10 1 11
7000 8000 1 2 2
7000 8000 2 2 3
7000 8000 3 2 4
7000 8000 4 2 5
7000 8000 5 2 6
7000 8000 6 2 7
7000 8000 7 2 8
7000 8000 8 1 9
7000 8000 9 1 10
7000 8000 10 1 11
8000 9000 1 2 2
8000 9000 2 2 3
8000 9000 3 2 4
8000 9000 4 2 5
8000 9000 5 2 6
8000 9000 6 2 7
8000 9000 7 2 8
8000 9000 8 2 9
8000 9000 9 1 10
8000 9000 10 1 11
9000 10000 1 2 2
9000 10000 2 2 3
9000 10000 3 2 4
9000 10000 4 2 5
9000 10000 5 2 6
9000 10000 6 2 7
9000 10000 7 2 8
9000 10000 8 2 9
9000 10000 9 2 10
9000 10000 10 1 11'

# --- driver SQL. Embedded mode auto-registers localhost:8080 (no CREATE WORKER).
# `min`/`max` are backtick-quoted (MIN/MAX are reserved lexer tokens). MINVAL maps to
# EquiWidthHistogram; budget 248 -> (248-8)/24 = 10 bins over [1,11). EVENTTIME uses the
# tuples' `timestamp` field (ingestion-time windows need $record.creationTs, absent for a
# File source). $1 = File-sink path the probe writes the reconstructed bins to.
sql() {
    local binsPath="$1"
    cat <<SQL
CREATE LOGICAL SOURCE stream(id UINT64, value UINT64, timestamp UINT64);
CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('$CSV' AS \`SOURCE\`.FILE_PATH, 'CSV' AS PARSER.\`TYPE\`, 'localhost:8080' AS \`SOURCE\`.HOST);
CREATE LOGICAL SOURCE keys(STATISTICID UINT64, STATISTICSTART UINT64, STATISTICEND UINT64);
CREATE PHYSICAL SOURCE FOR keys TYPE File SET ('$KEYS' AS \`SOURCE\`.FILE_PATH, 'CSV' AS PARSER.\`TYPE\`, 'localhost:8080' AS \`SOURCE\`.HOST);
CREATE SINK bins(keys.STATISTICSTART UINT64 NOT NULL, keys.STATISTICEND UINT64 NOT NULL, keys.binStart UINT64 NOT NULL, keys.binCounter UINT64 NOT NULL, keys.binEnd UINT64 NOT NULL) TYPE File SET ('$binsPath' AS \`SINK\`.FILE_PATH, 'CSV' AS \`SINK\`.OUTPUT_FORMAT, 'localhost:8080' AS \`SINK\`.HOST);
REQUEST STATISTIC DATA MINVAL ON stream(value) WINDOW TUMBLING(SIZE 1000 MS) EVENTTIME timestamp SET ('localhost:8080' AS host, 1 AS \`min\`, 11 AS \`max\`, 248 AS memory_budget);
SQL
}

# The store-probe query: read the just-built statistic (id 1) for each window key and
# emit its bins. Submitted after a pause so the build has populated the node-local store.
probe_sql='SELECT statisticStart, statisticEnd, binStart, binCounter, binEnd FROM ( SELECT EQUIWIDTHHISTOGRAM_PROBE(1, uint64, uint64) FROM keys ) INTO bins;'

# run_repl <flag true|false> <run-dir>: builds, waits, probes; all in the run dir so
# nes-repl.log and the bins sink are isolated. Sets globals RC, RUN_ERR, RUN_BINS.
run_repl() {
    local flag="$1" rundir="$2"
    mkdir -p "$rundir"
    RUN_ERR="$rundir/repl.err"
    RUN_BINS="$rundir/bins.out"
    # First the build (REQUEST STATISTIC), then a pause for it to populate the store, then
    # the probe; the trailing sleep lets the probe finish before EOF tears the REPL down.
    ( sql "$RUN_BINS"; sleep 8; echo "$probe_sql"; sleep 8 ) \
        | ( cd "$rundir" && timeout 120 "$REPL" -f JSON \
            --optimizer "enable_histogram_delta_compression=$flag" \
            -- "--worker.query_engine.number_of_worker_threads=1" ) \
        > "$rundir/repl.out" 2> "$RUN_ERR"
    RC=$?
}

# Extract just the "BEFORE optimization" plan block of the *build* query from stderr.
plan_block() { sed -n '/DEBUG: Statistic query BEFORE optimization/,/^$/p' "$1/repl.err"; }

# Normalised probe output: drop the schema header line (contains ':'), turn commas into
# spaces, and sort so the comparison is independent of emission order.
probe_rows() { grep -v ':' "$1/bins.out" 2>/dev/null | tr ',' ' ' | sort; }

fail() {
    echo "FAIL: $*" >&2
    echo "----- exit=$RC ; repl.log errors -----" >&2
    grep -E "\[E\]|failed to process" "$1/nes-repl.log" 2>/dev/null | grep -v "0x" >&2 || true
    echo "----- plan dump -----" >&2
    plan_block "$1" >&2
    echo "----- probe rows -----" >&2
    probe_rows "$1" >&2
    exit 1
}

assert_ran_clean() {
    local rundir="$1" label="$2"
    [[ "$RC" -eq 0 ]] || fail "$label: REPL exited non-zero ($RC) — query did not deploy/run" "$rundir"
    if grep -qE "\[E\]|failed to process" "$rundir/nes-repl.log" 2>/dev/null; then
        fail "$label: runtime error logged while the query ran" "$rundir"
    fi
}

echo "# REPL: $REPL"
echo "# data: 145 tuples / 10 windows, one-bucket-per-window delta -> $CSV"

GOLDEN="$(echo "$GOLDEN_TEXT" | sort)"

# --- case 1: flag ON -> GEN/RESOLVER split, runs, and reconstructs correctly -------
ON_DIR="$WORK/on"
run_repl true "$ON_DIR"
assert_ran_clean "$ON_DIR" "flag ON"
ON_PLAN="$(plan_block "$ON_DIR")"
grep -q "EquiWidthHistogramDeltaGen" <<<"$ON_PLAN" \
    || fail "flag ON: plan is missing the GEN build (EquiWidthHistogramDeltaGen)" "$ON_DIR"
grep -q "EquiWidthHistogramDeltaResolver" <<<"$ON_PLAN" \
    || fail "flag ON: plan is missing the RESOLVER build (EquiWidthHistogramDeltaResolver)" "$ON_DIR"
echo "PASS: flag ON runs the EquiWidthHistogramDeltaGen -> EquiWidthHistogramDeltaResolver split"

ON_ROWS="$(probe_rows "$ON_DIR")"
[[ "$ON_ROWS" == "$GOLDEN" ]] \
    || fail "flag ON: reconstructed bins do not match the expected one-bucket-per-window histogram" "$ON_DIR"
echo "PASS: flag ON — single-bin delta reconstructs to the exact expected per-window histogram"

# --- case 2: flag OFF -> single plain histogram, runs, correct -------------------
OFF_DIR="$WORK/off"
run_repl false "$OFF_DIR"
assert_ran_clean "$OFF_DIR" "flag OFF"
OFF_PLAN="$(plan_block "$OFF_DIR")"
grep -q "EquiWidthHistogram" <<<"$OFF_PLAN" \
    || fail "flag OFF: plan is missing the plain EquiWidthHistogram build" "$OFF_DIR"
if grep -q "Delta" <<<"$OFF_PLAN"; then
    fail "flag OFF: plan unexpectedly contains a delta build" "$OFF_DIR"
fi
echo "PASS: flag OFF runs a single plain EquiWidthHistogram build (no delta split)"

OFF_ROWS="$(probe_rows "$OFF_DIR")"
[[ "$OFF_ROWS" == "$GOLDEN" ]] \
    || fail "flag OFF: plain histogram bins do not match the expected histogram" "$OFF_DIR"

# --- the key correctness check: delta reconstructs identically to the plain build --
[[ "$ON_ROWS" == "$OFF_ROWS" ]] \
    || { echo "FAIL: delta-reconstructed bins differ from the plain histogram" >&2
         diff <(echo "$OFF_ROWS") <(echo "$ON_ROWS") >&2; exit 1; }
echo "PASS: delta reconstruction is byte-identical to the plain histogram"

echo "ALL PASSED"
