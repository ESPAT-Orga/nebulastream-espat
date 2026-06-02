#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Adaptive optimization benchmark.

Starts a nes-single-node-worker and nes-repl (distributed mode) as separate processes,
deploys a query, lets it run for a configurable duration, then tears everything down.

Usage (run from repository root):
    python -m scripts.benchmarking.adaptive-optimization.run_adaptive_optimization_benchmark
    python -m scripts.benchmarking.adaptive-optimization.run_adaptive_optimization_benchmark --duration 120
    python -m scripts.benchmarking.adaptive-optimization.run_adaptive_optimization_benchmark --clean
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    convert_unit_prefix,
    create_folder_and_remove_if_exists,
    get_vcpkg_dir,
    printError,
    printInfo,
    printSuccess,
)

# generate_bid_data.py sits in this same directory; the dotted hyphenated path
# `scripts.benchmarking.adaptive-optimization.generate_bid_data` cannot be imported, so we
# add the local directory to sys.path and import by short name.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_bid_data import DEFAULT_OUTPUT_A, DEFAULT_OUTPUT_B, ensure_dataset_a, ensure_dataset_b

# How many full passes through the current dataset before MemorySource flips to the other one.
# At ~400 MTup/s and 30M-row datasets one pass is ~75 ms, so 30 ≈ 2.25 s of one regime —
# short enough that a 60 s bench observes ~13 regime switches, long enough that each regime's
# histogram covers multiple closed windows (windowSize=60M event-time ≈ 0.3 s wall clock per
# window, so ~7 closed windows per regime).
REPLAYS_PER_FILE = 30

#### Build Configuration
build_dir = os.path.join(".", "build_dir")

cmake_flags = (
    "-G Ninja "
    "-DCMAKE_BUILD_TYPE=RelWithDebInfo "
    f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
    "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
    "-DNES_BUILD_NATIVE:BOOL=ON"
)

#### Binary Paths
worker_binary = os.path.join(build_dir, "nes-single-node-worker", "nes-single-node-worker")
repl_binary = os.path.join(build_dir, "nes-frontend", "apps", "nes-repl")

#### Worker / REPL Addresses
WORKER_GRPC = "localhost:8080"
WORKER_DATA = "localhost:9090"

#### Query to deploy (nexmark bid-like schema, Memory source with LOOP)
#
# Two filters are applied with the following selectivities (fields are independent):
#   bidValue < 10.45  →  selectivity 0.01
#     bidValue ~ N(mean=50, stddev=17); 1st percentile = 50 + 17 * Φ⁻¹(0.01) ≈ 10.45
#   price    < 888.49 →  selectivity 0.99
#     price    ~ N(mean=500, stddev=167); 99th percentile = 500 + 167 * Φ⁻¹(0.99) ≈ 888.49
#   Combined selectivity (AND): 0.01 * 0.99 ≈ 0.0099
#
def _expensive_filter_clause(sqrts: int) -> str:
    """Per-tuple SQRT chain summed > 0 (always passes). Empty for sqrts <= 0."""
    if sqrts <= 0:
        return ""
    terms = " + ".join(
        f"SQRT({'bidValue' if i % 2 == 0 else 'price'} + FLOAT64({1000 + i // 2}))"
        for i in range(sqrts)
    )
    return f"{terms} > FLOAT64(0.0)"


def make_data_select_sql(variant: str, sqrts: int) -> str:
    """SELECT block for the data query in the requested filter order.

    variant="bid_first":  bidValue first, then price (the original adaptive default).
    variant="price_first": price first, then bidValue (the swap target).

    The expensive SQRT clause, when present, is sandwiched between the two filters in both
    variants so per-tuple cost is comparable. The variant determines only which filter is
    more selective on the upstream side and therefore "wins" on that regime's distribution.
    """
    expensive = _expensive_filter_clause(sqrts)
    if variant == "price_first":
        first_filter = "price < FLOAT64(888.49)"
        second_filter = "bidValue < FLOAT64(10.45)"
    else:
        first_filter = "bidValue < FLOAT64(10.45)"
        second_filter = "price < FLOAT64(888.49)"
    if expensive:
        return f"""\
SELECT timestamp, auctionId, bidValue, price
FROM (
  SELECT timestamp, auctionId, bidValue, price
  FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE {first_filter})
  WHERE {expensive}
)
WHERE {second_filter}
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""
    return f"""\
SELECT timestamp, auctionId, bidValue, price
FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE {first_filter})
WHERE {second_filter}
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""


def make_setup_sql(data_path_a: str, data_path_b: str, sqrts: int, constant_workload: bool, variant: str = "bid_first") -> str:
    select_block = make_data_select_sql(variant, sqrts)
    # With --constant-workload, only regime A is loaded and looped indefinitely. With both,
    # the source alternates between A and B every REPLAYS_PER_FILE full passes to simulate a
    # workload-distribution shift on a deterministic schedule.
    if constant_workload:
        source_set_clause = f"""\
    'NATIVE' as PARSER.`TYPE`,
    '{data_path_a}' AS `SOURCE`.FILE_PATH,
    'timestamp' AS `SOURCE`.MONOTONIC_TIMESTAMP_FIELD,
    'true' AS `SOURCE`.LOOP,
    '{WORKER_GRPC}' AS `SOURCE`.HOST"""
    else:
        source_set_clause = f"""\
    'NATIVE' as PARSER.`TYPE`,
    '{data_path_a}' AS `SOURCE`.FILE_PATH,
    '{data_path_b}' AS `SOURCE`.FILE_PATH_2,
    '{REPLAYS_PER_FILE}' AS `SOURCE`.REPLAYS_PER_FILE,
    'timestamp' AS `SOURCE`.MONOTONIC_TIMESTAMP_FIELD,
    'true' AS `SOURCE`.LOOP,
    '{WORKER_GRPC}' AS `SOURCE`.HOST"""
    return f"""\
CREATE WORKER "{WORKER_GRPC}" SET ('{WORKER_DATA}' AS DATA);
CREATE LOGICAL SOURCE bid(timestamp UINT64 NOT NULL, auctionId INT32 NOT NULL, bidValue FLOAT64 NOT NULL, price FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR bid
TYPE Memory
SET(
{source_set_clause}
);
CREATE SINK someSink(BID.TIMESTAMP UINT64 NOT NULL, BID.AUCTIONID INT32 NOT NULL, BID.BIDVALUE FLOAT64 NOT NULL, BID.PRICE FLOAT64 NOT NULL)
TYPE File
SET(
    'out.csv' as `SINK`.FILE_PATH,
    'CSV' as `SINK`.OUTPUT_FORMAT,
    '{WORKER_GRPC}' AS `SINK`.HOST
);
{select_block}
"""


def make_reversed_query_sql(sqrts: int) -> str:
    """Filter-reversed alternate (price first); used by adaptive mode as --companion-switch-to-sql.
    Delegates to make_data_select_sql so the bid-first / price-first / adaptive paths stay in
    sync as the query shape evolves."""
    return make_data_select_sql("price_first", sqrts)

# Matches: Throughput for queryId QueryId(local=<UUID>, distributed=<horse-name>) in window <ts>-<ts> is <val> <prefix>Tup/s
_THROUGHPUT_RE = re.compile(
    r"Throughput for queryId QueryId\(local=[^,)]+, distributed=([^)]+)\)"
    r" in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s"
)

# Matches: [{"query_id": "<horse-name>"}]  (initial SELECT deployed via REPL stdin → JSON output path)
_JSON_QUERY_ID_RE = re.compile(r'^\[{"query_id":\s*"([^"]+)"}]')

# Matches: [AdaptiveOpt] Deployed query (id=<horse-name>).  (swap callback → plain-text output path)
_ADAPTIVE_QUERY_ID_RE = re.compile(r'\[AdaptiveOpt\] Deployed query \(id=([^)]+)\)')


def stream_output(proc, label, lines_out):
    """Read lines from a process stdout/stderr and print them with a label prefix."""
    for line in iter(proc.stdout.readline, b""):
        decoded = line.decode(errors="replace").rstrip()
        print(f"[{label}] {decoded}", flush=True)
        lines_out.append(decoded)


def wait_for_port(host, port, timeout=10.0, interval=0.2):
    """Poll until a TCP port accepts connections or timeout is reached."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=interval):
                return True
        except OSError:
            time.sleep(interval)
    return False


def terminate_process(proc, name, timeout=5):
    """Gracefully terminate a process, escalating to SIGKILL if needed."""
    if proc.poll() is not None:
        printInfo(f"{name} already exited (code {proc.returncode})")
        return
    printInfo(f"Terminating {name} (pid={proc.pid})...")
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
        printSuccess(f"{name} terminated cleanly")
    except subprocess.TimeoutExpired:
        printError(f"{name} did not stop in {timeout}s — sending SIGKILL")
        proc.kill()
        proc.wait()


def find_data_query_id(repl_lines, timeout=15.0):
    """Poll repl_lines until the SELECT response appears and return its distributed query ID.

    The REPL emits [{"query_id": "<horse-name>"}] (exactly one key) for a deployed SELECT.
    All other setup statement responses have additional keys (worker, source_name, sink_name, …).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for line in repl_lines:
            try:
                parsed = json.loads(line)
                if (
                    isinstance(parsed, list)
                    and len(parsed) == 1
                    and set(parsed[0].keys()) == {"query_id"}
                ):
                    return parsed[0]["query_id"]
            except (json.JSONDecodeError, KeyError):
                pass
        time.sleep(0.1)
    return None


def collect_all_data_query_ids(repl_lines):
    """Scan all REPL output and return the set of every data query's distributed ID.

    Two output paths emit query IDs:
      - Initial SELECT (via stdin → JSON path): [{"query_id": "<horse-name>"}]
      - Adaptive swap (via callback → plain-text): [AdaptiveOpt] Deployed query (id=<horse-name>).
    Statistic collection queries are deployed internally via StatisticCoordinator and never
    print a distributed query ID to the REPL stdout, so they are excluded naturally.
    """
    ids = set()
    for line in repl_lines:
        if m := _JSON_QUERY_ID_RE.search(line):
            ids.add(m.group(1))
        elif m := _ADAPTIVE_QUERY_ID_RE.search(line):
            ids.add(m.group(1))
    return ids


def parse_throughput(worker_lines, data_query_ids):
    """Return list of (window_start_ms, query_id, query_type, throughput_tup_per_s).

    `query_type` is "data" if the queryId was observed in REPL output (initial SELECT or
    adaptive swap), otherwise "stat" — the statistic-collection queries are deployed
    internally by StatisticCoordinator and never print a distributed query ID to REPL stdout,
    so they fall through to the "stat" bucket.
    """
    measurements = []
    for line in worker_lines:
        m = _THROUGHPUT_RE.search(line)
        if m:
            qid = m.group(1)
            window_start = int(m.group(2))
            throughput = convert_unit_prefix(float(m.group(4)), m.group(5))
            qtype = "data" if qid in data_query_ids else "stat"
            measurements.append((window_start, qid, qtype, throughput))
    return measurements


def write_throughput_csv(measurements, output_path):
    """Write all per-window throughput samples to a CSV for time-series plotting."""
    if not measurements:
        printError("No throughput data collected.")
        return
    min_ts = measurements[0][0]
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "query_id", "query_type", "throughput_tup_per_s"])
        for ts, qid, qtype, tput in measurements:
            writer.writerow([ts - min_ts, qid, qtype, tput])
    printSuccess(f"Throughput data ({len(measurements)} samples) written to {os.path.abspath(output_path)}")


def run_benchmark(
    duration: int,
    skip_build: bool,
    clean: bool,
    output: str,
    sqrts: int = 0,
    constant_workload: bool = False,
    fixed_variant: str = "",
):
    check_repository_root()

    if clean:
        create_folder_and_remove_if_exists(build_dir)

    if not skip_build:
        printInfo("Building NebulaStream...")
        compile_nebulastream(cmake_flags, build_dir)
        printSuccess("Build complete.")
    else:
        printInfo("Skipping build (--skip-build)")

    for binary, label in [(worker_binary, "nes-single-node-worker"), (repl_binary, "nes-repl")]:
        if not os.path.isfile(binary):
            printError(f"Binary not found: {binary}")
            printError("Run without --skip-build to compile first.")
            sys.exit(1)

    data_path_a = ensure_dataset_a(path=DEFAULT_OUTPUT_A)
    if constant_workload:
        # Single regime — regime B isn't loaded. The probe predicate is expected to fire only
        # in the very first window (initial convergence to the optimal ordering); after that
        # the histogram is stable and the swap callback should be a no-op.
        data_path_b = ""
    else:
        data_path_b = ensure_dataset_b(path=DEFAULT_OUTPUT_B)
    # When --fixed-variant is set we want a baseline: the chosen filter order runs without ANY
    # adaptive machinery (no build branch splice, no probe queries, no swap callback). This is
    # the comparison point for "what would performance be without the adaptive system?" — the
    # answer to whether adaptation actually pays for its own overhead.
    variant_for_query = fixed_variant if fixed_variant else "bid_first"
    setup_sql = make_setup_sql(data_path_a, data_path_b, sqrts, constant_workload, variant_for_query)
    if fixed_variant:
        printInfo(
            f"Using {sqrts} SQRT operators between filters; "
            f"workload={'CONSTANT (regime A only)' if constant_workload else 'ALTERNATING (A/B)'}; "
            f"fixed-variant={fixed_variant} (companion-statistic DISABLED for baseline).")
    else:
        printInfo(
            f"Using {sqrts} SQRT operators between filters; "
            f"workload={'CONSTANT (regime A only)' if constant_workload else 'ALTERNATING (A/B)'}; "
            f"variant=ADAPTIVE (companion-statistic enabled).")

    # --- Start worker ---
    printInfo(f"Starting nes-single-node-worker (grpc={WORKER_GRPC}, data={WORKER_DATA})...")
    worker_proc = subprocess.Popen(
        [
            worker_binary,
            "--grpc=0.0.0.0:8080",
            "--data_address=0.0.0.0:9090",
            "--worker.default_query_execution.operator_buffer_size=4194304",
            "--worker.number_of_buffers_in_global_buffer_manager=1024",
            "--worker.query_engine.number_of_worker_threads=12",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    worker_lines = []
    worker_thread = threading.Thread(
        target=stream_output, args=(worker_proc, "WORKER", worker_lines), daemon=True
    )
    worker_thread.start()

    grpc_port = int(WORKER_GRPC.split(":")[1])
    printInfo(f"Waiting for worker gRPC port {grpc_port}...")
    if not wait_for_port("localhost", grpc_port, timeout=15):
        printError("Worker did not open gRPC port within 15 seconds")
        terminate_process(worker_proc, "worker")
        sys.exit(1)
    printSuccess("Worker is up.")

    # --- Build REPL command ---
    # Base command (always present, no companion): the fixed-variant baseline path.
    repl_cmd = [repl_binary, "-f", "JSON"]
    # Adaptive mode: add the full --companion-* configuration so the REPL spawns the build
    # branch + two gated probes + swap callback. With --fixed-variant set we skip all of this
    # so the bench measures pure data-query throughput for that filter order — a clean
    # baseline to compare adaptive against.
    if not fixed_variant:
        repl_cmd += [
            "--companion-statistic",
            # Splice the build branch into the data query (one source thread feeds both
            # subtrees) and run gated probes whose swap callbacks flip the workload-switch.
            "--companion-domain", "workload",
            "--companion-source", "bid",
            "--companion-field", "price",
            # MinVal maps to Equi_Width_Histogram, which is the only metric the gated probe
            # supports (StatisticStoreReader returns histogram bins as rows for the Selection
            # predicate to filter on). Cardinality goes to a CountMinSketch — different probe
            # operator, no in-pipeline bin filtering.
            "--companion-metric", "MinVal",
            # 60M event-time ms — at the steady-state ingest rate of ~200M tup/s the histogram
            # closes ~3× per wall-clock second, low enough to keep the statistic store bounded
            # while frequent enough that gated-probe trigger fires arrive within ~1s. Smaller
            # windows have OOM'd the worker (event time advances at the tuple-emit rate).
            "--companion-window-size-ms", "60000000",
            "--companion-event-time-field", "BID$TIMESTAMP",
            "--companion-host", WORKER_GRPC,
            # Probe-tick cadence matched to the window-close cadence (~0.3 s at our ingest rate
            # of ~200M tup/s with 60M event-time windows). Default is 10 s, which would leave the
            # swap callback up to 10 s behind a regime change — that's exactly the "stairs effect"
            # in the throughput curve when REPLAYS_PER_FILE-driven regime cycles are faster than
            # the sampling rate. 300 ms gives ~3 probe ticks per regime cycle and adaptation lag
            # stays bounded to one window-close cycle.
            "--companion-probe-interval-ms", "300",
            "--companion-switch-to-sql", make_reversed_query_sql(sqrts),
            # Histogram bucket range widened to [0, 2000] so both regimes' price distributions
            # fit (regime A: price~N(500,167); regime B: price~N(1277,167); regime B would be
            # clipped at the default max=1000).
            "--companion-histogram-min", "0",
            "--companion-histogram-max", "2000",
            # Two gated probes covering the two regimes by NON-OVERLAPPING price-bin ranges.
            # Probe A (fires on regime A: price~N(500, 167)): BINSTART < 900 → set switch=0
            #   (bid-first variant). Regime A's bidValue~N(50, 17) makes `bidValue < 10.45`
            #   the more selective filter (~0.9% pass) so we want it first.
            # Probe B (fires on regime B: price~N(1277, 167)): BINSTART >= 900 → set switch=1
            #   (price-first variant). Regime B's bidValue~N(-30, 17) makes `bidValue < 10.45`
            #   match ~99%, while `price < 888.49` is the selective one (~1%), so price-first wins.
            # Density threshold UINT64(500000): regime A's right-tail mass at BINSTART=900 is
            # ~160K tuples (~0.27% of 60M); regime B's left-tail there is ~2.16M (~3.6%). 500K
            # filters out the leaky tails so each probe matches only when its regime is
            # genuinely dominant.
            # Phase 2: two build branches monitoring TWO different fields, each with its own
            # predicate-and-target. Each request gets its own statisticId; both build branches
            # splice into BID via SpliceToRunningSourceTrait, and the source defers emission until
            # both have wired in (expected_splice_count=2). The probe inside each build branch
            # routes survivors to its own callback (no shared probe, no Generator polling).
            #
            # Probe A monitors PRICE (set via --companion-field "price" above): in regime A
            # (price ~ N(500, 167)) most mass is in [0, 900), so BINSTART < 900 → fires → target
            # switch=0 (bid-first variant; bidValue<10.45 is very selective in regime A).
            "--companion-condition", "BINSTART < UINT64(900) AND BINCOUNTER > UINT64(500000)",
            "--companion-target-value", "0",
            # Probe B monitors BIDVALUE: in regime B (bidValue ~ N(-30, 17)) most mass is below
            # the filter threshold 10.45, so BINSTART < 11 → fires → target switch=1.
            # Threshold 1500000: regime A's bidValue left-tail mass in [0, 11) is ~564K
            # (P(bidValue<11 | N(50, 17)) ≈ 0.94% × 60M); regime B's positive-tail mass there
            # is ~1.87M (P(0<bidValue<11 | N(-30, 17)) ≈ 3.12% × 60M). 1.5M cleanly separates.
            # NOTE: histogram-min=0/max=2000 (set globally for the bench) clips regime B's
            # negative bidValues — only its right shoulder survives, which is enough to
            # produce the distinguishable density spike Probe B's predicate keys on.
            "--companion-field-2", "bidValue",
            "--companion-condition-2", "BINSTART < UINT64(11) AND BINCOUNTER > UINT64(1500000)",
            "--companion-target-value-2", "1",
        ]

    # --- Start REPL ---
    printInfo("Starting nes-repl (distributed mode)...")
    repl_proc = subprocess.Popen(
        repl_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    repl_lines = []
    repl_thread = threading.Thread(
        target=stream_output, args=(repl_proc, "REPL", repl_lines), daemon=True
    )
    repl_thread.start()

    # --- Send SQL setup commands ---
    printInfo("Sending SQL setup commands to REPL...")
    try:
        repl_proc.stdin.write(setup_sql.encode())
        repl_proc.stdin.flush()
    except BrokenPipeError:
        printError("REPL stdin closed unexpectedly — did the REPL crash?")
        terminate_process(repl_proc, "nes-repl")
        terminate_process(worker_proc, "nes-single-node-worker")
        sys.exit(1)

    # --- Record the distributed query ID assigned to the data query ---
    printInfo("Waiting for data query deployment confirmation...")
    # Memory source parses the whole CSV at setup() before reporting deployed.
    # A 2.4 GB file takes ~1–2 min on this box, so give the REPL plenty of slack.
    data_query_id = find_data_query_id(repl_lines, timeout=300.0)
    if data_query_id is None:
        printError("Timed out waiting for the SELECT query response — REPL may have crashed.")
        terminate_process(repl_proc, "nes-repl")
        terminate_process(worker_proc, "nes-single-node-worker")
        sys.exit(1)
    printSuccess(f"Data query deployed with id: {data_query_id}")

    printSuccess(f"Query deployed. Running for {duration} seconds...")

    # --- Run for the configured duration ---
    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        printInfo("Interrupted by user — tearing down early.")

    # --- Tear down ---
    printInfo("Tearing down...")
    terminate_process(repl_proc, "nes-repl")
    terminate_process(worker_proc, "nes-single-node-worker")

    # Wait for streaming threads to drain the remaining pipe output
    repl_thread.join(timeout=5)
    worker_thread.join(timeout=5)

    # --- Parse and write throughput CSV ---
    data_query_ids = collect_all_data_query_ids(repl_lines)
    printInfo(f"Data query IDs observed across all deployments: {data_query_ids}")
    measurements = parse_throughput(worker_lines, data_query_ids)
    write_throughput_csv(measurements, output)

    printSuccess("Benchmark complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Adaptive optimization benchmark: single worker + distributed REPL."
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="How long (seconds) to let the query run before tearing down (default: 120).",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip the cmake configure + build step (binaries must already exist).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove and recreate the build directory before building.",
    )
    parser.add_argument(
        "--output",
        default="data_throughput_adaptive.csv",
        help="Path for the throughput CSV output (default: data_throughput_adaptive.csv).",
    )
    parser.add_argument(
        "--sqrts",
        type=int,
        default=0,
        help="Number of SQRT operators to insert between the two filters (default: 0). The SQRT chain "
        "is an always-true WHERE adding per-tuple CPU cost in the middle pipeline — useful for "
        "exposing throughput differences between filter orderings.",
    )
    parser.add_argument(
        "--constant-workload",
        action="store_true",
        help="Run with only regime A loaded (no FILE_PATH_2, no REPLAYS_PER_FILE alternation). "
        "Expected behavior: histogram converges to a single distribution within the first window "
        "and the swap callback should fire at most once (the initial reconfiguration). Useful for "
        "verifying the adaptive mechanism settles instead of toggling continuously.",
    )
    parser.add_argument(
        "--fixed-variant",
        choices=["bid_first", "price_first"],
        default="",
        help="If set, skip the entire companion-statistic deployment (no build branch splice, no "
        "gated probes, no swap callback) and run ONLY the chosen filter ordering. Use this to "
        "produce a baseline curve showing what raw throughput looks like without adaptation — "
        "the comparison point that justifies the overhead of the adaptive machinery. "
        "Combine with --constant-workload to also restrict to a single data regime; otherwise "
        "the alternating workload exposes the dip when the fixed order mismatches the regime. "
        "Mirrors run_bid_value_first_benchmark.py / run_price_first_benchmark.py but keeps "
        "everything in one script so parameter drift between baselines and adaptive can't happen.",
    )
    args = parser.parse_args()

    run_benchmark(
        duration=args.duration,
        skip_build=args.skip_build,
        clean=args.clean,
        output=args.output,
        constant_workload=args.constant_workload,
        sqrts=args.sqrts,
        fixed_variant=args.fixed_variant,
    )
