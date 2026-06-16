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
Swap-cost-only benchmark: same redeployment loop as the adaptive script, but with a
single, non-changing input distribution. Used to visualize the bare cost of periodic
query swaps independent of any actual workload shift.

Compared to run_adaptive_optimization_benchmark.py the only differences are:
  - the Memory source loads exactly one dataset (regime A); no FILE_PATH_2; no
    MILLIS_PER_FILE — every replayed pass produces the same distribution.
  - all other plumbing (MONOTONIC_TIMESTAMP_FIELD, LOOP, companion, REVERSED_QUERY_SQL,
    --companion-switch-to-sql, window size) is identical so the swap cadence matches.

The companion will still fire every ~N event-time units and trigger query swaps; the
throughput curve will show the gaps / dips caused by those redeployments, not by any
data-distribution change.

Usage (run from repository root):
    python -m scripts.benchmarking.adaptive-optimization.run_constant_workload_swap_benchmark
    python -m scripts.benchmarking.adaptive-optimization.run_constant_workload_swap_benchmark --duration 120
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_bid_data import DEFAULT_OUTPUT_A, ensure_dataset_a

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


def _expensive_filter_clause(sqrts: int) -> str:
    """Builds a per-tuple SQRT chain whose sum is trivially > 0 (always passes), so it can be
    used as an intermediate `WHERE` between the two real filters to make the middle pipeline
    CPU-bound. Each argument is `col + 1000+i/2` to guarantee positive SQRT inputs regardless of
    the field's distribution. Returns an empty string for sqrts <= 0 — callers must skip the
    enclosing subquery in that case so the original two-filter SQL is unchanged.
    """
    if sqrts <= 0:
        return ""
    terms = " + ".join(
        f"SQRT({'bidValue' if i % 2 == 0 else 'price'} + FLOAT64({1000 + i // 2}))"
        for i in range(sqrts)
    )
    return f"{terms} > FLOAT64(0.0)"


def make_setup_sql(data_path: str, sqrts: int) -> str:
    """Same SQL shape as the adaptive script, but with only one Memory-source file."""
    expensive = _expensive_filter_clause(sqrts)
    if expensive:
        select_block = f"""\
SELECT timestamp, auctionId, bidValue, price
FROM (
  SELECT timestamp, auctionId, bidValue, price
  FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE bidValue < FLOAT64(20.45))
  WHERE {expensive}
)
WHERE price < FLOAT64(888.49)
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""
    else:
        select_block = """\
SELECT timestamp, auctionId, bidValue, price
FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE bidValue < FLOAT64(20.45))
WHERE price < FLOAT64(888.49)
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""
    return f"""\
CREATE WORKER "{WORKER_GRPC}" SET ('{WORKER_DATA}' AS DATA);
CREATE LOGICAL SOURCE bid(timestamp UINT64 NOT NULL, auctionId INT32 NOT NULL, bidValue FLOAT64 NOT NULL, price FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR bid
TYPE LoopingMemory
SET(
    'NATIVE' as PARSER.`TYPE`,
    '{data_path}' AS `SOURCE`.FILE_PATH,
    'timestamp' AS `SOURCE`.MONOTONIC_TIMESTAMP_FIELD,
    'true' AS `SOURCE`.LOOP,
    '{WORKER_GRPC}' AS `SOURCE`.HOST
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
    """Filter-reversed alternate that the workload-switch flow attaches as the alternate
    pipeline. Same SQRT injection point as the data query."""
    expensive = _expensive_filter_clause(sqrts)
    if expensive:
        return (
            "SELECT timestamp, auctionId, bidValue, price "
            "FROM ("
            "SELECT timestamp, auctionId, bidValue, price "
            "FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE price < FLOAT64(888.49)) "
            f"WHERE {expensive}"
            ") "
            "WHERE bidValue < FLOAT64(20.45) "
            "INTO someSink "
            "SET (FALSE as `QUERY`.FUSE);"
        )
    return (
        "SELECT timestamp, auctionId, bidValue, price "
        "FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE price < FLOAT64(888.49)) "
        "WHERE bidValue < FLOAT64(20.45) "
        "INTO someSink "
        "SET (FALSE as `QUERY`.FUSE);"
    )

_THROUGHPUT_RE = re.compile(
    r"Throughput for queryId QueryId\(local=[^,)]+, distributed=([^)]+)\)"
    r" in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s"
)

_JSON_QUERY_ID_RE = re.compile(r'^\[{"query_id":\s*"([^"]+)"}]')

_ADAPTIVE_QUERY_ID_RE = re.compile(r'\[AdaptiveOpt\] Deployed query \(id=([^)]+)\)')


def stream_output(proc, label, lines_out):
    for line in iter(proc.stdout.readline, b""):
        decoded = line.decode(errors="replace").rstrip()
        print(f"[{label}] {decoded}", flush=True)
        lines_out.append(decoded)


def wait_for_port(host, port, timeout=10.0, interval=0.2):
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


def find_data_query_id(repl_lines, timeout=300.0):
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
    adaptive swap), otherwise "stat" — statistic-collection queries don't print a distributed
    query ID to REPL stdout so they fall through to the "stat" bucket.
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


def run_benchmark(duration: int, skip_build: bool, clean: bool, output: str, sqrts: int = 0):
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

    data_path = ensure_dataset_a(path=DEFAULT_OUTPUT_A)
    setup_sql = make_setup_sql(data_path, sqrts)
    printInfo(f"Using {sqrts} SQRT operators between filters.")

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

    printInfo("Starting nes-repl (distributed mode)...")
    repl_proc = subprocess.Popen(
        [
            repl_binary,
            "-f", "JSON",
            "--companion-statistic",
            # Splice the build branch into the data query so both filter chains run side by side
            # under one shared source, gated by a SwitchRegistry atomic that the swap callback
            # flips via gRPC SetSwitch (no stop/redeploy, source thread keeps running).
            "--companion-field", "price",
            # Selectivity maps to Equi_Width_Histogram (same as MinVal/MaxVal), the only metric the
            # histogram-probe operator can read. Cardinality (Count_Min_Sketch) wouldn't work with
            # the gated probe path.
            "--companion-metric", "Selectivity",
            # 60 M event-time ms — at the steady-state ingest rate of ~200 M tup/s the histogram
            # closes ~3× per wall-clock second, low enough to keep the statistic store bounded
            # while frequent enough that gated-probe trigger fires become observable within a
            # ~30 s bench. Smaller windows have OOM'd the worker (event time advances at the
            # tuple-emit rate).
            "--companion-window-size-ms", "60000000",
            "--companion-event-time-field", "BID$TIMESTAMP",
            "--companion-host", WORKER_GRPC,
            "--companion-switch-to-sql", make_reversed_query_sql(sqrts),
            # Selectivity-gated probe: fires when any histogram bin has > 0 tuples.
            "--companion-condition", "BINCOUNTER > UINT64(0)",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    repl_lines = []
    repl_thread = threading.Thread(
        target=stream_output, args=(repl_proc, "REPL", repl_lines), daemon=True
    )
    repl_thread.start()

    printInfo("Sending SQL setup commands to REPL...")
    try:
        repl_proc.stdin.write(setup_sql.encode())
        repl_proc.stdin.flush()
    except BrokenPipeError:
        printError("REPL stdin closed unexpectedly — did the REPL crash?")
        terminate_process(repl_proc, "nes-repl")
        terminate_process(worker_proc, "nes-single-node-worker")
        sys.exit(1)

    printInfo("Waiting for data query deployment confirmation...")
    data_query_id = find_data_query_id(repl_lines, timeout=300.0)
    if data_query_id is None:
        printError("Timed out waiting for the SELECT query response — REPL may have crashed.")
        terminate_process(repl_proc, "nes-repl")
        terminate_process(worker_proc, "nes-single-node-worker")
        sys.exit(1)
    printSuccess(f"Data query deployed with id: {data_query_id}")

    printSuccess(f"Query deployed. Running for {duration} seconds...")

    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        printInfo("Interrupted by user — tearing down early.")

    printInfo("Tearing down...")
    terminate_process(repl_proc, "nes-repl")
    terminate_process(worker_proc, "nes-single-node-worker")

    repl_thread.join(timeout=5)
    worker_thread.join(timeout=5)

    data_query_ids = collect_all_data_query_ids(repl_lines)
    printInfo(f"Data query IDs observed across all deployments: {data_query_ids}")
    measurements = parse_throughput(worker_lines, data_query_ids)
    write_throughput_csv(measurements, output)

    printSuccess("Benchmark complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Swap-cost-only benchmark: same redeployment loop as adaptive, constant workload."
    )
    parser.add_argument("--duration", type=int, default=120, help="Run duration in seconds (default: 120).")
    parser.add_argument("--skip-build", action="store_true", help="Skip the cmake configure + build step.")
    parser.add_argument("--clean", action="store_true", help="Remove and recreate the build directory before building.")
    parser.add_argument(
        "--output",
        default="data_throughput_constant_workload_swap.csv",
        help="Path for the throughput CSV output (default: data_throughput_constant_workload_swap.csv).",
    )
    parser.add_argument(
        "--sqrts",
        type=int,
        default=0,
        help="Number of SQRT operators to insert between the two filters (default: 0). The SQRT chain "
        "is an always-true WHERE that adds per-tuple CPU cost in the middle pipeline — useful for "
        "exposing throughput differences between filter orderings.",
    )
    args = parser.parse_args()
    run_benchmark(
        duration=args.duration,
        skip_build=args.skip_build,
        clean=args.clean,
        output=args.output,
        sqrts=args.sqrts,
    )
