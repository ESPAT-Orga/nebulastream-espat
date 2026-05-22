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
# At ~400 MTup/s and 30M-row datasets one pass is ~75 ms, so 130 ≈ 10 s of one regime —
# enough for the adaptive optimizer to notice a histogram shift and trigger a swap.
REPLAYS_PER_FILE = 130

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
# The SQRT-based expensive intermediate filter has been removed for now while we get the
# plumbing working; reintroduce it (see git history for the 150-SQRT _EXPENSIVE_FILTER) once
# the baseline measures cleanly.

def make_setup_sql(data_path_a: str, data_path_b: str) -> str:
    return f"""\
CREATE WORKER "{WORKER_GRPC}" SET ('{WORKER_DATA}' AS DATA);
CREATE LOGICAL SOURCE bid(timestamp UINT64 NOT NULL, auctionId INT32 NOT NULL, bidValue FLOAT64 NOT NULL, price FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR bid
TYPE Memory
SET(
    'NATIVE' as PARSER.`TYPE`,
    '{data_path_a}' AS `SOURCE`.FILE_PATH,
    '{data_path_b}' AS `SOURCE`.FILE_PATH_2,
    '{REPLAYS_PER_FILE}' AS `SOURCE`.REPLAYS_PER_FILE,
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
SELECT timestamp, auctionId, bidValue, price
FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE bidValue < FLOAT64(10.45))
WHERE price < FLOAT64(888.49)
INTO someSink
SET (FALSE as `QUERY`.FUSE);
"""

# Same query with filter order reversed (price first, then bidValue).
REVERSED_QUERY_SQL = (
    "SELECT timestamp, auctionId, bidValue, price "
    "FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE price < FLOAT64(888.49)) "
    "WHERE bidValue < FLOAT64(10.45) "
    "INTO someSink "
    "SET (FALSE as `QUERY`.FUSE);"
)

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
    """Return list of (window_start_ms, query_id, throughput_tup_per_s) for all data queries."""
    measurements = []
    for line in worker_lines:
        m = _THROUGHPUT_RE.search(line)
        if m and m.group(1) in data_query_ids:
            window_start = int(m.group(2))
            throughput = convert_unit_prefix(float(m.group(4)), m.group(5))
            measurements.append((window_start, m.group(1), throughput))
    return measurements


def write_throughput_csv(measurements, output_path):
    """Write all per-window throughput samples to a CSV for time-series plotting."""
    if not measurements:
        printError("No throughput data collected for the data query.")
        return
    min_ts = measurements[0][0]
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "query_id", "throughput_tup_per_s"])
        for ts, qid, tput in measurements:
            writer.writerow([ts - min_ts, qid, tput])
    printSuccess(f"Throughput data ({len(measurements)} samples) written to {os.path.abspath(output_path)}")


def run_benchmark(duration: int, skip_build: bool, clean: bool, output: str):
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
    data_path_b = ensure_dataset_b(path=DEFAULT_OUTPUT_B)
    setup_sql = make_setup_sql(data_path_a, data_path_b)

    # --- Start worker ---
    printInfo(f"Starting nes-single-node-worker (grpc={WORKER_GRPC}, data={WORKER_DATA})...")
    worker_proc = subprocess.Popen(
        [
            worker_binary,
            "--grpc=0.0.0.0:8080",
            "--data_address=0.0.0.0:9090",
            "--worker.default_query_execution.operator_buffer_size=4194304",
            "--worker.number_of_buffers_in_global_buffer_manager=1024",
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

    # --- Start REPL ---
    printInfo("Starting nes-repl (distributed mode)...")
    repl_proc = subprocess.Popen(
        [
            repl_binary,
            "-f", "JSON",
            "--companion-statistic",
            "--companion-source", "bid",
            "--companion-field", "price",
            "--companion-metric", "Cardinality",
            # With BOTH data query and companion running, each query sees ~30 MTup/s on its
            # firstPipeline (vs ~390 MTup/s when only the data query was deployed). With
            # MONOTONIC_TIMESTAMP_FIELD on, BID$TIMESTAMP advances 1 per tuple, so 300 M units
            # ≈ 10 s wall-clock at 30 MTup/s. Tune as needed.
            "--companion-window-size-ms", "300000000",
            "--companion-event-time-field", "BID$TIMESTAMP",
            "--companion-host", WORKER_GRPC,
            "--companion-switch-to-sql", REVERSED_QUERY_SQL,
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
    args = parser.parse_args()

    run_benchmark(duration=args.duration, skip_build=args.skip_build, clean=args.clean, output=args.output)
