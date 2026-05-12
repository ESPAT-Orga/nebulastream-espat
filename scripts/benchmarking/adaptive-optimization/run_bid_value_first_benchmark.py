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
Single-query baseline benchmark: bidValue-filter first.

Deploys the bidValue-first variant of the adaptive-optimization data query without any
companion statistic query or adaptive swap. Records firstPipeline throughput to CSV so the
result can be compared head-to-head with the price-first variant (run_price_first_benchmark.py).

Usage (run from repository root):
    python -m scripts.benchmarking.adaptive-optimization.run_bid_value_first_benchmark
    python -m scripts.benchmarking.adaptive-optimization.run_bid_value_first_benchmark --duration 60
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
# At ~400 MTup/s and 30M-row datasets one pass is ~75 ms, so 130 ≈ 10 s of one regime.
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

# bidValue-first ordering: cheap selective filter (bidValue) runs first, then price filter.
# Both filters go into their own pipelines because FUSE=FALSE. Memory source alternates
# between regime-A (bid-filter selective, this query's cheap regime) and regime-B (bid-filter
# non-selective, where this query's order is suboptimal).
def _expensive_filter_clause(sqrts: int) -> str:
    """Per-tuple SQRT chain summed > 0 (always passes). Empty for sqrts <= 0."""
    if sqrts <= 0:
        return ""
    terms = " + ".join(
        f"SQRT({'bidValue' if i % 2 == 0 else 'price'} + FLOAT64({1000 + i // 2}))"
        for i in range(sqrts)
    )
    return f"{terms} > FLOAT64(0.0)"


def make_setup_sql(data_path_a: str, data_path_b: str, sqrts: int) -> str:
    expensive = _expensive_filter_clause(sqrts)
    if expensive:
        select_block = f"""\
SELECT timestamp, auctionId, bidValue, price
FROM (
  SELECT timestamp, auctionId, bidValue, price
  FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE bidValue < FLOAT64(10.45))
  WHERE {expensive}
)
WHERE price < FLOAT64(888.49)
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""
    else:
        select_block = """\
SELECT timestamp, auctionId, bidValue, price
FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE bidValue < FLOAT64(10.45))
WHERE price < FLOAT64(888.49)
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""
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

_THROUGHPUT_RE = re.compile(
    r"Throughput for queryId QueryId\(local=[^,)]+, distributed=([^)]+)\)"
    r" in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s"
)

_JSON_QUERY_ID_RE = re.compile(r'^\[{"query_id":\s*"([^"]+)"}]')


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


def find_data_query_id(repl_lines, timeout=60.0):
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


def parse_throughput(worker_lines, data_query_id):
    measurements = []
    for line in worker_lines:
        m = _THROUGHPUT_RE.search(line)
        if m and m.group(1) == data_query_id:
            window_start = int(m.group(2))
            throughput = convert_unit_prefix(float(m.group(4)), m.group(5))
            measurements.append((window_start, m.group(1), throughput))
    return measurements


def write_throughput_csv(measurements, output_path):
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

    data_path_a = ensure_dataset_a(path=DEFAULT_OUTPUT_A)
    data_path_b = ensure_dataset_b(path=DEFAULT_OUTPUT_B)
    setup_sql = make_setup_sql(data_path_a, data_path_b, sqrts)
    printInfo(f"Using {sqrts} SQRT operators between filters.")

    printInfo(f"Starting nes-single-node-worker (grpc={WORKER_GRPC}, data={WORKER_DATA})...")
    worker_proc = subprocess.Popen(
        [
            worker_binary,
            "--grpc=0.0.0.0:8080",
            "--data_address=0.0.0.0:9090",
            # 4 MB buffers so the parsed 60M-tuple dataset (~1.68 GB row-layout) fits in ~420
            # buffers — well within the 1024-buffer pool. At 65 KB each it needed ~25k buffers,
            # blowing the pool at setup.
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

    printInfo("Starting nes-repl (distributed mode, no companion)...")
    repl_proc = subprocess.Popen(
        [repl_binary, "-f", "JSON"],
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

    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        printInfo("Interrupted by user — tearing down early.")

    printInfo("Tearing down...")
    terminate_process(repl_proc, "nes-repl")
    terminate_process(worker_proc, "nes-single-node-worker")

    repl_thread.join(timeout=5)
    worker_thread.join(timeout=5)

    measurements = parse_throughput(worker_lines, data_query_id)
    write_throughput_csv(measurements, output)

    printSuccess("Benchmark complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Single-query baseline: bidValue-filter first (no companion / no adaptive swap)."
    )
    parser.add_argument("--duration", type=int, default=120, help="Run duration in seconds (default: 120).")
    parser.add_argument("--skip-build", action="store_true", help="Skip the cmake configure + build step.")
    parser.add_argument("--clean", action="store_true", help="Remove and recreate the build directory before building.")
    parser.add_argument(
        "--output",
        default="data_throughput_bid_value_first.csv",
        help="Path for the throughput CSV output (default: data_throughput_bid_value_first.csv).",
    )
    parser.add_argument(
        "--sqrts",
        type=int,
        default=0,
        help="Number of SQRT operators to insert between the two filters (default: 0). The SQRT chain "
        "is an always-true WHERE adding per-tuple CPU cost in the middle pipeline.",
    )
    args = parser.parse_args()
    run_benchmark(
        duration=args.duration,
        skip_build=args.skip_build,
        clean=args.clean,
        output=args.output,
        sqrts=args.sqrts,
    )
