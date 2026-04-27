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
import os
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    create_folder_and_remove_if_exists,
    get_vcpkg_dir,
    printError,
    printInfo,
    printSuccess,
)

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

#### Query to deploy (nexmark bid-like schema, generator source)
SETUP_SQL = f"""\
CREATE WORKER "{WORKER_GRPC}" SET ('{WORKER_DATA}' AS DATA);
CREATE LOGICAL SOURCE bid(timestamp UINT64 NOT NULL, auctionId INT32 NOT NULL, bidder INT32 NOT NULL, price FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR bid
TYPE Generator
SET(
    'ALL' as `SOURCE`.STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as PARSER.`TYPE`,
    'emit_rate 10' AS `SOURCE`.GENERATOR_RATE_CONFIG,
    10000000 AS `SOURCE`.MAX_RUNTIME_MS,
    1 AS `SOURCE`.SEED,
    'SEQUENCE UINT64 0 10000000 1, SEQUENCE INT32 0 1000 1, SEQUENCE INT32 0 100 1, SEQUENCE FLOAT64 0.0 1000.0 1.0' AS `SOURCE`.GENERATOR_SCHEMA,
    '{WORKER_GRPC}' AS `SOURCE`.HOST
);
CREATE SINK someSink(BID.TIMESTAMP UINT64 NOT NULL, BID.AUCTIONID INT32 NOT NULL, BID.BIDDER INT32 NOT NULL, BID.PRICE FLOAT64 NOT NULL)
TYPE File
SET(
    'out.csv' as `SINK`.FILE_PATH,
    'CSV' as `SINK`.OUTPUT_FORMAT,
    '{WORKER_GRPC}' AS `SINK`.HOST
);
SELECT timestamp, auctionId, bidder, price FROM bid INTO someSink;
"""


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


def run_benchmark(duration: int, skip_build: bool, clean: bool):
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

    # --- Start worker ---
    printInfo(f"Starting nes-single-node-worker (grpc={WORKER_GRPC}, data={WORKER_DATA})...")
    worker_proc = subprocess.Popen(
        [worker_binary, f"--grpc=0.0.0.0:8080", f"--data_address=0.0.0.0:9090"],
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

    # --- Send SQL setup commands ---
    printInfo("Sending SQL setup commands to REPL...")
    try:
        repl_proc.stdin.write(SETUP_SQL.encode())
        repl_proc.stdin.flush()
    except BrokenPipeError:
        printError("REPL stdin closed unexpectedly — did the REPL crash?")
        terminate_process(repl_proc, "nes-repl")
        terminate_process(worker_proc, "nes-single-node-worker")
        sys.exit(1)

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

    printSuccess("Benchmark complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Adaptive optimization benchmark: single worker + distributed REPL."
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="How long (seconds) to let the query run before tearing down (default: 60).",
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
    args = parser.parse_args()

    run_benchmark(duration=args.duration, skip_build=args.skip_build, clean=args.clean)
