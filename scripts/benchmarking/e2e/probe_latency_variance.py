#!/usr/bin/env python3
"""Focused two-experiment probe latency benchmark.

Measures probe latency for build_windows_per_probe_window=1 vs =100 on the
small (100 K row) ClusterMonitoring dataset with 16 worker threads.

Run from the repo root:
    python3 -m scripts.benchmarking.e2e.probe_latency_variance

Produces NO thousands of experiments — exactly 2 probe experiments,
each repeated NUM_REPS times, then reports mean ± stddev per experiment.

The latency is read from the LatencyListener log lines emitted by the worker.
These lines are produced once per task that crosses from pipeline-0 (source)
to pipeline-1 (processing).  For windows=1 each probe tuple triggers real
sketch-cell iteration; for windows=100 (before the StatisticStoreReader fix)
every lookup missed so nothing real happened and the measured values were
pure scheduling noise — producing enormous variance.
"""

import csv
import math
import os
import re
import subprocess
import sys
import tempfile
import time

# --- Paths (repo root must be the cwd) ---------------------------------------
BUILD_DIR = os.environ.get("NES_BUILD_DIR", "cmake-build-release")
WORKER_BIN = os.path.join(BUILD_DIR, "nes-single-node-worker", "nes-single-node-worker")
CLI_BIN = os.path.join(BUILD_DIR, "nes-frontend", "apps", "nes-cli")
LARGE_DATASET = "cmake-build-release/nes-systests/testdata/large/cluster_monitoring/google-cluster-data-original_1G.csv"
SMALL_DATASET = "nes-systests/testdata/small/cluster_monitoring/google-cluster-data-lightsaber_100K.csv"
# Use the 1G dataset when present; fall back to the small one.
DATASET = LARGE_DATASET if os.path.exists(LARGE_DATASET) else SMALL_DATASET

# Benchmark parameters --------------------------------------------------------
STATISTIC_ID = 300
BUILD_WINDOW_SIZE_SEC = 1
MEMORY_BUDGET = 1024
NUM_WORKER_THREADS = 16
NUM_REPS = 20           # repetitions per (windows=1 / windows=100)
NUM_PROBE_TUPLES = 1    # probe tuples per repetition
# Each probe tuple is repeated so the file source fills multiple TupleBuffers,
# giving the LatencyListener more samples per probe run.
# At 4096-byte buffers, 32 bytes/probe-tuple → ~128 tuples/buffer.
# 1000 repetitions → ~8 LatencyListener measurements per probe run.
NUM_PROBE_REPETITIONS = 1000

# -----------------------------------------------------------------------------

sys.path.insert(0, os.path.abspath("."))

from scripts.benchmarking.common.worker_lifecycle import (
    submit_query,
    wait_for_query_to_finish,
    stop_queries_and_wait,
    terminate_process_if_exists,
)
from scripts.benchmarking.common.config import THROUGHPUT_LISTENER_INTERVAL


def _start_worker_direct(log_f, numberOfWorkerThreads, bufferSizeInBytes,
                          buffersInGlobalBufferManager, enableLatency, statisticStoreType, cli_f):
    """Start the worker without systemd-run (Docker-compatible)."""
    cmd = [
        WORKER_BIN,
        "--grpc=localhost:8080",
        "--data_address=localhost:9090",
        f"--worker.query_engine.number_of_worker_threads={numberOfWorkerThreads}",
        "--worker.default_query_execution.execution_mode=COMPILER",
        f"--worker.number_of_buffers_in_global_buffer_manager={buffersInGlobalBufferManager}",
        "--worker.default_query_optimization.join_strategy=HASH_JOIN",
        "--worker.query_engine.admission_queue_size=1000000",
        "--worker.default_query_execution.page_size=8192",
        f"--worker.default_query_execution.operator_buffer_size={bufferSizeInBytes}",
        f"--worker.latency_listener={str(enableLatency).lower()}",
        f"--worker.statistic_store_type={statisticStoreType}",
        f"--worker.throughput_listener_interval_in_ms={THROUGHPUT_LISTENER_INTERVAL}",
    ]
    if cli_f is not None:
        cli_f.write(f"=== Start worker: {' '.join(cmd)} ===\n")
        cli_f.flush()
    proc = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)
    time.sleep(5)
    if proc.poll() is not None:
        raise RuntimeError(f"Worker exited immediately with code {proc.returncode}")
    return proc


def _build_window_size_ms():
    return BUILD_WINDOW_SIZE_SEC * 1000


def _first_timestamp(dataset_path):
    with open(dataset_path) as f:
        return int(f.readline().split(",")[0])


def _first_window_start(dataset_path):
    ts = _first_timestamp(dataset_path)
    w = _build_window_size_ms()
    return (ts // w) * w


def _write_build_yaml(path, dataset_path):
    content = f"""query: SELECT COUNTMINSKETCH({STATISTIC_ID}, userId, {MEMORY_BUDGET}) FROM build_source WINDOW TUMBLING(creationTS, size {BUILD_WINDOW_SIZE_SEC} sec) INTO void_sink;
sinks:
  - name: void_sink
    host: localhost:8080
    schema:
      - name: build_source$statisticid
        type: UINT64
        nullable: false
      - name: build_source$statisticstart
        type: UINT64
        nullable: false
      - name: build_source$statisticend
        type: UINT64
        nullable: false
      - name: build_source$statisticnumberofseentuples
        type: UINT64
        nullable: false
    type: Void
    config: {{}}
    parser_config: {{}}
logical:
  - name: build_source
    schema:
      - name: creationTS
        type: UINT64
        nullable: false
      - name: jobId
        type: UINT64
        nullable: false
      - name: taskId
        type: UINT64
        nullable: false
      - name: machineId
        type: INT64
        nullable: false
      - name: eventType
        type: INT16
        nullable: false
      - name: userId
        type: INT16
        nullable: false
      - name: category
        type: INT16
        nullable: false
      - name: priority
        type: INT16
        nullable: false
      - name: cpu
        type: FLOAT64
        nullable: false
      - name: ram
        type: FLOAT64
        nullable: false
      - name: disk
        type: FLOAT64
        nullable: false
      - name: constraints
        type: BOOLEAN
        nullable: false
physical:
  - logical: build_source
    host: localhost:8080
    type: File
    parser_config:
      type: CSV
    source_config:
      file_path: {os.path.abspath(dataset_path)}
workers:
  - host: localhost:8080
    data_address: localhost:9090
"""
    with open(path, "w") as f:
        f.write(content)


def _write_probe_csv(path, build_windows_per_probe_window, dataset_path):
    bw = _build_window_size_ms()
    pw = bw * build_windows_per_probe_window
    start = _first_window_start(dataset_path)
    rows = []
    for _ in range(NUM_PROBE_REPETITIONS):
        for i in range(NUM_PROBE_TUPLES):
            s = start + i * pw
            rows.append([STATISTIC_ID, s, s + pw, 0])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)
    return len(rows)


def _write_probe_yaml(path, probe_csv_path):
    content = f"""query: |
  SELECT STATISTICSTART, STATISTICEND, rowIndex, columnIndex, counter FROM (
    SELECT COUNTMIN_PROBE({STATISTIC_ID}, uint64)
    FROM probe_source
  ) INTO void_sink;
sinks:
  - name: void_sink
    host: localhost:8080
    schema:
      - name: probe_source$STATISTICSTART
        type: UINT64
        nullable: false
      - name: probe_source$STATISTICEND
        type: UINT64
        nullable: false
      - name: probe_source$rowIndex
        type: UINT64
        nullable: false
      - name: probe_source$columnIndex
        type: UINT64
        nullable: false
      - name: probe_source$counter
        type: UINT64
        nullable: false
    type: Void
    config: {{}}
    parser_config: {{}}
logical:
  - name: probe_source
    schema:
      - name: STATISTICID
        type: UINT64
        nullable: false
      - name: STATISTICSTART
        type: UINT64
        nullable: false
      - name: STATISTICEND
        type: UINT64
        nullable: false
      - name: STATISTICNUMBEROFSEENTUPLES
        type: UINT64
        nullable: false
physical:
  - logical: probe_source
    host: localhost:8080
    type: File
    parser_config:
      type: CSV
    source_config:
      file_path: {os.path.abspath(probe_csv_path)}
workers:
  - host: localhost:8080
    data_address: localhost:9090
"""
    with open(path, "w") as f:
        f.write(content)


_LAT_PAT = re.compile(
    r'Latency for queryId QueryId\([^)]*\) and \d+ tasks over duration \d+-\d+ is (\d+\.\d+) (\w*)s'
)
_UNIT = {"": 1.0, "m": 1e-3, "u": 1e-6, "n": 1e-9}


def _parse_all_latencies(log_path, query_id):
    """Return every individual latency measurement (seconds) for query_id."""
    data = []
    try:
        with open(log_path) as f:
            for line in f:
                if str(query_id) not in line:
                    continue
                m = _LAT_PAT.search(line)
                if m:
                    data.append(float(m.group(1)) * _UNIT.get(m.group(2), 1.0))
    except FileNotFoundError:
        pass
    return data


def _wait_for_measurements_and_stop(log_path, query_ids, probe_yaml, cli_f,
                                     settle_s=3.0, max_wait_s=30):
    """Poll the worker log until no new latency lines appear for settle_s seconds, then stop."""
    deadline = time.time() + max_wait_s
    last_count = 0
    stable_since = time.time()
    while time.time() < deadline:
        cur = sum(len(_parse_all_latencies(log_path, qid)) for qid in query_ids)
        if cur > last_count:
            last_count = cur
            stable_since = time.time()
        elif cur > 0 and (time.time() - stable_since) >= settle_s:
            break
        time.sleep(0.5)
    stop_queries_and_wait(query_ids, probe_yaml, cli_f, timeout=15)


def _stats(values):
    if not values:
        return float("nan"), float("nan"), 0
    n = len(values)
    mean = sum(values) / n
    if n > 1:
        var = sum((x - mean) ** 2 for x in values) / (n - 1)
        std = math.sqrt(var)
    else:
        std = 0.0
    return mean, std, n


def _fmt_us(secs):
    return f"{secs * 1e6:.1f} μs"


def main():
    if not os.path.exists(WORKER_BIN):
        sys.exit(f"Worker binary not found: {WORKER_BIN}\n"
                 "Build it first: docker run ... cmake --build cmake-build-debug --target nes-single-node-worker nes-repl")
    if not os.path.exists(DATASET):
        sys.exit(f"Dataset not found: {DATASET}")
    print(f"Dataset: {DATASET}")

    workdir = tempfile.mkdtemp(prefix="probe_variance_", dir=os.getcwd())
    log_path = os.path.join(workdir, "worker.log")
    cli_log_path = os.path.join(workdir, "cli.log")
    print(f"Working directory: {workdir}")

    build_yaml = os.path.join(workdir, "build.yaml")
    _write_build_yaml(build_yaml, DATASET)

    results = {}

    with open(log_path, "w") as log_f, open(cli_log_path, "w") as cli_f:
        print(f"\nStarting worker ({NUM_WORKER_THREADS} threads, latency listener ON) …")
        worker = _start_worker_direct(
            log_f,
            numberOfWorkerThreads=NUM_WORKER_THREADS,
            bufferSizeInBytes=32768,       # 32 KB: ~819 cells/buf → far fewer output buffers needed
            buffersInGlobalBufferManager=20000,
            enableLatency=True,
            statisticStoreType="SUB_STORES",
            cli_f=cli_f,
        )
        time.sleep(6)

        try:
            # Phase 1: Build — submit, wait for data to flow (fixed time), then stop.
            # Event-time windows don't auto-close on EOF so we can't rely on "Stopped" status.
            print("Submitting build query …")
            build_ids = submit_query(build_yaml, cli_f)
            time.sleep(90)          # 18.6 M rows; 90s is conservative for Release build on 16 threads
            stop_queries_and_wait(build_ids, build_yaml, cli_f, timeout=30)
            print("Build done.")
            time.sleep(2)           # let the store settle

            # Phase 2: Probe — windows=1 then windows=100
            for bwpw in [1, 100]:
                probe_csv = os.path.join(workdir, f"probe_{bwpw}x.csv")
                probe_yaml = os.path.join(workdir, f"probe_{bwpw}x.yaml")
                nrows = _write_probe_csv(probe_csv, bwpw, DATASET)
                _write_probe_yaml(probe_yaml, probe_csv)
                print(f"\nProbing windows={bwpw} ({nrows} CSV rows, {NUM_REPS} repetitions) …")

                all_latencies = []
                for rep in range(NUM_REPS):
                    seen_before = sum(len(_parse_all_latencies(log_path, "_")) for _ in [""])
                    probe_ids = submit_query(probe_yaml, cli_f)
                    # Wait until measurements appear and stabilise, then stop.
                    _wait_for_measurements_and_stop(log_path, probe_ids, probe_yaml, cli_f)
                    lats = _parse_all_latencies(log_path, probe_ids[0]) if probe_ids else []
                    all_latencies.extend(lats)
                    status = "hit" if lats else "MISS (no latency measurements)"
                    print(f"  rep {rep+1}/{NUM_REPS}: {len(lats)} measurements  {status}")
                    time.sleep(1)  # brief pause between reps

                results[bwpw] = all_latencies

        finally:
            terminate_process_if_exists(worker)

    print("\n" + "=" * 60)
    print("Probe latency summary (Debug build, small 100 K dataset)")
    print(f"  worker threads: {NUM_WORKER_THREADS}")
    print(f"  execution mode: COMPILER")
    print(f"  build window  : {BUILD_WINDOW_SIZE_SEC} sec")
    print(f"  repetitions   : {NUM_REPS}")
    print("=" * 60)
    for bwpw in [1, 100]:
        vals = results.get(bwpw, [])
        mean, std, n = _stats(vals)
        cv = (std / mean * 100) if mean > 0 else float("nan")
        print(f"  windows={bwpw:3d}: n={n:4d}  mean={_fmt_us(mean)}  stddev={_fmt_us(std)}  CV={cv:.1f}%")
    print("=" * 60)
    print(f"\nFull data written to: {workdir}")


if __name__ == "__main__":
    main()
