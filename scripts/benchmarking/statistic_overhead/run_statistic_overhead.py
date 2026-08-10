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

"""statistic_overhead — how much analytical throughput does statistic collection cost?

Holds a fixed analytical workload (N concurrent ClusterMonitoring Q2 sliding-window aggregations)
and sweeps how many of them carry a spliced equi-width histogram over their GROUP BY key. Because
the histogram rides on the query's own source, the ingestion load is identical at every point of the
sweep — so a change in analytical throughput is the cost of maintaining the synopsis, nothing else.
The N=0 arm is the baseline.

One run = one (num_statistic_queries, run_idx) pair: start the producer, start the worker, start one
nes-repl per analytical query (k of them with a companion), let it reach steady state, then read
per-query throughput back out of the worker's throughput listener.

Usage (from the repository root):
    python3 -m scripts.benchmarking.statistic_overhead.run_statistic_overhead --output-dir <dir>
"""

import argparse
import csv
import os
import re
import statistics
import subprocess
import time
from collections import defaultdict

from scripts.benchmarking.common.worker_lifecycle import (
    check_log_for_buffer_exhaustion,
    dump_worker_log_tail,
    start_single_node_worker,
    terminate_process_if_exists,
)
from scripts.benchmarking.statistic_overhead import config
from scripts.benchmarking.statistic_overhead.shared_submission import shutdown_shared, submit_shared
from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    convert_unit_prefix,
    create_folder_and_remove_if_exists,
    printError,
    printInfo,
    printSuccess,
)

ROLE_ANALYTICAL = "analytical"
ROLE_STATISTIC = "statistic"


## Query construction ##########################################################

## Producer ############################################################

def ensure_generator_image():
    """Build the producer image if it is missing. Idempotent; the shell wrapper also does this."""
    probe = subprocess.run(["docker", "image", "inspect", config.GENERATOR_IMAGE],
                           capture_output=True)
    if probe.returncode == 0:
        return
    printInfo(f"Building {config.GENERATOR_IMAGE} from {config.GENERATOR_BUILD_CONTEXT}")
    subprocess.run(["docker", "build", "-t", config.GENERATOR_IMAGE,
                    config.GENERATOR_BUILD_CONTEXT], check=True)


def stop_generator(log_path=None):
    """Remove the producer container, first saving its logs when a path is given.

    The logs must be captured here, not at READY: the accept/disconnect lines that show whether every
    query's sources actually connected are only written once the worker dials in.
    """
    if log_path is not None:
        logs = subprocess.run(["docker", "logs", config.GENERATOR_CONTAINER],
                              capture_output=True, text=True)
        with open(log_path, "w") as f:
            f.write(logs.stdout + logs.stderr)
    subprocess.run(["docker", "rm", "-f", config.GENERATOR_CONTAINER], capture_output=True)


def start_generator(log_path, tuples_per_sec):
    """Start the producer and block until it prints READY, i.e. both ports are bound."""
    stop_generator()
    cmd = [
        "docker", "run", "-d", "--name", config.GENERATOR_CONTAINER,
        "--network", config.GENERATOR_DOCKER_NETWORK,
        config.GENERATOR_IMAGE,
        "--port-base", str(config.GENERATOR_PORT_BASE),
        "--events-per-sec", str(config.EVENTS_PER_SEC),
        "--job-domain", str(config.JOB_DOMAIN),
        "--seed", str(config.GENERATOR_SEED),
        "--tuples-per-sec", str(tuples_per_sec),
    ]
    subprocess.run(cmd, check=True, capture_output=True)

    deadline = time.time() + config.GENERATOR_READY_TIMEOUT_S
    while time.time() < deadline:
        logs = subprocess.run(["docker", "logs", config.GENERATOR_CONTAINER],
                              capture_output=True, text=True)
        combined = logs.stdout + logs.stderr
        if "READY" in combined:
            return
        time.sleep(0.5)

    stop_generator(log_path)
    raise RuntimeError(f"generator did not report READY within "
                       f"{config.GENERATOR_READY_TIMEOUT_S}s; see {log_path}")


## Throughput parsing ##########################################################

# Matches the ThroughputListener output in SingleNodeWorker.cpp. The `distributed=` name is the id
# nes-cli returned, so it is the join key between submitted queries and measurements.
THROUGHPUT_RE = re.compile(
    r'Throughput for queryId QueryId\(local=[\w-]+, distributed=(\w+)\) '
    r'in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s'
)


def parse_throughput_samples(log_path):
    """Return [(query_id, window_start_ms, throughput_tps)] from the worker stdout log."""
    samples = []
    try:
        with open(log_path) as f:
            for line in f:
                match = THROUGHPUT_RE.search(line)
                if match:
                    samples.append((
                        match.group(1),
                        int(match.group(2)),
                        convert_unit_prefix(float(match.group(4)), match.group(5)),
                    ))
    except FileNotFoundError:
        printError(f"Worker log {log_path} not found.")
    return samples


def steady_state_means(samples, warmup_seconds):
    """Mean throughput per query over the steady-state part of the run.

    Timestamps are normalized against the earliest window seen, so anything within the warm-up —
    query compilation, source connect, ramp-up — is dropped. The last window of each query may be
    partial (it is flushed on QueryStop), so it goes too whenever there is more than one.
    """
    if not samples:
        return {}
    t0 = min(start for _, start, _ in samples)
    per_query = defaultdict(list)
    for query_id, start, tps in samples:
        if start - t0 >= warmup_seconds * 1000:
            per_query[query_id].append((start, tps))

    means = {}
    for query_id, entries in per_query.items():
        values = [tps for _, tps in sorted(entries)]
        if len(values) > 1:
            values = values[:-1]
        if values:
            means[query_id] = statistics.fmean(values)
    return means


def write_per_query_csv(samples, roles, csv_path):
    if not samples:
        return
    t0 = min(start for _, start, _ in samples)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(config.PER_QUERY_FIELDNAMES)
        for query_id, start, tps in sorted(samples, key=lambda s: (s[0], s[1])):
            writer.writerow([query_id, roles.get(query_id, "unknown"), start, start - t0, tps])


## One measurement #############################################################

def run_one(num_statistic_queries, run_idx, output_dir, tuples_per_sec):
    """Execute a single (tuples_per_sec, num_statistic_queries, run_idx) combination."""
    run_dir = os.path.join(output_dir,
                           f"tps_{tuples_per_sec}_stats_{num_statistic_queries:02d}_run_{run_idx}")
    create_folder_and_remove_if_exists(run_dir)
    worker_log = os.path.join(run_dir, "worker.log")
    generator_log = os.path.join(run_dir, "generator.log")
    cli_log_path = os.path.join(run_dir, "nes-cli.log")

    row = {
        'num_statistic_queries': num_statistic_queries,
        'run_idx': run_idx,
        'num_analytical_queries': config.NUM_ANALYTICAL_QUERIES,
        'num_worker_threads': config.WORKER_THREADS,
        'window_size_sec': config.WINDOW_SIZE_SEC,
        'window_advance_sec': config.WINDOW_ADVANCE_SEC,
        'job_domain': config.JOB_DOMAIN,
        'events_per_sec': config.EVENTS_PER_SEC,
        'tuples_per_sec_per_query': tuples_per_sec,
        'offered_tps': config.offered_tps(tuples_per_sec, num_statistic_queries),
    }
    issues = []

    worker_process = None
    analytical_ids = []
    repl_procs, repl_logs = [], []
    with open(cli_log_path, "w") as cli_log, open(worker_log, "w") as worker_stdout:
        try:
            start_generator(generator_log, tuples_per_sec)
            worker_process = start_single_node_worker(
                worker_stdout,
                numberOfWorkerThreads=config.WORKER_THREADS,
                executionMode=config.EXECUTION_MODE,
                joinStrategy=config.JOIN_STRATEGY,
                pageSize=config.PAGE_SIZE,
                bufferSizeInBytes=config.BUFFER_SIZE_IN_BYTES,
                buffersInGlobalBufferManager=config.BUFFERS_IN_GLOBAL_BUFFER_MANAGER,
                enableLatency=config.ENABLE_LATENCY,
                statisticStoreType=config.STATISTIC_STORE_TYPE,
                cli_log_file=cli_log,
                throughput_listener_interval_in_ms=config.THROUGHPUT_LISTENER_INTERVAL_MS,
                use_systemd_run=config.USE_SYSTEMD_RUN,
            )
            time.sleep(config.WAIT_AFTER_WORKER_START)

            # One nes-repl per analytical query; histograms ride along as spliced companions, so
            # the connection count stays at NUM_ANALYTICAL_QUERIES for every N.
            repl_procs, _, repl_logs, analytical_ids = submit_shared(num_statistic_queries, run_dir)
            if len(analytical_ids) != config.NUM_ANALYTICAL_QUERIES:
                issues.append(f"only {len(analytical_ids)}/{config.NUM_ANALYTICAL_QUERIES} "
                              f"analytical queries deployed")

            time.sleep(config.WARMUP_SECONDS + config.MEASUREMENT_WINDOW_SECONDS)

            if worker_process.poll() is not None:
                raise RuntimeError(f"worker exited with code {worker_process.returncode}")

            # Stop before tearing the worker down: QueryStop flushes the listener's pending windows.
            shutdown_shared(repl_procs, repl_logs)
            time.sleep(2)
        finally:
            if worker_process is not None:
                terminate_process_if_exists(worker_process)
            stop_generator(generator_log)

    samples = parse_throughput_samples(worker_log)
    analytical_set = set(analytical_ids)
    # Any query the worker reports that is not one of the analytical queries. In practice this is
    # EMPTY even when the companions are working: the throughput listener measures "the first
    # pipeline after the source", and a spliced build branch consumes the data query's source thread
    # rather than owning one, so it never emits a throughput event. statistic_throughput_tps is
    # therefore not evidence either way — do not treat 0 as a failure. Confirming the histogram is
    # populated needs the statistic store, not this listener.
    statistic_ids = {qid for qid, _, _ in samples if qid not in analytical_set}
    roles = {qid: ROLE_ANALYTICAL for qid in analytical_set}
    roles.update({qid: ROLE_STATISTIC for qid in statistic_ids})
    write_per_query_csv(samples, roles, os.path.join(
        output_dir,
        f"per_query_throughput_{tuples_per_sec}_{num_statistic_queries:02d}_{run_idx}.csv"))

    means = steady_state_means(samples, config.WARMUP_SECONDS)
    analytical = [tps for qid, tps in means.items() if qid in analytical_set]
    statistic = [tps for qid, tps in means.items() if qid in statistic_ids]

    if not analytical:
        issues.append("no analytical throughput measured")
        dump_worker_log_tail(worker_log)
    if len(analytical) != config.NUM_ANALYTICAL_QUERIES:
        issues.append(f"only {len(analytical)}/{config.NUM_ANALYTICAL_QUERIES} analytical queries measured")
    if check_log_for_buffer_exhaustion(worker_log):
        issues.append("buffer exhaustion")

    row['analytical_throughput_tps'] = sum(analytical) if analytical else -1
    row['analytical_throughput_median_tps'] = statistics.median(analytical) if analytical else -1
    row['num_analytical_measured'] = len(analytical)
    row['statistic_throughput_tps'] = sum(statistic) if statistic else 0
    row['num_statistic_measured'] = len(statistic)
    row['issue'] = ";".join(issues) if issues else "ok"
    return row


## Driver ######################################################################

def _failure_row(num_statistic_queries, run_idx, tuples_per_sec, message):
    row = {name: "" for name in config.FIELDNAMES}
    row.update({
        'num_statistic_queries': num_statistic_queries,
        'run_idx': run_idx,
        'num_analytical_queries': config.NUM_ANALYTICAL_QUERIES,
        'num_worker_threads': config.WORKER_THREADS,
        'window_size_sec': config.WINDOW_SIZE_SEC,
        'window_advance_sec': config.WINDOW_ADVANCE_SEC,
        'job_domain': config.JOB_DOMAIN,
        'events_per_sec': config.EVENTS_PER_SEC,
        'tuples_per_sec_per_query': tuples_per_sec,
        'offered_tps': config.offered_tps(tuples_per_sec, num_statistic_queries),
        'issue': f"exception:{message}",
    })
    return row


def main():
    parser = argparse.ArgumentParser(description="statistic_overhead benchmark")
    parser.add_argument("--output-dir", default=".", help="Directory for results and per-run logs")
    parser.add_argument("--statistic-query-counts", type=int, nargs="+",
                        default=config.STATISTIC_QUERY_COUNTS,
                        help="Numbers of statistic queries to sweep")
    parser.add_argument("--tuples-per-sec", type=int, nargs="+", default=config.TUPLES_PER_SEC_LIST,
                        help="Offered load in tuples/sec per query (a join reads both streams). Pass "
                             "several at --statistic-query-counts 0 to calibrate: the knee is the "
                             "highest rate the baseline still fully sustains.")
    parser.add_argument("--num-runs", type=int, default=config.NUM_RUNS,
                        help="Repetitions per sweep point")
    parser.add_argument("--skip-build", action="store_true", help="Do not rebuild NebulaStream")
    args = parser.parse_args()

    check_repository_root()
    if not args.skip_build:
        compile_nebulastream(config.cmake_flags(), config.build_dir)
    ensure_generator_image()

    os.makedirs(args.output_dir, exist_ok=True)
    csv_path = os.path.join(args.output_dir, config.RESULTS_CSV)
    with open(csv_path, "w", newline="") as f:
        csv.DictWriter(f, fieldnames=config.FIELDNAMES).writeheader()

    total = len(args.tuples_per_sec) * len(args.statistic_query_counts) * args.num_runs
    completed = 0
    started = time.time()

    for tuples_per_sec in args.tuples_per_sec:
        for num_statistic_queries in args.statistic_query_counts:
            for run_idx in range(args.num_runs):
                completed += 1
                printInfo(f"[{completed}/{total}] {tuples_per_sec} tup/s/query, "
                          f"{config.NUM_ANALYTICAL_QUERIES} Q2 aggregations + "
                          f"{num_statistic_queries} statistic queries, run {run_idx}")
                try:
                    row = run_one(num_statistic_queries, run_idx, args.output_dir, tuples_per_sec)
                except Exception as e:  # noqa: BLE001 - one bad combination must not kill the sweep
                    printError(f"Combination failed: {e}")
                    stop_generator()
                    row = _failure_row(num_statistic_queries, run_idx, tuples_per_sec, str(e))
                with open(csv_path, "a", newline="") as f:
                    csv.DictWriter(f, fieldnames=config.FIELDNAMES).writerow(row)
                printInfo(f"    -> analytical={row.get('analytical_throughput_tps')} "
                          f"offered={row.get('offered_tps')} "
                          f"issue={row.get('issue')} "
                          f"elapsed={time.time() - started:.0f}s")

    printSuccess(f"Results written to {csv_path}")


if __name__ == "__main__":
    main()
