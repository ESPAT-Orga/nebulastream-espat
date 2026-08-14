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

"""statistic_grid — does independent monitoring disturb a workload that is keeping up?

A 2D grid: k concurrent ClusterMonitoring Q2 queries alongside j independent equi-width-histogram
queries, every one with its own TCP source. Because nothing is spliced, j is not capped by k and the
statistic queries pay for their own ingestion — so this measures the cost of monitoring queries
*including* that ingestion, which is a different claim from the shared experiment's marginal
synopsis cost. The two belong side by side.

Deliberately run BELOW saturation. A saturated system invites the objection that the measurement is
just capacity being divided; at a fixed sustainable rate the question is sharp — does adding
monitoring disturb a workload that was comfortably keeping up? The sustained fraction is therefore
reported alongside throughput, and the load as a fraction of capacity belongs in any write-up.

Output is deliberately ONE csv at the top of --output-dir: `results_statistic_grid.csv`, holding every
(rate, k, j, run) point of the sweep. Per-run logs and the per-query diagnostic live in per-run
subdirectories, so analysing a sweep means downloading exactly one file:

    scp <host>:<output-dir>/results_statistic_grid.csv .../plots/results_statistic_grid.csv

Usage (from the repository root):
    python3 -m scripts.benchmarking.statistic_overhead.run_statistic_grid --output-dir <dir>
"""

import argparse
import csv
import os
import statistics
import time

from scripts.benchmarking.common.worker_lifecycle import (
    check_log_for_buffer_exhaustion,
    dump_worker_log_tail,
    start_single_node_worker,
    terminate_process_if_exists,
)
from scripts.benchmarking.statistic_overhead import config
from scripts.benchmarking.statistic_overhead.grid_submission import shutdown_grid, submit_grid
from scripts.benchmarking.statistic_overhead.run_statistic_overhead import (
    ensure_generator_image,
    parse_throughput_samples,
    start_generator,
    steady_state_means,
    stop_generator,
)
from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    create_folder_and_remove_if_exists,
    printError,
    printInfo,
    printSuccess,
)

ROLE_ANALYTICAL = "analytical"
ROLE_STATISTIC = "statistic"


def write_per_query_csv(samples, roles, csv_path):
    if not samples:
        return
    t0 = min(start for _, start, _ in samples)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(config.PER_QUERY_FIELDNAMES)
        for query_id, start, tps in sorted(samples, key=lambda s: (s[0], s[1])):
            writer.writerow([query_id, roles.get(query_id, "unknown"), start, start - t0, tps])


def run_one(num_analytical, num_statistic, run_idx, output_dir, tuples_per_sec):
    """Execute one (k, j, run_idx) grid point and return its result row."""
    # The rate belongs in the name: without it, two rates sharing an --output-dir would overwrite
    # each other's logs and per-query CSVs while the results CSV happily recorded both.
    tag = f"r{tuples_per_sec // 1000}k_k{num_analytical:02d}_j{num_statistic:02d}"
    run_dir = os.path.join(output_dir, f"{tag}_run_{run_idx}")
    create_folder_and_remove_if_exists(run_dir)
    worker_log = os.path.join(run_dir, "worker.log")
    generator_log = os.path.join(run_dir, "generator.log")
    cli_log_path = os.path.join(run_dir, "nes-cli.log")

    row = {
        'num_analytical_queries': num_analytical,
        'num_statistic_queries': num_statistic,
        'run_idx': run_idx,
        'num_worker_threads': config.WORKER_THREADS,
        'window_size_sec': config.WINDOW_SIZE_SEC,
        'window_advance_sec': config.WINDOW_ADVANCE_SEC,
        'statistic_window_sec': config.GRID_STATISTIC_WINDOW_SEC,
        'job_domain': config.JOB_DOMAIN,
        'events_per_sec': config.EVENTS_PER_SEC,
        'memory_budget': config.MEMORY_BUDGET,
        'tuples_per_sec_per_query': tuples_per_sec,
        'offered_tps': config.grid_offered_tps(tuples_per_sec, num_analytical, num_statistic),
        'analytical_offered_tps': config.grid_analytical_offered_tps(tuples_per_sec, num_analytical),
    }
    issues = []

    worker_process = None
    repl_proc = repl_log = None
    analytical_ids, statistic_ids = [], []

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

            repl_proc, repl_log, analytical_ids, statistic_ids = submit_grid(
                num_analytical, num_statistic, run_dir)
            if len(analytical_ids) != num_analytical:
                issues.append(f"only {len(analytical_ids)}/{num_analytical} analytical deployed")
            if len(statistic_ids) != num_statistic:
                issues.append(f"only {len(statistic_ids)}/{num_statistic} statistic deployed")

            time.sleep(config.WARMUP_SECONDS + config.MEASUREMENT_WINDOW_SECONDS)

            if worker_process.poll() is not None:
                raise RuntimeError(f"worker exited with code {worker_process.returncode}")

            # Stop before tearing the worker down: QueryStop flushes the listener's pending windows.
            shutdown_grid(repl_proc, repl_log)
            time.sleep(2)
        finally:
            if worker_process is not None:
                terminate_process_if_exists(worker_process)
            stop_generator(generator_log)

    samples = parse_throughput_samples(worker_log)
    analytical_set, statistic_set = set(analytical_ids), set(statistic_ids)
    roles = {qid: ROLE_ANALYTICAL for qid in analytical_set}
    roles.update({qid: ROLE_STATISTIC for qid in statistic_set})
    # Inside the run's own directory, NOT next to the results CSV: the top level must hold exactly
    # one CSV so the whole experiment is a single-file download. This one is a per-run diagnostic
    # (it is where starvation is visible), so it belongs with that run's logs.
    write_per_query_csv(samples, roles, os.path.join(run_dir, "per_query.csv"))

    means = steady_state_means(samples, config.WARMUP_SECONDS)
    analytical = [tps for qid, tps in means.items() if qid in analytical_set]
    statistic = [tps for qid, tps in means.items() if qid in statistic_set]

    if not analytical:
        issues.append("no analytical throughput measured")
        dump_worker_log_tail(worker_log)
    if len(analytical) != num_analytical:
        issues.append(f"only {len(analytical)}/{num_analytical} analytical queries measured")
    if check_log_for_buffer_exhaustion(worker_log):
        issues.append("buffer exhaustion")

    row['analytical_throughput_tps'] = sum(analytical) if analytical else -1
    row['analytical_throughput_median_tps'] = statistics.median(analytical) if analytical else -1
    row['num_analytical_measured'] = len(analytical)
    row['statistic_throughput_tps'] = sum(statistic) if statistic else 0
    row['num_statistic_measured'] = len(statistic)
    row['issue'] = ";".join(issues) if issues else "ok"
    return row


def _failure_row(num_analytical, num_statistic, run_idx, tuples_per_sec, message):
    row = {name: "" for name in config.GRID_FIELDNAMES}
    row.update({
        'num_analytical_queries': num_analytical,
        'num_statistic_queries': num_statistic,
        'run_idx': run_idx,
        'tuples_per_sec_per_query': tuples_per_sec,
        'offered_tps': config.grid_offered_tps(tuples_per_sec, num_analytical, num_statistic),
        'analytical_offered_tps': config.grid_analytical_offered_tps(tuples_per_sec, num_analytical),
        'issue': f"exception:{message}",
    })
    return row


def main():
    parser = argparse.ArgumentParser(description="statistic_overhead 2D isolated grid")
    parser.add_argument("--output-dir", default=".", help="Directory for results and per-run logs")
    parser.add_argument("--analytical-counts", type=int, nargs="+",
                        default=config.GRID_ANALYTICAL_COUNTS, help="k values to sweep")
    parser.add_argument("--statistic-counts", type=int, nargs="+",
                        default=config.GRID_STATISTIC_COUNTS, help="j values to sweep")
    parser.add_argument("--tuples-per-sec", type=int, nargs="+",
                        default=config.GRID_TUPLES_PER_SEC_PER_QUERY,
                        help="Offered load per query; one grid is run per rate. Keep at least one "
                             "rate below saturation: a saturated system measures capacity division, "
                             "not the cost of monitoring.")
    parser.add_argument("--num-runs", type=int, default=config.NUM_RUNS)
    parser.add_argument("--skip-build", action="store_true", help="Do not rebuild NebulaStream")
    args = parser.parse_args()

    check_repository_root()
    if not args.skip_build:
        compile_nebulastream(config.cmake_flags(), config.build_dir)
    ensure_generator_image()

    os.makedirs(args.output_dir, exist_ok=True)
    csv_path = os.path.join(args.output_dir, config.GRID_RESULTS_CSV)
    with open(csv_path, "w", newline="") as f:
        csv.DictWriter(f, fieldnames=config.GRID_FIELDNAMES).writeheader()

    total = (len(args.tuples_per_sec) * len(args.analytical_counts)
             * len(args.statistic_counts) * args.num_runs)
    completed = 0
    started = time.time()

    # Rate is the outermost loop so each rate's grid completes as a block: if the sweep is cut short,
    # what exists is a whole arm rather than a ragged slice of every arm.
    for tuples_per_sec in args.tuples_per_sec:
        for num_analytical in args.analytical_counts:
            for num_statistic in args.statistic_counts:
                for run_idx in range(args.num_runs):
                    completed += 1
                    printInfo(f"[{completed}/{total}] {tuples_per_sec} tup/s/query, "
                              f"k={num_analytical} analytical + j={num_statistic} statistic, "
                              f"run {run_idx}")
                    try:
                        row = run_one(num_analytical, num_statistic, run_idx,
                                      args.output_dir, tuples_per_sec)
                    except Exception as e:  # noqa: BLE001 - one bad point must not kill the grid
                        printError(f"Grid point failed: {e}")
                        stop_generator()
                        row = _failure_row(num_analytical, num_statistic, run_idx,
                                           tuples_per_sec, str(e))
                    with open(csv_path, "a", newline="") as f:
                        csv.DictWriter(f, fieldnames=config.GRID_FIELDNAMES).writerow(row)
                    printInfo(f"    -> analytical={row.get('analytical_throughput_tps')} "
                              f"offered={row.get('analytical_offered_tps')} "
                              f"issue={row.get('issue')} elapsed={time.time() - started:.0f}s")

    printSuccess(f"Results written to {csv_path}")


if __name__ == "__main__":
    main()
