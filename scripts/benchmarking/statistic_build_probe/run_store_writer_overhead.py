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

"""Store-writer overhead benchmark — one build query, run WITH and WITHOUT the StatisticStoreWriter.

For each (dataset, aggregation, memory_budget, window, threads) we submit the SAME statistic-build query
twice, back-to-back as one paired run (on then off, same run_idx):
  - store_writer=on : default worker; the synopsis is computed AND persisted (StatisticStoreWriter runs),
  - store_writer=off: worker started with NES_STAT_OMIT_STORE_WRITER set; the synopsis is computed but the
                      StatisticStoreWriter chain (and the STATISTICID projection) is omitted.
The build runs identically in both variants, so overhead = throughput(off) - throughput(on) isolates the
store-writer's per-window insertion cost. Pairing on+off within one run means both see the same
thermal/clock state, so a per-run difference cancels the slow drift that a run-all-on-then-all-off order
would alias onto the on/off axis. Aggregations: the store-backed scalars Count / Avg / Sum and the
synopses Reservoir (sample) / EquiWidthHistogram / CountMin (sketch).

One row per (identity, store_writer, aggregation) is appended to results_store_writer_overhead.csv; the
StoreWriterOverhead notebook pivots on store_writer to compute the overhead per aggregation and budget.
"""

import argparse
import os
import time

from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    create_folder_and_remove_if_exists,
    printError,
    printInfo,
)
from scripts.benchmarking.common._console import step
from scripts.benchmarking.statistic_build_probe.configs import (
    DATASET_PATHS,
    NUM_RUNS_PER_EXPERIMENT,
    STORE_WRITER_AGGS,
    STORE_WRITER_DATASETS,
    STORE_WRITER_ENV,
    STORE_WRITER_FIELDNAMES,
    STORE_WRITER_MEMORY_BUDGETS,
    STORE_WRITER_SCALAR_AGGS,
    STORE_WRITER_WINDOW_SIZES,
    STORE_WRITER_WORKER_THREADS,
    allBufferConfigs,
    allExecutionModes,
    allJoinStrategies,
    allPageSizes,
    build_dir,
    cmake_flags,
)
from scripts.benchmarking.statistic_build_probe.synopsis_query_builder import build_query_yaml
from scripts.benchmarking.statistic_build_probe.runner_utils import (
    RunnerConfig,
    Trial,
    initialize_csv_file,
    run_grid,
)
from scripts.benchmarking.common.worker_lifecycle import (
    check_log_for_buffer_exhaustion,
    dump_worker_log_tail,
    parse_average_throughput_from_throughput_listener,
    parse_log_to_throughput_csv,
    start_single_node_worker,
    stop_queries_and_wait,
    submit_query,
    terminate_process_if_exists,
    wait_for_query_to_finish,
)
from scripts.benchmarking.statistic_build_probe.configs import WAIT_BETWEEN_COMMANDS_LONG

# StatisticStore implementation used for all trials. SUB_STORES partitions the store per worker thread
# (constructed with `concurrency`), so store-writer inserts don't contend on a single shared lock the way
# DefaultStatisticStore (one folly::Synchronized map) would at high thread counts. We do not sweep store types here.
STORE_TYPE = "SUB_STORES"


def _build_trials(args) -> list:
    """One Trial per (dataset, aggregation, memory_budget, window, threads).

    Each trial runs BOTH store-writer variants (on then off) back-to-back inside run_combination, so the
    pair is measured under the same thermal/clock state and the per-run overhead = tp(off) - tp(on)
    cancels slow drift. Scalars (Count/Avg/Sum) ignore memory_budget but are still swept over every budget
    so each memory_budget group in the plot carries all six aggregations.
    """
    datasets = args.queries or STORE_WRITER_DATASETS
    if args.build_dataset and len(datasets) != 1:
        raise SystemExit("--build-dataset overrides the path for a single dataset; pass exactly one -q.")
    threads = [str(t) for t in args.worker_threads] if args.worker_threads else STORE_WRITER_WORKER_THREADS
    budgets = args.memory_budgets if args.memory_budgets else STORE_WRITER_MEMORY_BUDGETS
    aggs = args.aggregations or STORE_WRITER_AGGS
    window_sizes = args.window_sizes if args.window_sizes else STORE_WRITER_WINDOW_SIZES

    bufSize, buffersGBM = allBufferConfigs[0]

    trials = []
    for dataset_name in datasets:
        if dataset_name not in DATASET_PATHS:
            raise SystemExit(f"Unknown dataset {dataset_name}; known: {list(DATASET_PATHS)}")
        dataset_path = (os.path.abspath(args.build_dataset) if args.build_dataset
                        else os.path.abspath(os.path.join(build_dir, DATASET_PATHS[dataset_name])))
        for window in window_sizes:
            for nthreads in threads:
                for agg in aggs:
                    for budget in budgets:
                        cfg = (allExecutionModes[0], nthreads, bufSize, buffersGBM,
                               allJoinStrategies[0], allPageSizes[0])
                        trials.append(Trial(
                            dataset_name=dataset_name, dataset_path=dataset_path,
                            statistic_type=agg, memory_budget=budget, build_window_size_sec=window,
                            worker_config=cfg, enable_latency=False, store_type=STORE_TYPE,
                            extras={"aggregation": agg},
                        ))
    return trials


def _run_variant(trial: Trial, store_writer: str, run_dir: str, cli_log_file) -> tuple:
    """Start worker (with/without the omit-writer env var), submit the build query, parse throughput.

    Returns (row, issues) for this single variant. run_dir is the per-variant subdirectory.
    """
    executionMode, numberOfWorkerThreads, bufferSizeInBytes, \
        buffersInGlobalBufferManager, joinStrategy, pageSize = trial.worker_config
    omit_store_writer = store_writer == "off"
    issues = []

    # The SQL planner reads NES_STAT_OMIT_STORE_WRITER from the worker's environment; the worker is a
    # subprocess of this runner (Popen inherits os.environ), so set/unset it before starting the worker.
    if omit_store_writer:
        os.environ[STORE_WRITER_ENV] = "1"
    else:
        os.environ.pop(STORE_WRITER_ENV, None)

    log_file_path = os.path.join(run_dir, "SingleNodeStdout.log")
    stdout_file = open(log_file_path, 'w')
    with step("worker", detail=f"starting ({store_writer} writer)") as add:
        worker_process = start_single_node_worker(
            stdout_file, numberOfWorkerThreads, executionMode,
            joinStrategy, pageSize, bufferSizeInBytes,
            buffersInGlobalBufferManager, trial.enable_latency,
            statisticStoreType=trial.store_type,
            cli_log_file=cli_log_file)
        time.sleep(WAIT_BETWEEN_COMMANDS_LONG)
        add(f"pid {worker_process.pid}")

    query_name = (f"{trial.statistic_type}_{store_writer}_{trial.dataset_name}_"
                  f"mb{trial.memory_budget}_{trial.build_window_size_sec}sec")
    try:
        yaml_str = build_query_yaml(
            dataset_name=trial.dataset_name, dataset_path=trial.dataset_path,
            synopsis_kind=trial.statistic_type, num_synopses=1,
            memory_budget=trial.memory_budget, window_size=trial.build_window_size_sec,
            store_backed=True, omit_store_writer=omit_store_writer)
        yaml_path = os.path.join(run_dir, f"{query_name}.yaml")
        with open(yaml_path, 'w') as f:
            f.write(yaml_str)

        with step("build", detail=f"{trial.statistic_type} writer={store_writer}") as add:
            start = time.time()
            query_ids = submit_query(yaml_path, cli_log_file)
            ok, reason = wait_for_query_to_finish(query_ids, yaml_path, worker_process=worker_process)
            duration_s = time.time() - start
            stop_queries_and_wait(query_ids, yaml_path, cli_log_file)
            throughput = parse_average_throughput_from_throughput_listener(log_file_path, query_ids)
            if throughput >= 0:
                add(f"{throughput:,.0f} tup/s")
            else:
                add("no_throughput")
            if not ok:
                add(f"FAIL:{reason}")

        if throughput < 0:
            dump_worker_log_tail(log_file_path)
        if not ok:
            issues.append(f"build:{reason}")
        if check_log_for_buffer_exhaustion(log_file_path):
            issues.append("buffer_exhaustion")

        row = {
            'dataset': trial.dataset_name,
            'statistic_type': trial.statistic_type,
            'memory_budget': trial.memory_budget,
            'build_window_size_sec': trial.build_window_size_sec,
            'executionMode': executionMode,
            'numberOfWorkerThreads': numberOfWorkerThreads,
            'buffersInGlobalBufferManager': buffersInGlobalBufferManager,
            'joinStrategy': joinStrategy,
            'bufferSizeInBytes': bufferSizeInBytes,
            'pageSize': pageSize,
            'enableLatency': trial.enable_latency,
            'statisticStoreType': trial.store_type,
            'store_writer': store_writer,
            'aggregation': trial.extras["aggregation"],
            'query_name': query_name,
            'tuplesPerSecond_listener': throughput,
            'build_duration_s': duration_s,
        }
        return row, issues
    finally:
        terminate_process_if_exists(worker_process)
        stdout_file.close()
        os.environ.pop(STORE_WRITER_ENV, None)
        parse_log_to_throughput_csv(log_file_path, os.path.join(run_dir, "throughput.csv"))


def run_combination(trial: Trial, run_dir: str, cli_log_file) -> tuple:
    """Run the same build query WITH then WITHOUT the store writer, back-to-back (one paired run).

    Both variants run under the same thermal/clock state, so run_grid stamps them with the same run_idx
    and the notebook's per-run overhead = tp(off) - tp(on) cancels slow drift. Returns both rows.
    """
    rows, all_issues = [], []
    for store_writer in ("on", "off"):
        variant_dir = os.path.join(run_dir, store_writer)
        os.makedirs(variant_dir, exist_ok=True)
        row, issues = _run_variant(trial, store_writer, variant_dir, cli_log_file)
        row['issue'] = ';'.join(issues) if issues else 'ok'
        rows.append(row)
        all_issues.extend(issues)
    return rows, all_issues


def _make_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Store-writer overhead: each build query with vs without the writer.")
    p.add_argument("-q", "--queries", nargs="+",
                   help=f"Datasets to run (default: {STORE_WRITER_DATASETS}).")
    p.add_argument("--build-dataset", type=str, default=None,
                   help="Override the dataset CSV path (requires exactly one -q). For smoke runs on small data.")
    p.add_argument("-w", "--worker-threads", nargs="+",
                   help=f"Worker thread counts (default: {STORE_WRITER_WORKER_THREADS}).")
    p.add_argument("--memory-budgets", nargs="+", type=int,
                   help=f"Memory budgets in bytes (default: {STORE_WRITER_MEMORY_BUDGETS}).")
    p.add_argument("--aggregations", nargs="+", choices=STORE_WRITER_AGGS,
                   help=f"Aggregations to benchmark (default: {STORE_WRITER_AGGS}).")
    p.add_argument("--window-sizes", nargs="+", type=int,
                   help=f"Tumbling window size(s) in seconds (default: {STORE_WRITER_WINDOW_SIZES}).")
    p.add_argument("--num-runs", type=int, default=NUM_RUNS_PER_EXPERIMENT,
                   help=f"Runs per combination (default: {NUM_RUNS_PER_EXPERIMENT}).")
    p.add_argument("--clean", action="store_true",
                   help="Remove and recreate the build directory before building.")
    p.add_argument("--skip-build", action="store_true",
                   help="Skip compile_nebulastream — only when the worker is up to date.")
    p.add_argument("--output-dir", type=str, default=None,
                   help="Directory for the output CSV. Created if missing.")
    return p


def _banner_extras(trial: Trial) -> str:
    return f"{trial.extras['aggregation']} writer=on+off"


def _run_dir_extras(trial: Trial) -> str:
    return trial.extras['aggregation']


if __name__ == "__main__":
    args = _make_argparser().parse_args()

    output_dir = args.output_dir if args.output_dir else "."
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "results_store_writer_overhead.csv")

    printInfo("Parsed arguments:")
    for k, v in vars(args).items():
        printInfo(f"  {k}: {v}")
    print()

    check_repository_root()

    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    if not args.skip_build:
        compile_nebulastream(cmake_flags, build_dir)

    trials = _build_trials(args)

    missing = sorted({t.dataset_path for t in trials if not os.path.exists(t.dataset_path)})
    if missing:
        for m in missing:
            printError(f"Dataset not found: {m}")
        raise SystemExit(1)

    if os.geteuid() == 0:
        try:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write("-1000")
        except OSError:
            pass

    initialize_csv_file(csv_path, STORE_WRITER_FIELDNAMES)

    runner_config = RunnerConfig(
        runner_name="storewr",
        csv_filename="results_store_writer_overhead.csv",
        fieldnames=STORE_WRITER_FIELDNAMES,
        banner_extras=_banner_extras,
        run_dir_extras=_run_dir_extras,
    )

    run_grid(args=args, runner_config=runner_config, trials=trials,
             run_combination=run_combination, output_dir=output_dir, csv_path=csv_path)
