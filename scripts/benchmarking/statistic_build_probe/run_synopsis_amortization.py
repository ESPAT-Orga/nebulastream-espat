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

"""Synopsis amortization benchmark — one build query maintaining N synopses.

The question: when does a single full-record Reservoir sample beat maintaining
one specialised synopsis (CountMin / EquiWidthHistogram) per field? This runner
measures it directly by submitting ONE query that builds N synopses over the
first-N numeric fields of a dataset (one scan, one duration, one throughput).

Per parameter combination it:
  1. starts a fresh nes-single-node-worker,
  2. renders + submits a single multi-synopsis build query (synopsis_query_builder),
  3. records the average throughput from the throughput listener,
  4. tears the worker down and appends one row to results_synopsis_amortization.csv.

The CSV is a superset of results_statistic_build.csv plus a ``num_synopses``
column: rows with num_synopses == 1 reproduce the single-synopsis build view
(Sum / Passthrough / Reservoir / CountMin / EquiWidthHistogram), while CountMin /
EquiWidthHistogram rows with num_synopses > 1 carry the amortization curve.
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
    SYNOPSIS_AMORT_BASE_IDS,
    SYNOPSIS_AMORT_DATASETS,
    SYNOPSIS_AMORT_FIELDNAMES,
    SYNOPSIS_AMORT_MEMORY_BUDGETS,
    SYNOPSIS_AMORT_SCALING_TYPES,
    SYNOPSIS_AMORT_SINGLE_TYPES,
    SYNOPSIS_AMORT_WINDOW_SIZES,
    SYNOPSIS_AMORT_WORKER_THREADS,
    allBufferConfigs,
    allExecutionModes,
    allJoinStrategies,
    allPageSizes,
    build_dir,
    cmake_flags,
)
from scripts.benchmarking.statistic_build_probe.synopsis_query_builder import (
    build_query_yaml,
    n_ladder_for,
)
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

# Synopses go to the default StatisticStore; we do not sweep store types here.
STORE_TYPE = "DEFAULT"


def _build_trials(args) -> list:
    """One Trial per (dataset, window_size, kind, N, budget, threads).

    Scaling kinds (CountMin / EquiWidthHistogram) sweep N over the dataset's
    {1,2,4,8,F} ladder and every budget. Reservoir is pinned to N == 1 but keeps
    the budget sweep (sample size scales with the budget). Sum / Passthrough are
    pinned to N == 1 and a single budget (they ignore memory_budget). Window sizes
    default to a single value but can be swept (--window-sizes 1 5 10).
    """
    datasets = args.queries or SYNOPSIS_AMORT_DATASETS
    if args.build_dataset and len(datasets) != 1:
        raise SystemExit("--build-dataset overrides the path for a single dataset; pass exactly one -q.")
    threads = [str(t) for t in args.worker_threads] if args.worker_threads else SYNOPSIS_AMORT_WORKER_THREADS
    budgets = args.memory_budgets if args.memory_budgets else SYNOPSIS_AMORT_MEMORY_BUDGETS
    scaling_types = args.statistic_types_scaling or SYNOPSIS_AMORT_SCALING_TYPES
    single_types = args.statistic_types_single if args.statistic_types_single is not None else SYNOPSIS_AMORT_SINGLE_TYPES
    window_sizes = args.window_sizes if args.window_sizes else SYNOPSIS_AMORT_WINDOW_SIZES

    bufSize, buffersGBM = allBufferConfigs[0]
    worker_cfg = (allExecutionModes[0], None, bufSize, buffersGBM, allJoinStrategies[0], allPageSizes[0])

    trials = []
    for dataset_name in datasets:
        if dataset_name not in DATASET_PATHS:
            raise SystemExit(f"Unknown dataset {dataset_name}; known: {list(DATASET_PATHS)}")
        dataset_path = (os.path.abspath(args.build_dataset) if args.build_dataset
                        else os.path.abspath(os.path.join(build_dir, DATASET_PATHS[dataset_name])))
        ladder = n_ladder_for(dataset_name)
        if args.max_n is not None:
            ladder = [n for n in ladder if n <= args.max_n]

        def add_trial(kind, n, budget, nthreads, window):
            cfg = (worker_cfg[0], nthreads, worker_cfg[2], worker_cfg[3], worker_cfg[4], worker_cfg[5])
            trials.append(Trial(
                dataset_name=dataset_name, dataset_path=dataset_path,
                statistic_type=kind, memory_budget=budget, build_window_size_sec=window,
                worker_config=cfg, enable_latency=False, store_type=STORE_TYPE,
                extras={"num_synopses": n},
            ))

        for window in window_sizes:
            for nthreads in threads:
                for kind in scaling_types:
                    for n in ladder:
                        for budget in budgets:
                            add_trial(kind, n, budget, nthreads, window)
                for kind in single_types:
                    if kind == "Reservoir":
                        for budget in budgets:
                            add_trial(kind, 1, budget, nthreads, window)
                    else:  # Sum / Passthrough ignore memory_budget
                        add_trial(kind, 1, budgets[0], nthreads, window)
    return trials


def run_combination(trial: Trial, run_dir: str, cli_log_file) -> tuple:
    """Start worker, submit the multi-synopsis build query, parse throughput."""
    executionMode, numberOfWorkerThreads, bufferSizeInBytes, \
        buffersInGlobalBufferManager, joinStrategy, pageSize = trial.worker_config
    num_synopses = trial.extras["num_synopses"]
    issues = []

    log_file_path = os.path.join(run_dir, "SingleNodeStdout.log")
    stdout_file = open(log_file_path, 'w')
    with step("worker", detail="starting") as add:
        worker_process = start_single_node_worker(
            stdout_file, numberOfWorkerThreads, executionMode,
            joinStrategy, pageSize, bufferSizeInBytes,
            buffersInGlobalBufferManager, trial.enable_latency,
            statisticStoreType=trial.store_type,
            cli_log_file=cli_log_file)
        time.sleep(WAIT_BETWEEN_COMMANDS_LONG)
        add(f"pid {worker_process.pid}")

    query_name = (f"{trial.statistic_type}Amort_{trial.dataset_name}_"
                  f"N{num_synopses}_mb{trial.memory_budget}_{trial.build_window_size_sec}sec")
    try:
        yaml_str = build_query_yaml(
            dataset_name=trial.dataset_name, dataset_path=trial.dataset_path,
            synopsis_kind=trial.statistic_type, num_synopses=num_synopses,
            memory_budget=trial.memory_budget, window_size=trial.build_window_size_sec)
        yaml_path = os.path.join(run_dir, f"{query_name}.yaml")
        with open(yaml_path, 'w') as f:
            f.write(yaml_str)

        with step("build", detail=f"{trial.statistic_type} N={num_synopses}") as add:
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
            'num_synopses': num_synopses,
            'query_name': query_name,
            'tuplesPerSecond_listener': throughput,
            'build_duration_s': duration_s,
        }
        return row, issues
    finally:
        terminate_process_if_exists(worker_process)
        stdout_file.close()
        parse_log_to_throughput_csv(log_file_path, os.path.join(run_dir, "throughput.csv"))


def _make_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Synopsis amortization: one query, N synopses.")
    p.add_argument("-q", "--queries", nargs="+",
                   help=f"Datasets to run (default: {SYNOPSIS_AMORT_DATASETS}).")
    p.add_argument("--build-dataset", type=str, default=None,
                   help="Override the dataset CSV path (requires exactly one -q). For smoke runs on small data.")
    p.add_argument("-w", "--worker-threads", nargs="+",
                   help=f"Worker thread counts (default: {SYNOPSIS_AMORT_WORKER_THREADS}).")
    p.add_argument("--memory-budgets", nargs="+", type=int,
                   help=f"Memory budgets in bytes (default: {SYNOPSIS_AMORT_MEMORY_BUDGETS}).")
    p.add_argument("--statistic-types-scaling", nargs="+",
                   choices=["CountMin", "EquiWidthHistogram"],
                   help=f"Scaling (N=1..F) synopsis types (default: {SYNOPSIS_AMORT_SCALING_TYPES}).")
    p.add_argument("--statistic-types-single", nargs="*",
                   choices=["Reservoir", "Sum", "Passthrough"],
                   help=f"Single-synopsis baseline types (default: {SYNOPSIS_AMORT_SINGLE_TYPES}; "
                        "pass with no values to disable baselines).")
    p.add_argument("--max-n", type=int, default=None,
                   help="Cap the N ladder (e.g. --max-n 3 for a quick smoke run).")
    p.add_argument("--window-sizes", nargs="+", type=int,
                   help=f"Tumbling window size(s) in seconds to sweep (default: {SYNOPSIS_AMORT_WINDOW_SIZES}). "
                        "Pass several to compare across windowing, e.g. --window-sizes 1 5 10.")
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
    return f"N={trial.extras['num_synopses']}"


def _run_dir_extras(trial: Trial) -> str:
    return f"N{trial.extras['num_synopses']}"


if __name__ == "__main__":
    args = _make_argparser().parse_args()

    output_dir = args.output_dir if args.output_dir else "."
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "results_synopsis_amortization.csv")

    printInfo("Parsed arguments:")
    for k, v in vars(args).items():
        printInfo(f"  {k}: {v}")
    print()

    check_repository_root()

    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    # cmake configure with ENABLE_LARGE_TESTS=1 fetches the datasets, so the
    # path-existence check must come *after* the build.
    if not args.skip_build:
        compile_nebulastream(cmake_flags, build_dir)

    trials = _build_trials(args)

    # Fail fast if any dataset file is missing.
    missing = sorted({t.dataset_path for t in trials if not os.path.exists(t.dataset_path)})
    if missing:
        for m in missing:
            printError(f"Dataset not found: {m}")
        raise SystemExit(1)
    for ds in sorted({t.dataset_name for t in trials}):
        printInfo(f"Dataset {ds}: ladder N={n_ladder_for(ds)}")

    if os.geteuid() == 0:
        try:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write("-1000")
        except OSError:
            pass

    initialize_csv_file(csv_path, SYNOPSIS_AMORT_FIELDNAMES)

    runner_config = RunnerConfig(
        runner_name="amort",
        csv_filename="results_synopsis_amortization.csv",
        fieldnames=SYNOPSIS_AMORT_FIELDNAMES,
        banner_extras=_banner_extras,
        run_dir_extras=_run_dir_extras,
    )

    run_grid(args=args, runner_config=runner_config, trials=trials,
             run_combination=run_combination, output_dir=output_dir, csv_path=csv_path)
