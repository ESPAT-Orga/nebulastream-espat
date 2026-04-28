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

"""Shared scaffolding for the build / accuracy / probe runners.

Each runner only has to define its per-combination work; everything else
(argparse, dataset resolution, the outer loop, banners, run-index overwriting,
CSV writing, ETA, failure tracking) lives here so the three runners stay in
sync.
"""

import argparse
import ast
import csv
import itertools
import os
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from scripts.benchmarking.utils import printError, printInfo, printSuccess
from scripts.benchmarking.e2e._console import (
    banner,
    clear_lines,
    get_step_counter,
    reset_step_counter,
    step,
)
from scripts.benchmarking.e2e.configs import (
    DATASET_PATHS,
    NUM_RUNS_PER_EXPERIMENT,
    allBufferConfigs,
    allBuildWindowSizesSec,
    allDatasets,
    allExecutionModes,
    allJoinStrategies,
    allNumberOfWorkerThreads,
    allPageSizes,
    allStatisticStoreTypes,
    build_dir,
    memoryBudgetConfig,
)


@dataclass
class Trial:
    """One fully-specified parameter combination — one banner unit."""

    dataset_name: str
    dataset_path: str
    statistic_type: str
    memory_budget: int
    build_window_size_sec: int
    worker_config: tuple   # (executionMode, nWorkerThreads, bufferSize, buffersGBM, joinStrategy, pageSize)
    enable_latency: bool
    store_type: str
    extras: dict = field(default_factory=dict)  # probe-only: num_statistic_ids, build_windows_per_probe_window, num_probe_repetitions


@dataclass
class RunnerConfig:
    """Per-runner customisation for the shared driver."""

    runner_name: str                     # "build" / "accuracy" / "probe" — used in run_dir basename and failure log filenames
    csv_filename: str                    # "results_statistic_<runner_name>.csv"
    fieldnames: list                     # CSV header (full set including run_idx + issue)
    # Optional callbacks for runners that need extra dimensions in the banner
    # / run_dir name (probe's num_statistic_ids, build_windows_per_probe_window, ...).
    banner_extras: Callable = field(default=lambda trial: "")
    run_dir_extras: Callable = field(default=lambda trial: "")


# === Helpers shared by every runner ===========================================

def parse_buffer_config(config_strings):
    result = []
    for s in config_strings:
        parsed = ast.literal_eval(s.strip())
        if isinstance(parsed, tuple) and len(parsed) == 2:
            result.append(parsed)
        else:
            raise ValueError(f"Invalid buffer config: {s}")
    return result


def estimate_eta(start_time, end_time, completed_runs, total_runs):
    elapsed = end_time - start_time
    avg = elapsed / completed_runs
    eta_seconds = avg * (total_runs - completed_runs)
    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
    eta_h, rem = divmod(eta_seconds, 3600)
    eta_m, eta_s = divmod(rem, 60)
    return int(eta_h), int(eta_m), eta_s, eta_time


def make_argparser(description: str) -> argparse.ArgumentParser:
    """Common CLI args used by build / accuracy / probe."""
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--all", action="store_true", help="Run all configured datasets.")
    p.add_argument("-q", "--queries", nargs="+",
                   help="Datasets to run (Nexmark, ClusterMonitoring).")
    p.add_argument("--build-dataset", type=str, default=None,
                   help="Override the build dataset path. When set, every dataset uses this CSV.")
    p.add_argument("-w", "--worker-threads", nargs="+", help="Worker thread counts to sweep.")
    p.add_argument("-b", "--buffer-config", nargs="+",
                   help="Buffer configs as tuples like '(1234, 100)'.")
    p.add_argument("--statistic-types", nargs="+",
                   choices=["Reservoir", "EquiWidthHistogram", "CountMin"],
                   help="Statistic types to run (default: all in dataset config).")
    p.add_argument("--memory-budgets", nargs="+", type=int,
                   help="Memory budgets to sweep. Default: configured list.")
    p.add_argument("--window-sizes", nargs="+", type=int,
                   help="Build window sizes (sec). Default: configured list.")
    p.add_argument("--statistic-store-types", nargs="+",
                   choices=["DEFAULT", "WINDOW", "SUB_STORES"],
                   help="StatisticStore implementations to benchmark.")
    p.add_argument("--clean", action="store_true",
                   help="Remove and recreate the build directory before building.")
    p.add_argument("--output-dir", type=str, default=None,
                   help="Directory for output CSV files. Created if missing.")
    p.add_argument("--skip-build", action="store_true",
                   help="Skip compile_nebulastream — only call when the worker is up to date.")
    p.add_argument("--num-runs", type=int, default=NUM_RUNS_PER_EXPERIMENT,
                   help=f"Number of runs per parameter combination (default: {NUM_RUNS_PER_EXPERIMENT}).")
    return p


def resolve_datasets(args) -> list:
    """Apply --build-dataset override and verify each dataset path exists.

    Mutates ``allDatasets`` entries to set the resolved ``"path"`` field.
    """
    for ds in allDatasets:
        if args.build_dataset:
            ds["path"] = os.path.abspath(args.build_dataset)
        else:
            ds["path"] = os.path.abspath(os.path.join(build_dir, DATASET_PATHS[ds["name"]]))
        if not os.path.exists(ds["path"]):
            printError(f"Dataset not found for {ds['name']}: {ds['path']}")
            raise SystemExit(1)
        printInfo(f"Dataset {ds['name']}: {ds['path']}")
    return allDatasets


def filter_datasets(args) -> list:
    datasets = allDatasets if not args.queries else [d for d in allDatasets if d["name"] in args.queries]
    if not datasets:
        printError(f"No datasets match --queries {args.queries}.")
        raise SystemExit(1)
    return datasets


def select_sweeps(args):
    """Resolve all CLI overrides into the actual lists used to build trials."""
    worker_threads = [str(t) for t in args.worker_threads] if args.worker_threads else allNumberOfWorkerThreads
    buffer_configs = parse_buffer_config(args.buffer_config) if args.buffer_config else allBufferConfigs
    statistic_types_to_run = args.statistic_types  # None = use per-dataset config
    memory_budgets_to_run = args.memory_budgets if args.memory_budgets else memoryBudgetConfig
    window_sizes_to_run = args.window_sizes if args.window_sizes else allBuildWindowSizesSec
    statistic_store_types_to_run = args.statistic_store_types or allStatisticStoreTypes
    return (worker_threads, buffer_configs, statistic_types_to_run,
            memory_budgets_to_run, window_sizes_to_run, statistic_store_types_to_run)


def worker_combinations(worker_threads, buffer_configs, enable_latency_list):
    """Cartesian product of worker config dimensions."""
    return list(itertools.product(
        allExecutionModes, worker_threads, buffer_configs, allJoinStrategies, allPageSizes,
        enable_latency_list))


def docker_buffer_override(bufferSize, buffersGBM, has_explicit_buffer_config):
    """In the docker CI host, force a known-good buffer config to avoid OOMs."""
    if not has_explicit_buffer_config and socket.gethostname() == "docker-hostname":
        return 102400, 40000
    return bufferSize, buffersGBM


def build_basic_trials(args, *, enable_latency_list) -> list:
    """Standard cartesian-product trial list used by the build and accuracy runners."""
    datasets = filter_datasets(args)
    (worker_threads, buffer_configs, statistic_types_to_run,
     memory_budgets_to_run, window_sizes_to_run, store_types) = select_sweeps(args)

    trials = []
    for dataset in datasets:
        types = statistic_types_to_run or dataset["statistics"]
        for stat_type in types:
            if stat_type not in dataset["statistics"]:
                continue
            for mb in memory_budgets_to_run:
                for ws in window_sizes_to_run:
                    for wc in worker_combinations(worker_threads, buffer_configs, enable_latency_list):
                        execMode, nThreads, (bufSize, buffersGBM), joinStrat, pageSize, enableLatency = wc
                        bufSize, buffersGBM = docker_buffer_override(
                            bufSize, buffersGBM, has_explicit_buffer_config=bool(args.buffer_config))
                        worker_cfg = (execMode, nThreads, bufSize, buffersGBM, joinStrat, pageSize)
                        for store_type in store_types:
                            trials.append(Trial(
                                dataset_name=dataset["name"], dataset_path=dataset["path"],
                                statistic_type=stat_type, memory_budget=mb, build_window_size_sec=ws,
                                worker_config=worker_cfg, enable_latency=enableLatency, store_type=store_type,
                            ))
    return trials


def initialize_csv_file(csv_path: str, fieldnames: list) -> None:
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()


# === The shared outer loop ====================================================

def _trial_banner_dimensions(trial: Trial, banner_extras_fn) -> str:
    execMode, nThreads, bufSize, buffersGBM, joinStrat, pageSize = trial.worker_config
    extras = banner_extras_fn(trial)
    extras_part = f" · {extras}" if extras else ""
    return (f"{trial.dataset_name} · {trial.statistic_type} · "
            f"mb={trial.memory_budget} ws={trial.build_window_size_sec}s · "
            f"{nThreads}t{extras_part} · {trial.store_type}")


def _run_dir_basename(runner_config: RunnerConfig, trial: Trial, run_idx: int, ts: int) -> str:
    execMode, nThreads, bufSize, buffersGBM, joinStrat, pageSize = trial.worker_config
    extras = runner_config.run_dir_extras(trial)
    extras_part = f"_{extras}" if extras else ""
    return (f"{runner_config.runner_name}_{ts}_{trial.dataset_name}_{trial.statistic_type}_"
            f"mb{trial.memory_budget}_ws{trial.build_window_size_sec}sec_"
            f"{nThreads}t_{trial.store_type}{extras_part}_run{run_idx}")


def _make_failure_row(trial: Trial, run_idx: int, exception_str: str, fieldnames: list) -> dict:
    """Build a CSV row for a trial that crashed — only identity columns + issue.

    Any fieldnames not covered are left absent and DictWriter writes them empty.
    """
    execMode, nThreads, bufSize, buffersGBM, joinStrat, pageSize = trial.worker_config
    row = {
        'dataset': trial.dataset_name,
        'statistic_type': trial.statistic_type,
        'memory_budget': trial.memory_budget,
        'build_window_size_sec': trial.build_window_size_sec,
        'executionMode': execMode,
        'numberOfWorkerThreads': nThreads,
        'buffersInGlobalBufferManager': buffersGBM,
        'joinStrategy': joinStrat,
        'bufferSizeInBytes': bufSize,
        'pageSize': pageSize,
        'enableLatency': trial.enable_latency,
        'statisticStoreType': trial.store_type,
        'run_idx': run_idx,
        'issue': f'exception:{exception_str}',
    }
    # Probe's extras are also identity-ish; surface them when present.
    for k, v in trial.extras.items():
        if k in fieldnames:
            row[k] = v
    return {k: v for k, v in row.items() if k in fieldnames}


def run_grid(*, args, runner_config: RunnerConfig, trials: list,
             run_combination: Callable[[Trial, str, Any], tuple],
             output_dir: str, csv_path: str) -> None:
    """Outer loop driver shared by all three runners.

    For each trial:
      - print one banner with run-span [N/M] or [N-M/M].
      - loop num_runs in place (clear lines + reset_step_counter).
      - call ``run_combination(trial, run_dir, cli_log_file) -> (row, issues)``.
      - write the row (with run_idx + issue stamped on) to ``csv_path``.
      - emit ``[done]`` step with run= and ETA.
    """
    from scripts.benchmarking.utils import create_folder_and_remove_if_exists

    total_runs = len(trials) * args.num_runs
    completed_runs = 0
    failed_experiments = []
    problematic_experiments = []
    start_time = time.time()

    printInfo(f"Total trials: {len(trials)}")
    printInfo(f"Runs per trial: {args.num_runs}")
    printInfo(f"Total runs: {total_runs}")
    print()

    for trial in trials:
        print()
        first_run_num = completed_runs + 1
        last_run_num = completed_runs + args.num_runs
        run_span = (f"{first_run_num}/{total_runs}" if args.num_runs == 1
                    else f"{first_run_num}-{last_run_num}/{total_runs}")
        banner(
            f"[{run_span}] {datetime.now().strftime('%H:%M:%S')}  "
            + _trial_banner_dimensions(trial, runner_config.banner_extras)
        )

        lines_in_prev_run = 0
        for run_idx in range(args.num_runs):
            if run_idx > 0:
                clear_lines(lines_in_prev_run)
            reset_step_counter()

            run_dir = os.path.join(
                os.path.abspath(output_dir),
                _run_dir_basename(runner_config, trial, run_idx, int(time.time())))
            create_folder_and_remove_if_exists(run_dir)
            cli_log_path = os.path.join(run_dir, "run.log")
            cli_log_file = open(cli_log_path, 'w')

            desc = f"{trial.dataset_name}/{trial.statistic_type} mb={trial.memory_budget} run={run_idx}"

            try:
                row, issues = run_combination(trial, run_dir, cli_log_file)
                row['run_idx'] = run_idx
                row['issue'] = ';'.join(issues) if issues else 'ok'

                if issues:
                    for issue in issues:
                        problematic_experiments.append({
                            'description': desc, 'issue': issue, 'output_folder': run_dir,
                        })

                with step("done") as add:
                    with open(csv_path, 'a', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=runner_config.fieldnames)
                        writer.writerow(row)
                    add(f"run={run_idx}")
                    add("row written")
                    if completed_runs + 1 < total_runs:
                        eta_h, eta_m, eta_s, eta_time = estimate_eta(
                            start_time, time.time(), completed_runs + 1, total_runs)
                        add(f"ETA {eta_h}h {eta_m:02d}m (~{eta_time.strftime('%H:%M:%S')})")
            except Exception as e:
                printError(f"Experiment failed: {e}")
                failed_experiments.append({'description': desc, 'error': str(e), 'output_folder': run_dir})
                with open(csv_path, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=runner_config.fieldnames)
                    writer.writerow(_make_failure_row(trial, run_idx, str(e), runner_config.fieldnames))
            finally:
                cli_log_file.close()

            lines_in_prev_run = get_step_counter()
            completed_runs += 1

    elapsed = time.time() - start_time
    h, rem = divmod(elapsed, 3600)
    m, s = divmod(rem, 60)
    printInfo(f"\n\nDone in {int(h)}h {int(m)}m {s:.1f}s")

    abs_csv_path = os.path.abspath(csv_path)
    printInfo(f"Results: {abs_csv_path}")

    if problematic_experiments:
        ct_file = os.path.join(output_dir, f"{runner_config.runner_name}_crashes_and_failures.txt")
        with open(ct_file, 'w') as f:
            for i, p in enumerate(problematic_experiments, 1):
                msg = f"[{i}] {p['description']}\n    Issue: {p['issue']}\n    Output: {p['output_folder']}\n"
                printError(msg)
                f.write(msg + "\n")
        printError(f"Wrote {ct_file}")

    if failed_experiments:
        f_file = os.path.join(output_dir, f"{runner_config.runner_name}_failed_experiments.txt")
        with open(f_file, 'w') as f:
            for i, p in enumerate(failed_experiments, 1):
                msg = f"[{i}] {p['description']}\n    Error: {p['error']}\n    Output: {p['output_folder']}\n"
                printError(msg)
                f.write(msg + "\n")
        printError(f"Wrote {f_file}")

    if not failed_experiments and not problematic_experiments:
        printSuccess(f"All {total_runs} runs completed successfully.")
