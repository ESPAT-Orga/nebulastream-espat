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

"""Statistic build benchmark — build query + throughput on the standalone single-node-worker.

Per parameter combination this script:
  1. starts a fresh nes-single-node-worker,
  2. submits the build query (existing *Build_*.yaml.template),
  3. records the average throughput from the throughput listener,
  4. tears the worker down and writes one row to results_statistic_build.csv.
"""

import os
import time

from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    create_folder_and_remove_if_exists,
    printInfo,
)
from scripts.benchmarking.e2e._console import step
from scripts.benchmarking.e2e.build_phase import run_build_phase
from scripts.benchmarking.e2e.configs import (
    BUILD_FIELDNAMES,
    STATISTIC_IDS,
    WAIT_BETWEEN_COMMANDS_LONG,
    build_dir,
    cmake_flags,
    enableLatencyForBuild,
)
from scripts.benchmarking.e2e.runner_utils import (
    RunnerConfig,
    Trial,
    build_basic_trials,
    initialize_csv_file,
    make_argparser,
    resolve_datasets,
    run_grid,
)
from scripts.benchmarking.e2e.worker_lifecycle import (
    check_log_for_buffer_exhaustion,
    dump_worker_log_tail,
    parse_log_to_throughput_csv,
    start_single_node_worker,
    terminate_process_if_exists,
)


def run_combination(trial: Trial, run_dir: str, cli_log_file) -> tuple:
    """Start worker, run build query, parse throughput. Returns (row_dict, issues)."""
    executionMode, numberOfWorkerThreads, bufferSizeInBytes, \
        buffersInGlobalBufferManager, joinStrategy, pageSize = trial.worker_config
    issues = []
    statistic_id = STATISTIC_IDS[trial.statistic_type]

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

    try:
        result = run_build_phase(
            worker_process=worker_process, run_dir=run_dir, log_file_path=log_file_path,
            statistic_type=trial.statistic_type, dataset_name=trial.dataset_name,
            dataset_path=trial.dataset_path, memory_budget=trial.memory_budget,
            window_size=trial.build_window_size_sec, statistic_id=statistic_id,
            cli_log_file=cli_log_file)

        if result.throughput < 0:
            dump_worker_log_tail(log_file_path)
        if not result.ok:
            issues.append(f"build:{result.reason}")
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
            'query_name': f"{trial.statistic_type}Build_{trial.dataset_name}_{trial.memory_budget}_{trial.build_window_size_sec}sec",
            'tuplesPerSecond_listener': result.throughput,
            'build_duration_s': result.duration_s,
        }
        return row, issues
    finally:
        terminate_process_if_exists(worker_process)
        stdout_file.close()
        parse_log_to_throughput_csv(log_file_path, os.path.join(run_dir, "throughput.csv"))


if __name__ == "__main__":
    args = make_argparser("Build statistics, measure build throughput.").parse_args()

    output_dir = args.output_dir if args.output_dir else "."
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "results_statistic_build.csv")

    printInfo("Parsed arguments:")
    for k, v in vars(args).items():
        printInfo(f"  {k}: {v}")
    print()

    check_repository_root()

    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    # Build NES first — cmake configure with ENABLE_LARGE_TESTS=1 fetches the
    # Nexmark / ClusterMonitoring datasets, so dataset-existence checks must
    # come *after* this step.
    if not args.skip_build:
        compile_nebulastream(cmake_flags, build_dir)

    resolve_datasets(args)

    if os.geteuid() == 0:
        try:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write("-1000")
        except OSError:
            pass

    trials = build_basic_trials(args, enable_latency_list=enableLatencyForBuild)
    initialize_csv_file(csv_path, BUILD_FIELDNAMES)

    runner_config = RunnerConfig(
        runner_name="build",
        csv_filename="results_statistic_build.csv",
        fieldnames=BUILD_FIELDNAMES,
    )

    run_grid(args=args, runner_config=runner_config, trials=trials,
             run_combination=run_combination, output_dir=output_dir, csv_path=csv_path)
