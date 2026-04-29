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

"""Statistic probe benchmark.

Phase 1 (Build): runs a build query on a fixed dataset to populate the statistic store.
Phase 2 (Probe): runs a probe query reading from a CSV file with probe tuples.

Measures probe throughput via the throughput listener and query duration for plausibility.
"""

import csv
import os
import time

import yaml

from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    create_folder_and_remove_if_exists,
    printError,
    printInfo,
)
from scripts.benchmarking.e2e._console import step
from scripts.benchmarking.e2e.configs import (
    PROBE_FIELDNAMES,
    QUERY_CONFIGS_DIR,
    STATISTIC_IDS,
    WAIT_BETWEEN_COMMANDS_LONG,
    allBuildWindowSizesSec,
    allBuildWindowsPerProbeWindow,
    allDatasets,
    allNumProbeRepetitions,
    allNumStatisticIds,
    build_dir,
    cmake_flags,
    enableLatencyForProbe,
    histogramMaxValue,
    histogramMinValue,
    memoryBudgetConfig,
    NUM_PROBE_TUPLES,
)
from scripts.benchmarking.e2e.runner_utils import (
    RunnerConfig,
    Trial,
    docker_buffer_override,
    filter_datasets,
    initialize_csv_file,
    make_argparser,
    resolve_datasets,
    run_grid,
    select_sweeps,
    worker_combinations,
)
from scripts.benchmarking.e2e.worker_lifecycle import (
    check_log_for_buffer_exhaustion,
    dump_worker_log_tail,
    parse_average_latency_from_latency_listener,
    parse_average_throughput_from_throughput_listener,
    parse_log_to_throughput_csv,
    start_single_node_worker,
    stop_queries_and_wait,
    submit_query,
    terminate_process_if_exists,
    wait_for_query_to_finish,
)


# === Probe-specific helpers ===================================================

def load_template(name):
    with open(os.path.join(QUERY_CONFIGS_DIR, name), 'r') as f:
        return f.read()


def generate_probe_csv(probe_csv_path, statistic_ids, build_dataset_path,
                       num_probes, num_repetitions,
                       build_window_size_ms, build_windows_per_probe_window):
    """Generate a probe CSV file with probe tuples."""
    if isinstance(statistic_ids, int):
        statistic_ids = [statistic_ids]

    probe_window_size_ms = build_window_size_ms * build_windows_per_probe_window

    with open(build_dataset_path, 'r') as f:
        first_line = f.readline().strip()
        first_timestamp = int(first_line.split(',')[0])
    first_window_start = (first_timestamp // build_window_size_ms) * build_window_size_ms

    probe_rows = []
    for sid in statistic_ids:
        for i in range(num_probes):
            start_ts = first_window_start + i * probe_window_size_ms
            end_ts = start_ts + probe_window_size_ms
            probe_rows.append([sid, start_ts, end_ts, 0])

    total_rows = len(probe_rows) * num_repetitions
    os.makedirs(os.path.dirname(probe_csv_path), exist_ok=True)
    with open(probe_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        for _ in range(num_repetitions):
            writer.writerows(probe_rows)
    return total_rows, first_window_start


def get_build_statistic_ids(statistic_type, num_statistic_ids):
    base_id = STATISTIC_IDS[statistic_type]
    return [base_id + i for i in range(num_statistic_ids)]


def generate_build_query(statistic_type, memory_budget, output_dir, build_dataset_path,
                         num_statistic_ids, dataset_name, build_window_size_sec):
    template = load_template(f"{statistic_type}Build_{dataset_name}.yaml.template")
    base_id = STATISTIC_IDS[statistic_type]
    statistic_ids = get_build_statistic_ids(statistic_type, num_statistic_ids)

    format_args = {
        "statistic_id": base_id,
        "window_size": build_window_size_sec,
        "build_dataset_path": build_dataset_path,
        "memory_budget": memory_budget,
    }

    if statistic_type == "EquiWidthHistogram":
        format_args["min_value"] = histogramMinValue
        format_args["max_value"] = histogramMaxValue
    elif statistic_type not in ("Reservoir", "CountMin"):
        raise ValueError(f"Unknown statistic type: {statistic_type}")

    name = f"{statistic_type}Build_{memory_budget}"

    if num_statistic_ids == 1:
        filepath = os.path.join(output_dir, f"{name}.yaml")
        with open(filepath, 'w') as f:
            f.write(template.format(**format_args))
    else:
        single_yaml = template.format(**format_args)
        doc = yaml.safe_load(single_yaml)
        single_sql = doc['query']
        queries = [single_sql.replace(str(base_id), str(sid), 1) for sid in statistic_ids]
        doc['query'] = queries
        filepath = os.path.join(output_dir, f"{name}.yaml")
        with open(filepath, 'w') as f:
            yaml.dump(doc, f, default_flow_style=False, sort_keys=False)

    return filepath, name, statistic_ids


def generate_probe_query(statistic_type, memory_budget, output_dir, probe_csv_path, dataset_name):
    template = load_template(f"{statistic_type}Probe_{dataset_name}.yaml.template")
    statistic_id = STATISTIC_IDS[statistic_type]

    format_args = {
        "statistic_id": statistic_id,
        "probe_csv_path": probe_csv_path,
        "counter_type": "uint64",
    }

    if statistic_type not in ("Reservoir", "EquiWidthHistogram", "CountMin"):
        raise ValueError(f"Unknown statistic type: {statistic_type}")

    name = f"{statistic_type}Probe_{memory_budget}"
    filepath = os.path.join(output_dir, f"{name}.yaml")
    with open(filepath, 'w') as f:
        f.write(template.format(**format_args))
    return filepath, name


# === Per-trial work ===========================================================

def run_combination(trial: Trial, run_dir: str, cli_log_file) -> tuple:
    """Build then probe for one parameter combination. Returns (row, issues)."""
    executionMode, numberOfWorkerThreads, bufferSizeInBytes, \
        buffersInGlobalBufferManager, joinStrategy, pageSize = trial.worker_config
    issues = []

    num_statistic_ids = trial.extras["num_statistic_ids"]
    build_windows_per_probe_window = trial.extras["build_windows_per_probe_window"]
    num_probe_repetitions = trial.extras["num_probe_repetitions"]
    num_probe_tuples = trial.extras["num_probe_tuples"]

    build_query_path, build_name, statistic_ids = generate_build_query(
        trial.statistic_type, trial.memory_budget, run_dir, trial.dataset_path,
        num_statistic_ids=num_statistic_ids, dataset_name=trial.dataset_name,
        build_window_size_sec=trial.build_window_size_sec)

    probe_csv_path = os.path.abspath(os.path.join(run_dir, f"probe_tuples_{build_name}.csv"))
    probe_query_path, probe_name = generate_probe_query(
        trial.statistic_type, trial.memory_budget, run_dir, probe_csv_path,
        dataset_name=trial.dataset_name)

    log_file_path = os.path.join(run_dir, f"SingleNodeStdout_{build_name}.log")
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

    build_throughput = -1
    build_duration = 0.0
    build_latency = ''
    probe_throughput = -1
    probe_duration = 0.0
    probe_latency = ''

    try:
        # === Phase 1: Build ===
        with step("build", detail=f"{build_name} · {num_statistic_ids} id(s)") as add:
            t0 = time.time()
            build_query_ids = submit_query(build_query_path, cli_log_file)
            build_ok, build_reason = wait_for_query_to_finish(
                build_query_ids, build_query_path, worker_process=worker_process)
            build_duration = time.time() - t0
            stop_queries_and_wait(build_query_ids, build_query_path, cli_log_file)
            build_throughput = parse_average_throughput_from_throughput_listener(log_file_path, build_query_ids)
            if build_throughput >= 0:
                add(f"{build_throughput:,.0f} tup/s")
            else:
                add("no_throughput")
            build_latency = (parse_average_latency_from_latency_listener(log_file_path, build_query_ids)
                             if trial.enable_latency else '')
            if trial.enable_latency and build_latency != -1:
                add(f"lat={build_latency * 1000:.3f}ms")
            if not build_ok:
                add(f"FAIL:{build_reason}")

        if build_throughput < 0:
            dump_worker_log_tail(log_file_path)

        if not build_ok:
            issues.append(f"build:{build_reason}")
        else:
            # === Probe CSV (sized only once we know the build succeeded) ===
            with step("probe-csv") as add:
                total_rows, first_window_start = generate_probe_csv(
                    probe_csv_path, statistic_ids, trial.dataset_path,
                    build_window_size_ms=trial.build_window_size_sec * 1000,
                    build_windows_per_probe_window=build_windows_per_probe_window,
                    num_probes=num_probe_tuples,
                    num_repetitions=num_probe_repetitions)
                add(f"{total_rows:,} tuples")
                add(f"{len(statistic_ids)}id × {num_probe_tuples}win × {num_probe_repetitions}rep")

            # === Phase 2: Probe ===
            with step("probe", detail=probe_name) as add:
                t0 = time.time()
                probe_query_ids = submit_query(probe_query_path, cli_log_file)
                probe_ok, probe_reason = wait_for_query_to_finish(
                    probe_query_ids, probe_query_path, worker_process=worker_process)
                probe_duration = time.time() - t0
                stop_queries_and_wait(probe_query_ids, probe_query_path, cli_log_file)
                probe_throughput = parse_average_throughput_from_throughput_listener(log_file_path, probe_query_ids)
                if probe_throughput >= 0:
                    add(f"{probe_throughput:,.0f} tup/s")
                else:
                    add("no_throughput")
                probe_latency = (parse_average_latency_from_latency_listener(log_file_path, probe_query_ids)
                                 if trial.enable_latency else '')
                if trial.enable_latency and probe_latency != -1:
                    add(f"lat={probe_latency * 1000:.3f}ms")
                if not probe_ok:
                    issues.append(f"probe:{probe_reason}")
                    add(f"FAIL:{probe_reason}")

        if check_log_for_buffer_exhaustion(log_file_path):
            issues = [i for i in issues if "crashed" not in i]
            issues.append("buffer_exhaustion")

        has_buffer_exhaustion = "buffer_exhaustion" in issues
        has_crash = any("crashed" in i for i in issues)
        if build_throughput < 0 and not has_buffer_exhaustion and not has_crash:
            issues.append("build:no_measurements")
        if (probe_throughput < 0 and not has_buffer_exhaustion and not has_crash
                and build_ok):
            issues.append("probe:no_measurements")
        if (trial.enable_latency and build_latency == -1
                and not has_buffer_exhaustion and not has_crash):
            issues.append("build:latency_no_measurements")
        if (trial.enable_latency and probe_latency == -1
                and not has_buffer_exhaustion and not has_crash and build_ok):
            issues.append("probe:latency_no_measurements")

        return {
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
            'query_name': build_name,
            'num_statistic_ids': num_statistic_ids,
            'build_windows_per_probe_window': build_windows_per_probe_window,
            'num_probe_tuples': num_probe_tuples,
            'num_probe_repetitions': num_probe_repetitions,
            'probe_throughput_listener': probe_throughput,
            'probe_duration_s': probe_duration,
            'probe_latency_listener': probe_latency,
            'build_throughput_listener': build_throughput,
            'build_duration_s': build_duration,
            'build_latency_listener': build_latency,
        }, issues
    finally:
        terminate_process_if_exists(worker_process)
        stdout_file.close()
        parse_log_to_throughput_csv(log_file_path, os.path.join(run_dir, f"throughput_{build_name}.csv"))


# === Trial generation =========================================================

def build_probe_trials(args, num_probe_tuples) -> list:
    """Probe sweeps a denser grid than build/accuracy: extra dimensions
    (num_statistic_ids, build_windows_per_probe_window, num_probe_repetitions)
    are stamped into Trial.extras."""
    datasets = filter_datasets(args)
    (worker_threads, buffer_configs, statistic_types_to_run,
     memory_budgets_to_run, _window_sizes_unused, store_types) = select_sweeps(args)

    trials = []
    for dataset in datasets:
        types = statistic_types_to_run or dataset["statistics"]
        for stat_type in types:
            if stat_type not in ("Reservoir", "EquiWidthHistogram", "CountMin"):
                continue
            for mb in memory_budgets_to_run:
                for wc in worker_combinations(worker_threads, buffer_configs, enableLatencyForProbe):
                    execMode, nThreads, (bufSize, buffersGBM), joinStrat, pageSize, enableLatency = wc
                    bufSize, buffersGBM = docker_buffer_override(
                        bufSize, buffersGBM, has_explicit_buffer_config=bool(args.buffer_config))
                    worker_cfg = (execMode, nThreads, bufSize, buffersGBM, joinStrat, pageSize)
                    for num_statistic_ids in allNumStatisticIds:
                        for build_window_size_sec in allBuildWindowSizesSec:
                            for build_windows_per_probe_window in allBuildWindowsPerProbeWindow:
                                for num_probe_repetitions in allNumProbeRepetitions:
                                    for store_type in store_types:
                                        trials.append(Trial(
                                            dataset_name=dataset["name"], dataset_path=dataset["path"],
                                            statistic_type=stat_type, memory_budget=mb,
                                            build_window_size_sec=build_window_size_sec,
                                            worker_config=worker_cfg, enable_latency=enableLatency,
                                            store_type=store_type,
                                            extras={
                                                "num_statistic_ids": num_statistic_ids,
                                                "build_windows_per_probe_window": build_windows_per_probe_window,
                                                "num_probe_repetitions": num_probe_repetitions,
                                                "num_probe_tuples": num_probe_tuples,
                                            },
                                        ))
    return trials


def _probe_banner_extras(trial: Trial) -> str:
    return (f"{trial.extras['num_statistic_ids']}id · "
            f"x{trial.extras['build_windows_per_probe_window']}win · "
            f"{trial.extras['num_probe_repetitions']}rep")


def _probe_run_dir_extras(trial: Trial) -> str:
    return (f"{trial.extras['num_statistic_ids']}ids_"
            f"{trial.extras['build_windows_per_probe_window']}xwindow_"
            f"{trial.extras['num_probe_repetitions']}reps")


if __name__ == "__main__":
    parser = make_argparser("Benchmark statistic probe queries.")
    parser.add_argument("--num-probe-tuples", type=int, default=NUM_PROBE_TUPLES,
                        help="Number of probe tuples to generate.")
    args = parser.parse_args()

    output_dir = args.output_dir if args.output_dir else "."
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "results_statistic_probe.csv")

    printInfo("Parsed arguments:")
    for k, v in vars(args).items():
        printInfo(f"  {k}: {v}")
    print()

    check_repository_root()

    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    if not args.skip_build:
        compile_nebulastream(cmake_flags, build_dir)

    resolve_datasets(args)

    if os.geteuid() == 0:
        try:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write("-1000")
        except OSError:
            pass
    else:
        printInfo("Skipping lowering oom score, as script is not root!")

    trials = build_probe_trials(args, num_probe_tuples=args.num_probe_tuples)
    initialize_csv_file(csv_path, PROBE_FIELDNAMES)

    runner_config = RunnerConfig(
        runner_name="probe",
        csv_filename="results_statistic_probe.csv",
        fieldnames=PROBE_FIELDNAMES,
        banner_extras=_probe_banner_extras,
        run_dir_extras=_probe_run_dir_extras,
    )

    run_grid(args=args, runner_config=runner_config, trials=trials,
             run_combination=run_combination, output_dir=output_dir, csv_path=csv_path)
