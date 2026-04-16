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
Python script that runs the below systest files for different worker configurations
"""

import argparse
import ast
import subprocess
import json
import os
import csv
import shutil
import itertools
import socket
import re
from datetime import datetime, timedelta
import time

from scripts.benchmarking.utils import *

#### Benchmark Configurations
build_dir = os.path.join(".", "build_dir")
working_dir = os.path.join(build_dir, "working_dir")
output_dir = "."
csv_file_path = "results_nebulastream.csv"
benchmark_json_file = os.path.abspath(os.path.join(working_dir, "BenchmarkResults.json"))
systest_executable = os.path.join(build_dir, "nes-systests/systest/systest")
test_data_dir = os.path.abspath(os.path.join(build_dir, "nes-systests/testdata"))
cmake_flags = ("-G Ninja "
               "-DCMAKE_BUILD_TYPE=Release "
               f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
               "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
               "-DENABLE_LARGE_TESTS=1 "
               "-DNES_BUILD_NATIVE:BOOL=ON "
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
               "-DNES_BUILD_NATIVE:BOOL=ON")
NUM_RUNS_PER_EXPERIMENT = 3

#### Worker Configurations
allExecutionModes = ["COMPILER"]  # ["COMPILER", "INTERPRETER"]
allNumberOfWorkerThreads = ['1', '4', '16']  # ['1', '4', '8', '16', '24'] #['4', '16']
allJoinStrategies = ["HASH_JOIN"]
allPageSizes = [8192]
# [4000000] if buffer size is 8192 #[500000] if buffer size is 102400
allBufferConfigs = [(1048576, 20000)]
#allEnableLatencyListeners = [False, True]
allEnableLatencyListeners = [True]
allBuildWindowSizesSec = [1, 30, 60]
#allBuildWindowSizesSec = [1, 60]
throughputListenerInterval = 200

#### Statistic Build Configurations
allReservoirSizes = [
    100,
    500,
    1000
]
allEquiWidthHistogramConfigs = [
    # (num_buckets, min_value, max_value, counter_type)
    (100, 0, 100 * 1000, "uint64"),
    (500, 0, 100 * 1000, "uint64"),
    (1000, 0, 100 * 1000, "uint64"),
]
allCountMinConfigs = [
    # (rows, cols, counter_type)
    (1, 100, "uint64"),
    (5, 100, "uint64"),
    (10, 100, "uint64"),

    (1, 1000, "uint64"),
    (5, 1000, "uint64"),
    (10, 1000, "uint64"),

    (1, 10000, "uint64"),
    (5, 10000, "uint64"),
    (10, 10000, "uint64"),
]

#### Dataset Configurations
# Each dataset lists which statistic types to benchmark.
# Templates are named {StatisticType}Build_{DatasetName}.test.template
allDatasets = [
    {
        "name": "Nexmark",
        "statistics": ["Reservoir", "EquiWidthHistogram", "CountMin"],
    },
    {
        "name": "ClusterMonitoring",
        "statistics": ["Reservoir", "EquiWidthHistogram", "CountMin"],
    },
]

QUERY_CONFIGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query-configs")
generated_test_dir = os.path.join("nes-systests",
                                  f"benchmark_statistic_build_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


def load_template(name):
    """Load a query template from the query-configs directory."""
    template_path = os.path.join(QUERY_CONFIGS_DIR, name)
    with open(template_path, 'r') as f:
        return f.read()


def generate_queries():
    """Generate query dict and .test files from statistic build configurations.

    Iterates over all datasets and build window sizes, using per-dataset templates.
    Returns a dict mapping ``(dataset_name, query_name)`` to a dict with the test
    file path and query metadata (statistic_type, statistic_config, build_window_size_sec).
    """
    os.makedirs(generated_test_dir, exist_ok=True)
    queries = {}

    for dataset in allDatasets:
        dataset_name = dataset["name"]
        stat_types = dataset["statistics"]

        for window_size in allBuildWindowSizesSec:
            if "Reservoir" in stat_types:
                template = load_template(f"ReservoirBuild_{dataset_name}.test.template")
                for reservoir_size in allReservoirSizes:
                    name = f"{dataset_name}_ReservoirBuild_{reservoir_size}_{window_size}sec"
                    filepath = os.path.join(generated_test_dir, f"{name}.test")
                    with open(filepath, 'w') as f:
                        f.write(template.format(reservoir_size=reservoir_size, window_size=window_size))
                    queries[(dataset_name, name)] = {
                        'test_file': f"{filepath}:01",
                        'statistic_type': 'Reservoir',
                        'statistic_config': str(reservoir_size),
                        'build_window_size_sec': window_size,
                    }

            if "EquiWidthHistogram" in stat_types:
                template = load_template(f"EquiWidthHistogramBuild_{dataset_name}.test.template")
                for num_buckets, min_value, max_value, counter_type in allEquiWidthHistogramConfigs:
                    name = f"{dataset_name}_EquiWidthHistogramBuild_{num_buckets}_{min_value}_{max_value}_{counter_type}_{window_size}sec"
                    filepath = os.path.join(generated_test_dir, f"{name}.test")
                    with open(filepath, 'w') as f:
                        f.write(template.format(num_buckets=num_buckets, min_value=min_value,
                                                max_value=max_value, counter_type=counter_type,
                                                window_size=window_size))
                    queries[(dataset_name, name)] = {
                        'test_file': f"{filepath}:01",
                        'statistic_type': 'EquiWidthHistogram',
                        'statistic_config': f"({num_buckets}, {min_value}, {max_value}, {counter_type})",
                        'build_window_size_sec': window_size,
                    }

            if "CountMin" in stat_types:
                template = load_template(f"CountMinBuild_{dataset_name}.test.template")
                for columns, rows, counter_type in allCountMinConfigs:
                    name = f"{dataset_name}_CountMinBuild_{columns}_{rows}_{counter_type}_{window_size}sec"
                    filepath = os.path.join(generated_test_dir, f"{name}.test")
                    with open(filepath, 'w') as f:
                        f.write(template.format(columns=columns, rows=rows, counter_type=counter_type,
                                                window_size=window_size))
                    queries[(dataset_name, name)] = {
                        'test_file': f"{filepath}:01",
                        'statistic_type': 'CountMin',
                        'statistic_config': f"({columns}, {rows}, {counter_type})",
                        'build_window_size_sec': window_size,
                    }

    return queries


def initialize_csv_file():
    """Initialize the CSV file with headers."""
    print("Initializing CSV file...")
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = [
            'dataset', 'statistic_type', 'statistic_config', 'build_window_size_sec',
            'bytesPerSecond', 'query_name', 'time', 'tuplesPerSecond', 'tuplesPerSecond_listener',
            'latency_listener',
            'executionMode', 'numberOfWorkerThreads', 'buffersInGlobalBufferManager',
            'joinStrategy',
            'bufferSizeInBytes', 'pageSize', 'enableLatency', 'issue'
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        printSuccess("CSV file initialized with headers.")


def parse_average_throughput_from_throughput_listener(console_output):
    # Regular expression to parse each log line
    log_pattern = re.compile(
        r'Throughput for queryId (\d+) in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s'
    )

    # List to store the extracted data
    data = []
    for line in console_output.split('\n'):
        # Use regex to find matches in the log line
        match = log_pattern.match(line)
        if match:
            throughput_value = float(match.group(4))
            unit_prefix = match.group(5)
            throughput_value = convert_unit_prefix(throughput_value, unit_prefix)

            # Append the extracted data to the list
            data.append(throughput_value)
    data = data[:-1]

    # Calculate average of the query
    if len(data) == 0:
        return -1
    average_throughput = sum(data) / len(data)
    return average_throughput


def parse_average_latency_from_latency_listener(console_output):
    """Parse latency measurements from the latency listener output.

    Expects lines like:
      Latency for queryId 1 and 1 tasks over duration 12345-12346 is 1.234 ms

    Returns the average latency in seconds, or -1 if no measurements found.
    """
    log_pattern = re.compile(
        r'Latency for queryId (\d+) and (\d+) tasks over duration (\d+)-(\d+) is (\d+\.\d+) (\w*)s'
    )
    unit_multipliers = {'': 1.0, 'm': 1e-3, 'u': 1e-6, 'n': 1e-9}
    data = []
    for line in console_output.split('\n'):
        match = log_pattern.match(line)
        if match:
            latency_value = float(match.group(5))
            unit_prefix = match.group(6)
            multiplier = unit_multipliers.get(unit_prefix, 1.0)
            data.append(latency_value * multiplier)
    if len(data) == 0:
        return -1
    return sum(data) / len(data)


def classify_crash(returncode, stdout):
    """Classify a benchmark failure based on exit code and stdout."""
    signal_num = -returncode if returncode < 0 else (returncode - 128 if returncode > 128 else 0)
    if signal_num > 0:
        signal_names = {6: "SIGABRT", 9: "SIGKILL", 11: "SIGSEGV", 15: "SIGTERM"}
        signal_name = signal_names.get(signal_num, f"signal {signal_num}")
        if "BUFFER_EXHAUSTION" in (stdout or ""):
            return "buffer_exhaustion"
        if signal_num == 9:
            return "oom_killed"
        return f"crashed ({signal_name})"
    return f"failed (exit code {returncode})"


def run_benchmark(config, dataset_name, query, query_info, queryIdx, workerConfigIdx, enableLatency, no_combinations, no_queries):
    """Run a single benchmark. Returns a string describing the issue, or None on success."""
    # Create the working directory
    create_folder_and_remove_if_exists(working_dir, indent="    ")

    stdout = ""
    benchmark_results = []
    issue = None
    try:
        # Running the query with a particular worker configuration
        worker_config = (f"--worker.query_engine.number_of_worker_threads={numberOfWorkerThreads} "
                         f"--worker.default_query_execution.execution_mode={executionMode} "
                         f"--worker.number_of_buffers_in_global_buffer_manager={buffersInGlobalBufferManager} "
                         f"--worker.default_query_optimization.join_strategy={joinStrategy} "
                         f"--worker.query_engine.admission_queue_size=1000000 "
                         f"--worker.default_query_execution.page_size={pageSize} "
                         f"--worker.default_query_execution.operator_buffer_size={bufferSizeInBytes} "
                         f"--worker.latency_listener={enableLatency} "
                         f"--worker.throughput_listener_interval_in_ms={throughputListenerInterval}")

        raw_command = f"{systest_executable} -b -t {os.path.abspath(query_info['test_file'])} --data {os.path.abspath(test_data_dir)} --workingDir={working_dir} -- {worker_config}"

        # Run in a separate systemd scope so that if the OOM killer fires, only this
        # scope is killed — the benchmark script and tmux session survive.
        benchmark_command = f"systemd-run --user --scope --quiet {raw_command}"

        print()
        printInfo(
            f"Running {query} [{queryIdx}/{no_queries}] for worker configuration [{workerConfigIdx}/{no_combinations}]...")
        stdout = run_command(benchmark_command, indent="    ")

        # Parse and save benchmark results
        with open(benchmark_json_file, 'r') as file:
            content = file.read()
            benchmark_results = json.loads(content)
            if not benchmark_results:
                printError(f"    WARNING: No benchmark results found in {benchmark_json_file}")
    except subprocess.CalledProcessError as e:
        issue = classify_crash(e.returncode, e.stdout or "")
        printError(f"Benchmark run {issue} (skipping)")
    except json.JSONDecodeError as e:
        issue = f"json_parse_error"
        printError(f"Failed to parse benchmark output as JSON: {e}")
    except Exception as e:
        issue = f"exception: {e}"
        printError(f"Benchmark run failed (skipping): {e}")

    fieldnames = [
        'dataset', 'statistic_type', 'statistic_config', 'build_window_size_sec',
        'bytesPerSecond', 'query_name', 'time', 'tuplesPerSecond', 'tuplesPerSecond_listener',
        'latency_listener',
        'executionMode', 'numberOfWorkerThreads', 'buffersInGlobalBufferManager',
        'joinStrategy',
        'bufferSizeInBytes', 'pageSize', 'enableLatency', 'issue'
    ]

    with open(csv_file_path, mode='a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        if not benchmark_results:
            issue = issue or "no_results"
            row = {
                'dataset': dataset_name,
                'query_name': query,
                'statistic_type': query_info['statistic_type'],
                'statistic_config': query_info['statistic_config'],
                'build_window_size_sec': query_info['build_window_size_sec'],
                'latency_listener': '',
                'issue': issue,
                **config,
            }
            writer.writerow(row)
            return issue

        average_throughput = parse_average_throughput_from_throughput_listener(stdout)
        latency = parse_average_latency_from_latency_listener(stdout) if enableLatency else ''
        if enableLatency:
            printInfo(f"    Average latency: {latency:.6f} s" if latency != -1 else "    Average latency: no measurements")
        for result in benchmark_results:
            result.pop('query name', None)
            result['dataset'] = dataset_name
            result['query_name'] = query
            result['statistic_type'] = query_info['statistic_type']
            result['statistic_config'] = query_info['statistic_config']
            result['build_window_size_sec'] = query_info['build_window_size_sec']
            result['tuplesPerSecond_listener'] = average_throughput
            result['latency_listener'] = latency
            if enableLatency and latency == -1:
                result['issue'] = 'latency_no_measurements'
            else:
                result['issue'] = 'ok'
            writer.writerow({**result, **config})
        print(f"    Results for config {config} written to CSV.")
    if enableLatency and latency == -1:
        return 'latency_no_measurements'
    return None


def estimate_eta(start_time, end_time, completed_runs, total_runs):
    """Estimate remaining time based on average run duration so far."""
    elapsed = end_time - start_time
    avg_per_run = elapsed / completed_runs
    eta_seconds = avg_per_run * (total_runs - completed_runs)
    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
    eta_h, eta_rem = divmod(eta_seconds, 3600)
    eta_m, eta_s = divmod(eta_rem, 60)
    return int(eta_h), int(eta_m), eta_s, eta_time


def parse_buffer_config(config_strings):
    """Parse a list of buffer config strings into a list of tuples."""
    result = []
    for s in config_strings:
        try:
            parsed = ast.literal_eval(s.strip())
            if isinstance(parsed, tuple) and len(parsed) == 2:
                result.append(parsed)
            else:
                raise ValueError(f"Expected a tuple of 2 elements, got {parsed}")
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Invalid tuple format: {s}. Expected format like '(1234, 100)'") from e
    return result


if __name__ == "__main__":
    # Initialize argument parser
    parser = argparse.ArgumentParser(description="Run NebulaStream queries.")
    parser.add_argument("--all", action="store_true", help="Run all queries.")
    parser.add_argument("-q", "--queries", nargs="+", help="List of queries to run.")
    parser.add_argument("-w", "--worker-threads", nargs="+", help="Number of worker threads to run the queries.")
    parser.add_argument("-b", "--buffer-config", nargs="+",
                        help="List of buffer configurations as tuples and buffer size is first, e.g., '(1234, 100) (128, 40)'.")
    parser.add_argument("--clean", action="store_true",
                        help="Remove and recreate the build directory before building.")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Directory for output CSV files. Created if it does not exist.")
    args = parser.parse_args()

    if args.output_dir:
        output_dir = args.output_dir
        os.makedirs(output_dir, exist_ok=True)
        csv_file_path = os.path.join(output_dir, "results_nebulastream.csv")

    # Generate query .test files from templates
    queries = generate_queries()

    # Determine which queries to run
    queries_to_run = queries

    if not args.all and args.queries:
        # Filter queries based on the query name (second element of the key tuple)
        queries_to_run = {k: v for k, v in queries.items() if k[1] in args.queries}

    # Determine the number of worker threads to run with
    number_of_worker_threads_to_run = allNumberOfWorkerThreads
    if args.worker_threads:
        number_of_worker_threads_to_run = [str(no_worker_threads) for no_worker_threads in args.worker_threads]

    # Parse buffer configurations
    if args.buffer_config:
        allBufferConfigs = parse_buffer_config(args.buffer_config)

    # Print results
    print(",".join(f"{ds}/{qn}" for ds, qn in queries_to_run.keys()))
    print(",".join(number_of_worker_threads_to_run))
    print(",".join(map(str, allBufferConfigs)))
    print(",".join(map(str, allEnableLatencyListeners)))

    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Optionally clean the build directory
    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    # Build NebulaStream
    compile_nebulastream(cmake_flags, build_dir)

    # Init csv files
    initialize_csv_file()

    # Lower our own OOM score so the kernel prefers to kill the system under test, not us.
    try:
        with open("/proc/self/oom_score_adj", "w") as f:
            f.write("-1000")
    except OSError:
        pass

    start_time = time.time()
    problematic_experiments = []

    # Iterate over all cross-product combinations for each query
    no_combinations = (
            len(allExecutionModes) *
            len(number_of_worker_threads_to_run) *
            len(allJoinStrategies) *
            len(allPageSizes) *
            len(allBufferConfigs) *
            len(allEnableLatencyListeners)
    )
    no_queries = len(queries_to_run)
    total_runs = no_queries * no_combinations * NUM_RUNS_PER_EXPERIMENT
    completed_runs = 0
    for queryIdx, ((dataset_name, query), query_info) in enumerate(queries_to_run.items()):
        workerConfigIdx = 0

        combinations = itertools.product(allExecutionModes, number_of_worker_threads_to_run,
                                         allBufferConfigs, allJoinStrategies,
                                         allPageSizes, allEnableLatencyListeners)
        for [executionMode, numberOfWorkerThreads, (bufferSizeInBytes, buffersInGlobalBufferManager), joinStrategy,
             pageSize, enableLatency] in combinations:
            workerConfigIdx += 1

            # Otherwise we run out-of-memory / out-of-buffers
            if not args.buffer_config:
                # For PI 4B with 8 GB of RAM
                if socket.gethostname() == "docker-hostname":
                    buffersInGlobalBufferManager = 40000
                    bufferSizeInBytes = 102400

            config = {
                'executionMode': executionMode,
                'numberOfWorkerThreads': numberOfWorkerThreads,
                'buffersInGlobalBufferManager': buffersInGlobalBufferManager,
                'joinStrategy': joinStrategy,
                'bufferSizeInBytes': bufferSizeInBytes,
                'pageSize': pageSize,
                'enableLatency': enableLatency
            }

            for i in range(NUM_RUNS_PER_EXPERIMENT):
                run_start = time.time()
                experiment_desc = (f"{dataset_name}/{query} "
                                   f"threads={numberOfWorkerThreads} "
                                   f"latency={enableLatency} "
                                   f"run={i}")
                printInfo(f"Starting config at {datetime.now().strftime('%H:%M:%S')}: {experiment_desc}")
                issue = run_benchmark(config, dataset_name, query, query_info, queryIdx + 1, workerConfigIdx, enableLatency, no_combinations, no_queries)
                if issue:
                    problematic_experiments.append({
                        'description': experiment_desc,
                        'issue': issue,
                    })
                run_end = time.time()
                completed_runs += 1
                eta_h, eta_m, eta_s, eta_time = estimate_eta(start_time, run_end, completed_runs, total_runs)
                run_elapsed = run_end - run_start
                run_m, run_s = divmod(run_elapsed, 60)
                printInfo(f"    [{completed_runs}/{total_runs}] finished at {datetime.now().strftime('%H:%M:%S')} "
                         f"(took {int(run_m)}m {run_s:.0f}s) | "
                         f"ETA: {eta_h}h {eta_m}m {eta_s:.0f}s remaining "
                         f"(~{eta_time.strftime('%H:%M:%S')})")

    elapsed = time.time() - start_time
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    printInfo(f"\n\nAll experiment runs completed in {int(hours)}h {int(minutes)}m {seconds:.1f}s")

    abs_csv_path = os.path.abspath(csv_file_path)
    printInfo(f"CSV Measurement file can be found in {abs_csv_path}")

    # --- Report problematic experiments (crashes, buffer exhaustion, other failures) ---
    if problematic_experiments:
        buffer_issues = [p for p in problematic_experiments if p['issue'] == 'buffer_exhaustion']
        crash_issues = [p for p in problematic_experiments if p['issue'] != 'buffer_exhaustion']

        if crash_issues:
            printError(f"\n{'=' * 80}")
            printError(f"CRASHES / FAILURES: {len(crash_issues)}")
            printError(f"{'=' * 80}")
            ct_file = os.path.join(output_dir, "build_crashes_and_failures.txt")
            with open(ct_file, 'w') as f:
                for i, entry in enumerate(crash_issues, 1):
                    msg = (f"[{i}] {entry['description']}\n"
                           f"    Issue: {entry['issue']}\n")
                    printError(msg)
                    f.write(msg + "\n")
            printError(f"Written to {os.path.abspath(ct_file)}")

        if buffer_issues:
            printError(f"\n{'=' * 80}")
            printError(f"BUFFER EXHAUSTION: {len(buffer_issues)}")
            printError(f"{'=' * 80}")
            be_file = os.path.join(output_dir, "build_buffer_exhaustion.txt")
            with open(be_file, 'w') as f:
                for i, entry in enumerate(buffer_issues, 1):
                    msg = (f"[{i}] {entry['description']}\n")
                    printError(msg)
                    f.write(msg + "\n")
            printError(f"Written to {os.path.abspath(be_file)}")
    else:
        printSuccess(f"All {total_runs} benchmark runs completed successfully.")
