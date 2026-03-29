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
Benchmarks statistic probe queries.

Phase 1 (Build): Runs a build query on a fixed dataset to populate the statistic store.
Phase 2 (Probe): Runs a probe query reading from a CSV file with probe tuples.

Measures probe throughput via the throughput listener and query duration for plausibility.

Statistic configurations come from run_statistic_build.py.
Experiment mechanics come from run_nes_multiple_statistic_queries_over_time.py.
"""

import argparse
import socket
import subprocess
import os
import csv
import re
import itertools
import time
import yaml
from datetime import datetime, timedelta

from scripts.benchmarking.utils import *

#### Configuration Constants
# Fixed dataset for the build phase (path relative to build_dir testdata or absolute)
BUILD_DATASET_PATH = "nes-systests/testdata/large/nexmark/bid_6GB.csv"
BUILD_WINDOW_SIZE_SEC = 10
NUM_PROBE_TUPLES = 10
# Number of times to repeat the probe tuples so the probe query runs long enough
# for the throughput listener to capture measurements.
NUM_PROBE_REPETITIONS = 100000

# Statistic hashes used in build queries (must match the hash in the SQL template)
STATISTIC_HASHES = {
    "Reservoir": 100,
    "EquiWidthHistogram": 200,
    "CountMin": 300,
}

#### Benchmark Configurations
build_dir = os.path.join(".", "build_dir")
working_dir = os.path.join(build_dir, "working_dir")
csv_file_path = "results_statistic_probe.csv"
single_node_executable = os.path.join(build_dir, "nes-single-node-worker/nes-single-node-worker")
nebuli_executable = [os.path.join(build_dir, "nes-frontend/apps/nes-cli"), "--debug"]
cmake_flags = ("-G Ninja "
               "-DCMAKE_BUILD_TYPE=Release "
               f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
               "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
               "-DENABLE_LARGE_TESTS=1 "
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
               "-DNES_BUILD_NATIVE:BOOL=ON")
NUM_RUNS_PER_EXPERIMENT = 1
WAIT_BETWEEN_COMMANDS_SHORT = 2
WAIT_BETWEEN_COMMANDS_LONG = 5
WAIT_BEFORE_SIGKILL = 10
BUILD_DONE_TIMEOUT = 30  # seconds of no new throughput before considering build done
PROBE_DONE_TIMEOUT = 15  # seconds of no new throughput before considering probe done

#### Worker Configurations
allExecutionModes = ["COMPILER"]
allNumberOfWorkerThreads = ['1', '4', '16']
allJoinStrategies = ["HASH_JOIN"]
allPageSizes = [8192]
allBufferConfigs = [(1048576, 20000)]
throughputListenerInterval = 200

#### Statistic Build Configurations
allReservoirSizes = [
    100,
    1000,
    10000
]
allEquiWidthHistogramConfigs = [
    # (num_buckets, min_value, max_value, counter_type)
    (10, 0, 1000000, "uint64"),
    (100, 0, 1000000, "uint64"),
    (1000, 0, 1000000, "uint64"),
]
allCountMinConfigs = [
    # (rows, columns, counter_type)
    (1, 10, "uint64"),
    (2, 100, "uint64"),
    (5, 1000, "uint64"),
]

QUERY_CONFIGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query-configs")


def load_template(name):
    """Load a query template from the query-configs directory."""
    template_path = os.path.join(QUERY_CONFIGS_DIR, name)
    with open(template_path, 'r') as f:
        return f.read()


def generate_probe_csv(probe_csv_path, statistic_hash, build_dataset_path,
                       num_probes=NUM_PROBE_TUPLES,
                       num_repetitions=NUM_PROBE_REPETITIONS,
                       window_size_ms=BUILD_WINDOW_SIZE_SEC * 1000):
    """Generate a probe CSV file with probe tuples.

    Schema: STATISTICHASH, STATISTICSTART, STATISTICEND, STATISTICNUMBEROFSEENTUPLES
    Each row probes a different window of the statistic.

    Window boundaries are derived from the first timestamp in the build dataset
    to match the tumbling windows created during the build phase. The set of
    probe tuples is repeated ``num_repetitions`` times so that the probe query
    runs long enough for the throughput listener to capture measurements.
    """
    # Read the first timestamp from the build dataset to compute correct window boundaries
    with open(build_dataset_path, 'r') as f:
        first_line = f.readline().strip()
        first_timestamp = int(first_line.split(',')[0])
    first_window_start = (first_timestamp // window_size_ms) * window_size_ms
    printInfo(f"First data timestamp: {first_timestamp}, first window start: {first_window_start}")

    # Build the base set of probe tuples
    probe_rows = []
    for i in range(num_probes):
        start_ts = first_window_start + i * window_size_ms
        end_ts = start_ts + window_size_ms
        probe_rows.append([statistic_hash, start_ts, end_ts, 0])

    total_rows = num_probes * num_repetitions
    os.makedirs(os.path.dirname(probe_csv_path), exist_ok=True)
    with open(probe_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        for _ in range(num_repetitions):
            writer.writerows(probe_rows)
    printSuccess(f"Generated probe CSV with {total_rows} tuples ({num_probes} x {num_repetitions}) at {probe_csv_path}")


def generate_build_query(statistic_type, config, output_dir, build_dataset_path):
    """Generate a build query yaml file and return its path."""
    template = load_template(f"{statistic_type}Build.yaml.template")
    statistic_hash = STATISTIC_HASHES[statistic_type]

    format_args = {
        "statistic_hash": statistic_hash,
        "window_size": BUILD_WINDOW_SIZE_SEC,
        "build_dataset_path": build_dataset_path,
    }

    if statistic_type == "Reservoir":
        format_args["reservoir_size"] = config
        name = f"ReservoirBuild_{config}"
    elif statistic_type == "EquiWidthHistogram":
        num_buckets, min_value, max_value, counter_type = config
        format_args.update(num_buckets=num_buckets, min_value=min_value,
                           max_value=max_value, counter_type=counter_type)
        name = f"EquiWidthHistogramBuild_{num_buckets}_{min_value}_{max_value}_{counter_type}"
    elif statistic_type == "CountMin":
        rows, columns, counter_type = config
        format_args.update(rows=rows, columns=columns, counter_type=counter_type)
        name = f"CountMinBuild_{rows}_{columns}_{counter_type}"
    else:
        raise ValueError(f"Unknown statistic type: {statistic_type}")

    filepath = os.path.join(output_dir, f"{name}.yaml")
    with open(filepath, 'w') as f:
        f.write(template.format(**format_args))

    return filepath, name


def generate_probe_query(statistic_type, config, output_dir, probe_csv_path):
    """Generate a probe query yaml file and return its path."""
    template = load_template(f"{statistic_type}Probe.yaml.template")
    statistic_hash = STATISTIC_HASHES[statistic_type]

    format_args = {
        "statistic_hash": statistic_hash,
        "probe_csv_path": probe_csv_path,
    }

    if statistic_type == "Reservoir":
        name = f"ReservoirProbe_{config}"
    elif statistic_type == "EquiWidthHistogram":
        num_buckets, min_value, max_value, counter_type = config
        format_args["counter_type"] = counter_type
        name = f"EquiWidthHistogramProbe_{num_buckets}_{min_value}_{max_value}_{counter_type}"
    elif statistic_type == "CountMin":
        rows, columns, counter_type = config
        format_args["counter_type"] = counter_type
        name = f"CountMinProbe_{rows}_{columns}_{counter_type}"
    else:
        raise ValueError(f"Unknown statistic type: {statistic_type}")

    filepath = os.path.join(output_dir, f"{name}.yaml")
    with open(filepath, 'w') as f:
        f.write(template.format(**format_args))

    return filepath, name


def terminate_process_if_exists(process):
    try:
        process.terminate()
        process.wait(timeout=10)
        printInfo(f"Process with PID {process.pid} terminated.")
    except subprocess.TimeoutExpired:
        printError(f"Process with PID {process.pid} did not terminate within timeout. Sending SIGKILL.")
        process.kill()
        process.wait()
        printError(f"Process with PID {process.pid} forcefully killed.")
    # Wait until the gRPC port is free before starting a new worker
    for _ in range(30):
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            if s.connect_ex(('::1', 8080)) != 0:
                break
        time.sleep(1)
    else:
        printError("Port 8080 still in use after 30s")


def start_single_node_worker(file_path_stdout, numberOfWorkerThreads, executionMode,
                             joinStrategy, pageSize, bufferSizeInBytes):
    """Start the single node worker with the given configuration."""
    worker_config = (f"--worker.query_engine.number_of_worker_threads={numberOfWorkerThreads} "
                     f"--worker.default_query_execution.execution_mode={executionMode} "
                     f"--worker.default_query_optimization.join_strategy={joinStrategy} "
                     f"--worker.query_engine.admission_queue_size=1000000 "
                     f"--worker.default_query_execution.page_size={pageSize} "
                     f"--worker.default_query_execution.operator_buffer_size={bufferSizeInBytes} "
                     f"--worker.throughput_listener_interval_in_ms={throughputListenerInterval}")

    cmd = f"{single_node_executable} {worker_config}"
    printInfo(f"Starting the single node worker with {cmd}")
    process = subprocess.Popen(cmd.split(" "), stdout=file_path_stdout, stderr=subprocess.STDOUT)
    pid = process.pid
    printSuccess(f"Started single node worker with pid {pid}")

    # Verify the worker is still alive after startup
    time.sleep(3)
    if process.poll() is not None:
        raise RuntimeError(f"Worker process exited immediately with code {process.returncode}")

    return process


def submit_query(query_file, cli_log_file, retries=3, retry_delay=5):
    """Submit a query via nes-cli and return the query id."""
    cmd = nebuli_executable + ["-t", query_file, "start"]
    printInfo(f"Submitting query via {' '.join(cmd)}...")
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True)
            query_id = result.stdout.strip()
            cli_log_file.write(f"=== Submit query: {cmd} ===\n")
            cli_log_file.write(f"stdout: {result.stdout}\n")
            cli_log_file.write(f"stderr: {result.stderr}\n")
            cli_log_file.flush()
            printSuccess(f"Submitted query with id {query_id}")
            return query_id
        except subprocess.CalledProcessError as e:
            last_error = e
            cli_log_file.write(f"=== Submit query FAILED (attempt {attempt}/{retries}): {cmd} ===\n")
            cli_log_file.write(f"exit status: {e.returncode}\n")
            cli_log_file.write(f"stdout: {e.stdout}\n")
            cli_log_file.write(f"stderr: {e.stderr}\n")
            cli_log_file.flush()
            if attempt < retries:
                printError(f"Submit attempt {attempt}/{retries} failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                printError(f"Command failed with exit status: {e.returncode}")
                printError(f"Standard output: {e.stdout}")
                printError(f"Error output: {e.stderr}")
                raise RuntimeError(f"nes-cli submit failed for {query_file}")


def stop_query(query_id, query_file, cli_log_file):
    """Stop a query via nes-cli."""
    cmd = nebuli_executable + ["-t", query_file, "stop", query_id]
    cli_log_file.write(f"=== Stop query: {' '.join(cmd)} ===\n")
    cli_log_file.flush()
    process = subprocess.Popen(cmd, stdout=cli_log_file, stderr=cli_log_file)
    return process


def wait_for_query_to_finish(log_file_path, timeout, query_id=None, poll_interval=2,
                             max_wait=300, worker_process=None):
    """Wait until no new throughput lines appear for `timeout` seconds.

    Returns a (success, reason) tuple:
      (True, "ok")           — query finished normally
      (False, "crashed:N")   — worker exited with signal/code N
      (False, "timeout")     — max_wait exceeded
    """
    last_line_count = 0
    stable_since = time.time()
    wait_start = time.time()
    query_id_str = str(query_id) if query_id is not None else None
    printInfo(f"Waiting for query to finish (timeout={timeout}s, max_wait={max_wait}s, queryId={query_id_str})...")

    while True:
        elapsed = time.time() - wait_start
        if elapsed > max_wait:
            printError(f"Max wait time ({max_wait}s) exceeded with {last_line_count} measurements. Giving up.")
            return False, "timeout"

        if worker_process is not None and worker_process.poll() is not None:
            printError(f"Worker process exited with code {worker_process.returncode} (likely crashed).")
            return False, f"crashed:{worker_process.returncode}"

        try:
            with open(log_file_path, 'r') as f:
                lines = [l for l in f.readlines() if 'Throughput for queryId' in l]
                if query_id_str is not None:
                    lines = [l for l in lines if f'queryId {query_id_str} ' in l]
            current_count = len(lines)
        except FileNotFoundError:
            time.sleep(poll_interval)
            continue

        if current_count > last_line_count:
            last_line_count = current_count
            stable_since = time.time()
        elif time.time() - stable_since > timeout:
            printSuccess(f"Query finished (no new throughput for {timeout}s, total {current_count} measurements)")
            return True, "ok"

        time.sleep(poll_interval)


def check_log_for_buffer_exhaustion(log_file_path):
    """Check if the worker log contains buffer exhaustion markers."""
    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                if 'BUFFER_EXHAUSTION' in line:
                    return True
    except FileNotFoundError:
        pass
    return False


def parse_average_throughput_from_throughput_listener(log_file_path, query_id=None):
    """Parse throughput from the worker log file for a specific query id."""
    log_pattern = re.compile(
        r'Throughput for queryId (\d+) in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s'
    )

    data = []
    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                match = log_pattern.match(line)
                if match:
                    matched_query_id = match.group(1)
                    if query_id is not None and matched_query_id != str(query_id):
                        continue
                    throughput_value = float(match.group(4))
                    unit_prefix = match.group(5)
                    throughput_value = convert_unit_prefix(throughput_value, unit_prefix)
                    data.append(throughput_value)
    except FileNotFoundError:
        printError(f"Log file {log_file_path} not found.")
        return -1

    # Drop the last measurement (may be partial), but keep at least one
    if len(data) > 1:
        data = data[:-1]

    if len(data) == 0:
        printError(f"No throughput measurements found in {log_file_path}.")
        return -1
    return sum(data) / len(data)


def parse_log_to_throughput_csv(log_file_path, csv_file_path):
    """Parse throughput from the worker log and write to CSV."""
    log_pattern = re.compile(
        r'Throughput for queryId (\d+) in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s'
    )

    data = []
    with open(log_file_path, mode='r') as log_file:
        for line in log_file:
            match = log_pattern.match(line)
            if match:
                query_id = match.group(1)
                start_timestamp = int(match.group(2))
                throughput_value = float(match.group(4))
                unit_prefix = match.group(5)
                throughput_value = convert_unit_prefix(throughput_value, unit_prefix)
                data.append((start_timestamp, query_id, throughput_value))

    if len(data) == 0:
        return

    min_timestamp = min(data, key=lambda x: x[0])[0]

    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['normalized_timestamp', 'query_id', 'throughput'])
        for start_timestamp, query_id, throughput in data:
            normalized_timestamp = start_timestamp - min_timestamp
            writer.writerow([normalized_timestamp, query_id, throughput])


def create_output_folder(appendix):
    timestamp = int(time.time())
    # Sanitize appendix to remove shell-hostile characters (parens, commas, spaces, quotes)
    safe_appendix = str(appendix).replace("(", "").replace(")", "").replace(",", "_").replace(" ", "").replace("'", "")
    folder_name = f"RunBenchmarkProbe_{timestamp}_{safe_appendix}"
    create_folder_and_remove_if_exists(folder_name)
    printSuccess(f"Created folder {folder_name}...")
    return folder_name


def initialize_csv_file():
    """Initialize the CSV file with headers."""
    printInfo("Initializing CSV file...")
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = [
            'statistic_type', 'statistic_config', 'query_name',
            'probe_throughput_listener', 'probe_duration_s',
            'build_throughput_listener', 'build_duration_s',
            'executionMode', 'numberOfWorkerThreads',
            'buffersInGlobalBufferManager', 'joinStrategy',
            'bufferSizeInBytes', 'pageSize'
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        printSuccess("CSV file initialized with headers.")


def generate_all_experiments():
    """Generate the list of (statistic_type, config) pairs."""
    experiments = []
    for reservoir_size in allReservoirSizes:
        experiments.append(("Reservoir", reservoir_size))
    for config in allEquiWidthHistogramConfigs:
        experiments.append(("EquiWidthHistogram", config))
    for config in allCountMinConfigs:
        experiments.append(("CountMin", config))
    return experiments


def run_experiment(statistic_type, statistic_config, worker_config, build_dataset_path,
                   output_dir, cli_log_file):
    """Run a single build+probe experiment. Returns a (result_dict, issues) tuple.

    ``issues`` is a list of strings describing non-fatal problems observed
    during the experiment (e.g. buffer exhaustion, worker crash, timeout).
    """
    executionMode, numberOfWorkerThreads, bufferSizeInBytes, \
        buffersInGlobalBufferManager, joinStrategy, pageSize = worker_config

    # Generate build query
    build_query_path, build_name = generate_build_query(
        statistic_type, statistic_config, output_dir, build_dataset_path)

    # Generate probe CSV
    statistic_hash = STATISTIC_HASHES[statistic_type]
    probe_csv_path = os.path.abspath(os.path.join(output_dir, f"probe_tuples_{build_name}.csv"))
    generate_probe_csv(probe_csv_path, statistic_hash, build_dataset_path)

    # Generate probe query
    probe_query_path, probe_name = generate_probe_query(
        statistic_type, statistic_config, output_dir, probe_csv_path)

    # Start single node worker
    log_file_path = os.path.join(output_dir, f"SingleNodeStdout_{build_name}.log")
    stdout_file = open(log_file_path, 'w')
    single_node_process = start_single_node_worker(
        stdout_file, numberOfWorkerThreads, executionMode,
        joinStrategy, pageSize, bufferSizeInBytes)

    time.sleep(WAIT_BETWEEN_COMMANDS_LONG)

    issues = []
    build_query_id = None
    probe_query_id = None
    try:
        # === Phase 1: Build ===
        printInfo("=" * 60)
        printInfo(f"Phase 1: Build ({build_name})")
        build_start_time = time.time()
        build_query_id = submit_query(build_query_path, cli_log_file)

        # Wait for build query to stop by itself (file source exhausted)
        build_ok, build_reason = wait_for_query_to_finish(
            log_file_path, timeout=BUILD_DONE_TIMEOUT,
            query_id=build_query_id, worker_process=single_node_process)
        build_end_time = time.time()
        build_duration = build_end_time - build_start_time
        printInfo(f"Build phase completed in {build_duration:.1f}s")

        if not build_ok:
            issues.append(f"build:{build_reason}")

        # Stop the build query to flush remaining throughput windows via QueryStop,
        # then parse the throughput from the complete log.
        stop_proc = stop_query(build_query_id, build_query_path, cli_log_file)
        try:
            stop_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            printError("Build query stop timed out")
        time.sleep(WAIT_BETWEEN_COMMANDS_SHORT)

        build_throughput = parse_average_throughput_from_throughput_listener(log_file_path, build_query_id)
        printInfo(f"Build average throughput: {build_throughput:.2f} Tup/s")

        # === Phase 2: Probe ===
        printInfo("=" * 60)
        printInfo(f"Phase 2: Probe ({probe_name})")
        probe_start_time = time.time()
        probe_query_id = submit_query(probe_query_path, cli_log_file)

        # Wait for probe query to stop by itself (file source exhausted)
        probe_ok, probe_reason = wait_for_query_to_finish(
            log_file_path, timeout=PROBE_DONE_TIMEOUT,
            query_id=probe_query_id, worker_process=single_node_process)
        probe_end_time = time.time()
        probe_duration = probe_end_time - probe_start_time
        printInfo(f"Probe phase completed in {probe_duration:.1f}s")

        if not probe_ok:
            issues.append(f"probe:{probe_reason}")

        # Stop the probe query to flush remaining throughput windows via QueryStop,
        # then parse the throughput from the complete log.
        stop_proc = stop_query(probe_query_id, probe_query_path, cli_log_file)
        try:
            stop_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        time.sleep(WAIT_BETWEEN_COMMANDS_SHORT)

        # Check for buffer exhaustion in the worker log
        if check_log_for_buffer_exhaustion(log_file_path):
            issues.append("buffer_exhaustion")

        probe_throughput = parse_average_throughput_from_throughput_listener(log_file_path, probe_query_id)
        printInfo(f"Probe average throughput: {probe_throughput:.2f} Tup/s")

        # Record missing measurements as issues
        if build_throughput < 0:
            issues.append("build:no_measurements")
        if probe_throughput < 0:
            issues.append("probe:no_measurements")

        # Parse full throughput log to CSV
        throughput_csv_path = os.path.join(output_dir, f"throughput_{build_name}.csv")
        parse_log_to_throughput_csv(log_file_path, throughput_csv_path)

        return {
            'statistic_type': statistic_type,
            'statistic_config': str(statistic_config),
            'query_name': build_name,
            'probe_throughput_listener': probe_throughput,
            'probe_duration_s': probe_duration,
            'build_throughput_listener': build_throughput,
            'build_duration_s': build_duration,
            'executionMode': executionMode,
            'numberOfWorkerThreads': numberOfWorkerThreads,
            'buffersInGlobalBufferManager': buffersInGlobalBufferManager,
            'joinStrategy': joinStrategy,
            'bufferSizeInBytes': bufferSizeInBytes,
            'pageSize': pageSize,
        }, issues

    finally:
        time.sleep(WAIT_BEFORE_SIGKILL)
        printInfo("=" * 60)
        printInfo("Cleaning up processes...")

        # Stop queries if still running
        stop_processes = []
        if build_query_id is not None:
            stop_processes.append(stop_query(build_query_id, build_query_path, cli_log_file))
        if probe_query_id is not None:
            stop_processes.append(stop_query(probe_query_id, probe_query_path, cli_log_file))
        for proc in stop_processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

        terminate_process_if_exists(single_node_process)
        stdout_file.close()


def estimate_eta(start_time, end_time, completed_runs, total_runs):
    """Estimate remaining time based on average run duration so far."""
    elapsed = end_time - start_time
    avg_per_run = elapsed / completed_runs
    eta_seconds = avg_per_run * (total_runs - completed_runs)
    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
    eta_h, eta_rem = divmod(eta_seconds, 3600)
    eta_m, eta_s = divmod(eta_rem, 60)
    return int(eta_h), int(eta_m), eta_s, eta_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark statistic probe queries.")
    parser.add_argument("--all", action="store_true", help="Run all statistic configurations.")
    parser.add_argument("--build-dataset", type=str, default=f'{build_dir}/{BUILD_DATASET_PATH}',
                        help="Path to the build dataset CSV file.")
    parser.add_argument("-w", "--worker-threads", nargs="+",
                        help="Number of worker threads to run the queries.")
    parser.add_argument("-b", "--buffer-config", nargs="+",
                        help="Buffer configurations as tuples, e.g., '(1048576, 20000)'.")
    parser.add_argument("--clean", action="store_true",
                        help="Remove and recreate the build directory before building.")
    parser.add_argument("--num-probe-tuples", type=int, default=NUM_PROBE_TUPLES,
                        help="Number of probe tuples to generate.")
    args = parser.parse_args()

    # Printing all arguments
    printInfo("Parsed arguments:")
    for arg, value in vars(args).items():
        printInfo(f"  {arg}: {value}")
    print()

    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Resolve build dataset path
    build_dataset_path = os.path.abspath(args.build_dataset)
    if not os.path.exists(build_dataset_path):
        printError(f"Build dataset not found: {build_dataset_path}")
        exit(1)
    printInfo(f"Using build dataset: {build_dataset_path}")

    # Determine worker threads
    number_of_worker_threads_to_run = allNumberOfWorkerThreads
    if args.worker_threads:
        number_of_worker_threads_to_run = [str(t) for t in args.worker_threads]

    # Parse buffer configurations
    buffer_configs = allBufferConfigs
    if args.buffer_config:
        import ast
        buffer_configs = []
        for s in args.buffer_config:
            parsed = ast.literal_eval(s.strip())
            if isinstance(parsed, tuple) and len(parsed) == 2:
                buffer_configs.append(parsed)
            else:
                raise ValueError(f"Invalid tuple format: {s}")

    # Override probe tuple count
    num_probe_tuples = args.num_probe_tuples

    # Optionally clean the build directory
    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    # Build NebulaStream
    compile_nebulastream(cmake_flags, build_dir)

    # Generate all experiments
    experiments = generate_all_experiments()

    # Create output directory
    generated_dir = f"benchmark_statistic_probe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(generated_dir, exist_ok=True)

    # Init csv file
    initialize_csv_file()

    # Cross product of worker configs
    worker_combinations = list(itertools.product(
        allExecutionModes, number_of_worker_threads_to_run,
        buffer_configs, allJoinStrategies, allPageSizes
    ))

    total_runs = len(experiments) * len(worker_combinations) * NUM_RUNS_PER_EXPERIMENT
    completed_runs = 0
    failed_experiments = []       # hard failures (submit failed, exception)
    problematic_experiments = []  # crashes, timeouts, buffer exhaustion
    start_time = time.time()

    printInfo(f"Total experiments: {len(experiments)}")
    printInfo(f"Total worker configurations: {len(worker_combinations)}")
    printInfo(f"Total runs: {total_runs}")
    print()

    for exp_idx, (statistic_type, statistic_config) in enumerate(experiments):
        for wc_idx, (executionMode, numberOfWorkerThreads,
                     (bufferSizeInBytes, buffersInGlobalBufferManager),
                     joinStrategy, pageSize) in enumerate(worker_combinations):

            worker_config = (executionMode, numberOfWorkerThreads, bufferSizeInBytes,
                             buffersInGlobalBufferManager, joinStrategy, pageSize)

            for run_idx in range(NUM_RUNS_PER_EXPERIMENT):
                run_start = time.time()

                # Create per-run output folder
                run_folder = create_output_folder(
                    f"{statistic_type}_{statistic_config}_{numberOfWorkerThreads}threads")

                # Open CLI log
                cli_log_path = os.path.join(run_folder, "nes-cli.log")
                cli_log_file = open(cli_log_path, 'w')

                experiment_desc = (f"{statistic_type} config={statistic_config} "
                                   f"threads={numberOfWorkerThreads} run={run_idx}")

                try:
                    printInfo(f"\n{'=' * 80}")
                    printInfo(f"Experiment [{completed_runs + 1}/{total_runs}]: {experiment_desc}")
                    printInfo(f"{'=' * 80}")

                    result, issues = run_experiment(
                        statistic_type, statistic_config, worker_config,
                        build_dataset_path, run_folder, cli_log_file)

                    if issues:
                        for issue in issues:
                            problematic_experiments.append({
                                'description': experiment_desc,
                                'issue': issue,
                                'output_folder': run_folder,
                            })

                    if result:
                        # Write result to CSV
                        with open(csv_file_path, mode='a', newline='') as csv_out:
                            fieldnames = [
                                'statistic_type', 'statistic_config', 'query_name',
                                'probe_throughput_listener', 'probe_duration_s',
                                'build_throughput_listener', 'build_duration_s',
                                'executionMode', 'numberOfWorkerThreads',
                                'buffersInGlobalBufferManager', 'joinStrategy',
                                'bufferSizeInBytes', 'pageSize'
                            ]
                            writer = csv.DictWriter(csv_out, fieldnames=fieldnames)
                            writer.writerow(result)
                        printSuccess(f"Results written to {csv_file_path}")

                except Exception as e:
                    printError(f"Experiment failed: {e}")
                    failed_experiments.append({
                        'description': experiment_desc,
                        'error': str(e),
                        'output_folder': run_folder,
                    })
                finally:
                    cli_log_file.close()

                run_end = time.time()
                completed_runs += 1

                if completed_runs < total_runs:
                    eta_h, eta_m, eta_s, eta_time = estimate_eta(
                        start_time, run_end, completed_runs, total_runs)
                    printInfo(f"[{completed_runs}/{total_runs}] took {run_end - run_start:.1f}s | "
                              f"ETA: {eta_h}h {eta_m}m {eta_s:.0f}s remaining "
                              f"(~{eta_time.strftime('%H:%M:%S')})")

    elapsed = time.time() - start_time
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    printInfo(f"\n\nAll experiment runs completed in {int(hours)}h {int(minutes)}m {seconds:.1f}s")

    abs_csv_path = os.path.abspath(csv_file_path)
    printInfo(f"CSV Measurement file can be found in {abs_csv_path}")

    # --- Report problematic experiments (crashes, timeouts, buffer exhaustion) ---
    if problematic_experiments:
        # Split into buffer exhaustion vs crashes/timeouts
        buffer_issues = [p for p in problematic_experiments if p['issue'] == 'buffer_exhaustion']
        crash_timeout_issues = [p for p in problematic_experiments if p['issue'] != 'buffer_exhaustion']

        if crash_timeout_issues:
            printError(f"\n{'=' * 80}")
            printError(f"CRASHES / TIMEOUTS: {len(crash_timeout_issues)}")
            printError(f"{'=' * 80}")
            ct_file = "crashes_and_timeouts.txt"
            with open(ct_file, 'w') as f:
                for i, entry in enumerate(crash_timeout_issues, 1):
                    msg = (f"[{i}] {entry['description']}\n"
                           f"    Issue: {entry['issue']}\n"
                           f"    Output: {entry['output_folder']}\n")
                    printError(msg)
                    f.write(msg + "\n")
            printError(f"Written to {os.path.abspath(ct_file)}")

        if buffer_issues:
            printError(f"\n{'=' * 80}")
            printError(f"BUFFER EXHAUSTION: {len(buffer_issues)}")
            printError(f"{'=' * 80}")
            be_file = "buffer_exhaustion.txt"
            with open(be_file, 'w') as f:
                for i, entry in enumerate(buffer_issues, 1):
                    msg = (f"[{i}] {entry['description']}\n"
                           f"    Output: {entry['output_folder']}\n")
                    printError(msg)
                    f.write(msg + "\n")
            printError(f"Written to {os.path.abspath(be_file)}")

    # --- Report hard failures (submit failed, exceptions) ---
    if failed_experiments:
        printError(f"\n{'=' * 80}")
        printError(f"FAILED EXPERIMENTS: {len(failed_experiments)} out of {total_runs}")
        printError(f"{'=' * 80}")
        failures_file = "failed_experiments.txt"
        with open(failures_file, 'w') as f:
            for i, failure in enumerate(failed_experiments, 1):
                msg = (f"[{i}] {failure['description']}\n"
                       f"    Error: {failure['error']}\n"
                       f"    Output: {failure['output_folder']}\n")
                printError(msg)
                f.write(msg + "\n")
        printError(f"Written to {os.path.abspath(failures_file)}")

    if not failed_experiments and not problematic_experiments:
        printSuccess(f"All {total_runs} experiments completed successfully.")
