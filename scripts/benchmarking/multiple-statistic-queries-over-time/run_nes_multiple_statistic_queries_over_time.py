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

import argparse
import subprocess
import json
import re
import os
import csv
import shutil
import itertools
import random
import time
import socket
import yaml
import pandas as pd

from scripts.benchmarking.utils import *

#### Benchmark Configurations
debug = False
build_dir = os.path.join(".", "build_dir")
build_type = "Release"
if debug:
    build_dir = os.path.join(".", "build_dir_debug")
    build_type = "Debug"

working_dir = os.path.join(build_dir, "working_dir")
latency_csv_file_path = "latency_results_nebulastream.csv"
throughput_csv_file_path = "throughput_results_nebulastream.csv"
config_file = "config.yaml"
single_node_executable = os.path.join(build_dir, "nes-single-node-worker/nes-single-node-worker")
nebuli_executable = os.path.join(build_dir, "nes-frontend/apps/nes-cli") + " --debug"
cmake_flags = ("-G Ninja "
               f"-DCMAKE_BUILD_TYPE={build_type} "
               f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
               "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
               "-DENABLE_LARGE_TESTS=1 "
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
               "-DNES_BUILD_NATIVE:BOOL=ON")
NUM_RUNS_PER_EXPERIMENT = 1
WAIT_BETWEEN_COMMANDS_SHORT = 2
WAIT_BETWEEN_COMMANDS_LONG = 5
WAIT_BEFORE_SIGKILL = 10

#### Worker Configurations
allExecutionModes = ["COMPILER"]
allNumberOfWorkerThreads = [24, 12]
allJoinStrategies = ["HASH_JOIN"]
allNumberOfEntriesSliceCaches = [10]
allPageSizes = [8192]

#### Statistic Build Configurations
allReservoirSizes = [100, 1000, 10000]
allHistogramConfigs = [
    # (num_buckets, min_value, max_value, counter_type)
    (10, 0, 1000000, "uint64"),
    (100, 0, 1000000, "uint64"),
    (1000, 0, 1000000, "uint64"),
]

#### Queries
statisticQueries = {
    "histogram": "scripts/benchmarking/multiple-statistic-queries-over-time/query-configs/statistic/histogram_query.yaml.template",
    "reservoir": "scripts/benchmarking/multiple-statistic-queries-over-time/query-configs/statistic/reservoir_query.yaml.template"}
analyticalQueries = {
    "aggregation": "scripts/benchmarking/multiple-statistic-queries-over-time/query-configs/analytical/agg_query.yaml.template",
    "filter": "scripts/benchmarking/multiple-statistic-queries-over-time/query-configs/analytical/filter_query.yaml.template"}

statistics_query_ids = []
analytical_query_ids = []

def create_output_folder(appendix):
    timestamp = int(time.time())
    folder_name = f"RunBenchmark_{timestamp}_{appendix}"
    create_folder_and_remove_if_exists(folder_name)
    print(f"Created folder {folder_name}...")
    return folder_name


def terminate_process_if_exists(process):
    try:
        process.terminate()
        process.wait(timeout=5)
        print(f"Process with PID {process.pid} terminated.")
    except subprocess.TimeoutExpired:
        print(f"Process with PID {process.pid} did not terminate within timeout. Sending SIGKILL.")
        process.kill()
        process.wait()
        print(f"Process with PID {process.pid} forcefully killed.")


def start_single_node_worker(file_path_stdout):
    # Running the query with a particular worker configuration
    worker_config = (f"--worker.query_engine.number_of_worker_threads={numberOfWorkerThreads} "
                     f"--worker.default_query_execution.execution_mode={executionMode} "
                     f"--worker.default_query_optimization.join_strategy={joinStrategy} "
                     f"--worker.default_query_execution.page_size={pageSize} "
                     f"--worker.latency_listener=true "
                     f"--worker.default_query_execution.operator_buffer_size={bufferSizeInBytes}")

    cmd = f"{single_node_executable} {worker_config}"
    print(f"Starting the single node worker with {cmd}")
    process = subprocess.Popen(cmd.split(" "), stdout=file_path_stdout, stderr=subprocess.STDOUT)
    pid = process.pid
    print(f"Started single node worker with pid {pid}")
    return process


def submitting_query(query_file, cli_log_file):
    cmd = f"{nebuli_executable} -t {query_file} start"
    print(f"Submitting the query via {cmd}...")
    try:
        result = subprocess.run(cmd.split(" "), check=True, stdout=subprocess.PIPE,  # Capture standard output
                                stderr=subprocess.PIPE,  # Capture standard error
                                text=True  # Decode output to a string
                                )
        query_id = result.stdout.strip()
        cli_log_file.write(f"=== Submit query: {cmd} ===\n")
        cli_log_file.write(f"stdout: {result.stdout}\n")
        cli_log_file.write(f"stderr: {result.stderr}\n")
        cli_log_file.flush()
        print(f"Submitted the query with id {query_id}")
        return query_id
    except subprocess.CalledProcessError as e:
        cli_log_file.write(f"=== Submit query FAILED: {cmd} ===\n")
        cli_log_file.write(f"exit status: {e.returncode}\n")
        cli_log_file.write(f"stdout: {e.stdout}\n")
        cli_log_file.write(f"stderr: {e.stderr}\n")
        cli_log_file.flush()
        print("Command failed with exit status:", e.returncode)
        print("Standard output:", e.stdout)
        print("Error output:", e.stderr)
        exit(1)

def start_query(query_id, cli_log_file):
    cmd = f"{nebuli_executable} start {query_id}"
    cli_log_file.write(f"=== Start query: {cmd} ===\n")
    cli_log_file.flush()
    process = subprocess.Popen(cmd.split(" "), stdout=cli_log_file, stderr=cli_log_file)
    return process


def stop_query(query_id, cli_log_file):
    cmd = f"{nebuli_executable} stop {query_id}"
    cli_log_file.write(f"=== Stop query: {cmd} ===\n")
    cli_log_file.flush()
    process = subprocess.Popen(cmd.split(" "), stdout=cli_log_file, stderr=cli_log_file)
    return process


def copy_and_modify_query_config(old_config, new_config, new_source_name, generator_rate_config, generator_type, flush_interval, histogram_config=None, reservoir_size=None):
    # Reading the template file as a string
    with open(old_config, 'r') as input_file:
        template = input_file.read()

    # Build the format arguments
    # The template indents {generator_rate_config} by 8 spaces under a YAML block scalar (|).
    # When the rate config contains newlines, subsequent lines need the same indentation.
    indented_rate_config = generator_rate_config.replace("\n", "\n        ")
    format_args = {
        "source_name": new_source_name,
        "generator_rate_config": indented_rate_config,
        "generator_rate_type": generator_type,
        "flush_interval": flush_interval,
    }
    if histogram_config is not None:
        num_buckets, min_value, max_value, counter_type = histogram_config
        format_args.update(num_buckets=num_buckets, min_value=min_value, max_value=max_value, counter_type=counter_type)
    if reservoir_size is not None:
        format_args["reservoir_size"] = reservoir_size

    # Apply all format placeholders and write the result
    with open(new_config, 'w') as file:
        file.write(template.format(**format_args))


def parse_log_to_latency_csv(log_file_path, csv_file_path):
    # Regular expression to parse each log line
    log_pattern = re.compile(
        r'Latency for queryId (\d+) and (\d+) tasks over duration (\d+)-(\d+) is (\d+\.\d+) (\w?)s'
    )

    # List to store the extracted data
    data = []

    # Open the log file for reading
    with open(log_file_path, mode='r') as log_file:
        for line in log_file:
            # Use regex to find matches in the log line
            match = log_pattern.match(line)
            if match:
                query_id = match.group(1)
                number_of_tasks = match.group(2)
                start_timestamp = int(match.group(3))
                end_timestamp = int(match.group(4))
                latency_value = float(match.group(5))
                unit_prefix = match.group(6)
                latency_value = convert_unit_prefix(latency_value, unit_prefix)
                query_type = "UNDEFINED"
                if query_id in statistics_query_ids:
                    query_type = "STATISTICS"
                else:
                    assert query_id in analytical_query_ids, "Query ID not found in analytical_query_ids."
                    query_type = "ANALYTICAL"


                # Append the extracted data to the list
                data.append((query_id, number_of_tasks, start_timestamp, end_timestamp, latency_value, query_type))

    # Calculate average of the query
    if len(data) == 0:
        return

    # Find the minimum timestamp to normalize
    min_timestamp = min(data, key=lambda x: x[2])[2]

    # Open the CSV file for writing
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        # Write the header
        writer.writerow(
            ['query_id', 'number_of_tasks', 'normalized_start_timestamp', 'normalized_end_timestamp', 'latency', 'query_type'])
        # Write the normalized data to the CSV file
        for query_id, number_of_tasks, start_timestamp, end_timestamp, latency_value, query_type in data:
            normalized_start_timestamp = start_timestamp - min_timestamp
            normalized_end_timestamp = end_timestamp - min_timestamp
            writer.writerow(
                [query_id, number_of_tasks, normalized_start_timestamp, normalized_end_timestamp, latency_value, query_type])


def parse_log_to_throughput_csv(log_file_path, csv_file_path):
    # Regular expression to parse each log line
    log_pattern = re.compile(
        r'Throughput for queryId (\d+) in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s'
    )

    # List to store the extracted data
    data = []

    # Open the log file for reading
    with open(log_file_path, mode='r') as log_file:
        for line in log_file:
            # Use regex to find matches in the log line
            match = log_pattern.match(line)
            if match:
                query_id = match.group(1)
                start_timestamp = int(match.group(2))
                throughput_value = float(match.group(4))
                unit_prefix = match.group(5)
                throughput_value = convert_unit_prefix(throughput_value, unit_prefix)
                query_type = "UNDEFINED"
                if query_id in statistics_query_ids:
                    query_type = "STATISTICS"
                else:
                    assert query_id in analytical_query_ids, "Query ID not found in analytical_query_ids."
                    query_type = "ANALYTICAL"

                # Append the extracted data to the list
                data.append((start_timestamp, query_id, throughput_value, query_type))

    # Calculate average of the query
    if len(data) == 0:
        return

    # Find the minimum timestamp to normalize
    min_timestamp = min(data, key=lambda x: x[0])[0]

    # Open the CSV file for writing
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        # Write the header
        writer.writerow(['normalized_timestamp', 'query_id', 'throughput', 'query_type'])
        # Write the normalized data to the CSV file
        for start_timestamp, query_id, throughput, query_type in data:
            normalized_timestamp = start_timestamp - min_timestamp
            writer.writerow([normalized_timestamp, query_id, throughput, query_type])


def concatenate_csv_files(folders, output_file, config_file, csv_file_path):
    # Initialize an empty list to store DataFrames
    dfs = []

    # Regex for extracting query and

    # Traverse through all subdirectories and files
    for folder in folders:
        # Read the CSV file into a DataFrame
        file_path = os.path.join(folder, csv_file_path)
        df = pd.read_csv(file_path)

        # Read the YAML configuration file
        config_filepath = os.path.join(folder, config_file)
        with open(config_filepath, 'r') as file:
            current_config = yaml.safe_load(file)

        # Assign the entire current_config dictionary to the DataFrame
        df = df.assign(**current_config)

        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    if dfs:
        concatenated_df = pd.concat(dfs, ignore_index=True)

        # Write the concatenated DataFrame to a new CSV file
        concatenated_df.to_csv(output_file, index=False)
        print(f"Concatenated CSV file saved to {output_file}")
    else:
        print("No CSV files found.")


def read_generator_rates(yaml_file_path):
    try:
        with open(yaml_file_path, 'r') as file:
            data = yaml.safe_load(file)

        # Convert each dictionary in the list to a tuple
        list_of_tuples = [(item['type'], item['rate']) for item in data]
        return list_of_tuples
    except yaml.YAMLError as exc:
        print(f"YAML Error: {exc}")
    except KeyError as exc:
        print(f"Key error: {exc}. Ensure the YAML structure matches the expected format.")
    except FileNotFoundError:
        print(f"File not found: {yaml_file_path}")


if __name__ == "__main__":
    # Initialize argument parser
    parser = argparse.ArgumentParser(description="Run NebulaStream queries.")
    parser.add_argument("--wait-between-queries", type=float, default=3.0, help="Time duration in seconds to wait between queries.")
    parser.add_argument("--wait-before-stopping-queries", type=float, default=5.0, help="Time duration in seconds to wait before stopping all queries.")
    parser.add_argument("--generator-rates", type=str, required=True, help="Path to yaml file containing the generator rates of the queries.")
    parser.add_argument("--number-of-queries", type=int, required=True, help="Number of queries to run concurrently. If there are more queries to be run than generator rates are provided, we use the last generator rates for the remaining queries.")
    parser.add_argument("--buffer-size", type=int, required=True, help="Buffer size for NebulaStream.")
    parser.add_argument("--number-of-buffers", type=int, required=True, help="Number of buffers in the buffer manager of NebulaStream")
    parser.add_argument("--flush-interval", type=int, default=5, help="Flush Interval for the generator source")
    parser.add_argument("--statistics-generator-rates", type=str, required=True, help="Path to yaml file containing the generator rates of the statistics queries.")
    parser.add_argument("--number-of-statistics-query-starts", type=int, required=True, help="Number of times to start a batch of statistics queries.")
    parser.add_argument("--statistics-queries-per-start", type=int, required=True, help="Number of statistics queries to start per batch.")
    parser.add_argument("--remove-build-dir", action="store_true", default=False, help="Remove and recreate the build directory before running")
    args = parser.parse_args()

    # Printing all arguments with their parsed values
    print("Parsed arguments:")
    for arg, value in vars(args).items():
        print(f"- {arg}: {value}")
    print()

    allBufferSizes = [args.buffer_size]
    allNumberOfBuffersInGlobalBufferManagers = [args.number_of_buffers]

    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Optionally recreate the build directory
    if args.remove_build_dir:
        create_folder_and_remove_if_exists(build_dir)

    # Reading generator rates from file
    allGeneratorRatesPerQuery = read_generator_rates(args.generator_rates)
    if len(allGeneratorRatesPerQuery) >= args.number_of_queries:
        allGeneratorRatesPerQuery = allGeneratorRatesPerQuery[:args.number_of_queries]
    else:
        allGeneratorRatesPerQuery = allGeneratorRatesPerQuery + [allGeneratorRatesPerQuery[-1]] * (args.number_of_queries - len(allGeneratorRatesPerQuery))

    # Reading statistics generator rates from file
    allStatisticsGeneratorRates = read_generator_rates(args.statistics_generator_rates)
    totalStatisticsQueries = args.number_of_statistics_query_starts * args.statistics_queries_per_start
    if len(allStatisticsGeneratorRates) >= totalStatisticsQueries:
        allStatisticsGeneratorRates = allStatisticsGeneratorRates[:totalStatisticsQueries]
    else:
        allStatisticsGeneratorRates = allStatisticsGeneratorRates + [allStatisticsGeneratorRates[-1]] * (totalStatisticsQueries - len(allStatisticsGeneratorRates))

    # Build NebulaStream
    compile_nebulastream(cmake_flags, build_dir)

    tcp_server_processes = []
    single_node_process = []
    stop_processes = []

    # Iterate over all cross-product combinations
    no_combinations = (
            len(allExecutionModes) *
            len(allNumberOfWorkerThreads) *
            len(allNumberOfBuffersInGlobalBufferManagers) *
            len(allJoinStrategies) *
            len(allNumberOfEntriesSliceCaches) *
            len(allBufferSizes) *
            len(allPageSizes) *
            len(analyticalQueries)
    )
    combinations = itertools.product(allExecutionModes, allNumberOfWorkerThreads,
                                     allNumberOfBuffersInGlobalBufferManagers, allJoinStrategies,
                                     allNumberOfEntriesSliceCaches, allBufferSizes,
                                     allPageSizes, analyticalQueries)

    counter = 0
    new_folders = []
    for [executionMode, numberOfWorkerThreads, buffersInGlobalBufferManager, joinStrategy,
         numberOfEntriesSliceCaches, bufferSizeInBytes, pageSize, analyticalQuery] in combinations:
        try:
            counter += 1
            print(f"Running combination [{counter}/{no_combinations}]")

            # Creating new output folder for this benchmark run and writing the current combination to a file
            folder_name = create_output_folder(analyticalQuery)
            new_folders.append(folder_name)
            with (open(os.path.join(folder_name, config_file), 'w') as file):
                # Write the combination to the file
                config = {
                    "executionMode": executionMode,
                    "numberOfWorkerThreads": numberOfWorkerThreads,
                    "buffersInGlobalBufferManager": buffersInGlobalBufferManager,
                    "joinStrategy": joinStrategy,
                    "numberOfEntriesSliceCaches": numberOfEntriesSliceCaches,
                    "bufferSizeInBytes": bufferSizeInBytes,
                    "pageSize": pageSize,
                    "query": analyticalQuery
                }
                yaml.dump(config, file, default_flow_style=False)

            # Starting the single node worker
            file_path_stdout = os.path.join(folder_name, "SingleNodeStdout.log")
            stdout_file = open(file_path_stdout, 'w')
            single_node_process = start_single_node_worker(stdout_file)

            # Open a log file for nes-cli output
            cli_log_path = os.path.join(folder_name, "nes-cli.log")
            cli_log_file = open(cli_log_path, 'w')
            time.sleep(WAIT_BETWEEN_COMMANDS_LONG)

            # Starting analytical queries
            for analytical_query_number, (generatorRateType, generatorRateConfig) in enumerate(
                    allGeneratorRatesPerQuery):
                new_query_config_name = os.path.join(folder_name, f"analytical_{analyticalQuery}_{analytical_query_number}.yaml")
                copy_and_modify_query_config(analyticalQueries[analyticalQuery], new_query_config_name,
                                             f"analytical_{analyticalQuery}_{analytical_query_number}_source",
                                             generatorRateConfig, generatorRateType, args.flush_interval)
                query_id = submitting_query(new_query_config_name, cli_log_file)
                analytical_query_ids.append(query_id)

            # Waiting to give the engine time to start the queries and for measuring the current throughput
            time.sleep(args.wait_between_queries)

            for concurrent_query_number in range(args.number_of_statistics_query_starts):
                for start_num in range(args.statistics_queries_per_start):
                    query_num = concurrent_query_number * args.statistics_queries_per_start + start_num
                    (statGeneratorRateType, statGeneratorRateConfig) = allStatisticsGeneratorRates[query_num]
                    print(f'query num = {query_num}')
                    # Changing the query yaml file to the new ports etc.
                    random_stat_query = random.choice(list(statisticQueries.keys()))
                    histogramConfig = None
                    if random_stat_query == "histogram":
                        histogramConfig = random.choice(allHistogramConfigs)
                    reservoirSize = None
                    if random_stat_query == "reservoir":
                        reservoirSize = random.choice(allReservoirSizes)
                    new_query_config_name = os.path.join(folder_name, f"statistics_{random_stat_query}_{query_num}.yaml")
                    copy_and_modify_query_config(statisticQueries[random_stat_query], new_query_config_name,
                                                 f"statistics_{random_stat_query}_{query_num}_source",
                                                 statGeneratorRateConfig, statGeneratorRateType, args.flush_interval, histogram_config=histogramConfig, reservoir_size=reservoirSize)
                    # Submitting the query
                    query_id = submitting_query(new_query_config_name, cli_log_file)
                    statistics_query_ids.append(query_id)

                # Waiting to give the engine time to start the query and for measuring the current throughput
                time.sleep(args.wait_between_queries)

            # Waiting to take measurements
            time.sleep(args.wait_before_stopping_queries)

            # Stopping all queries
            stop_processes = []
            for query_id in statistics_query_ids + analytical_query_ids:
                stop_processes.append(stop_query(query_id, cli_log_file))

            # Wait for all stop processes to finish so their output is flushed
            for proc in stop_processes:
                proc.wait()

        finally:
            time.sleep(WAIT_BEFORE_SIGKILL)  # Wait additional time before cleanup
            all_processes = tcp_server_processes + [single_node_process] + stop_processes
            for proc in all_processes:
                print(f"Trying to terminate {proc}")
                if not proc:
                    continue
                terminate_process_if_exists(proc)
            stdout_file.close()
            cli_log_file.close()

        throughput_full_csv_path = os.path.join(folder_name, throughput_csv_file_path)
        parse_log_to_throughput_csv(file_path_stdout, throughput_full_csv_path)

        latency_full_csv_path = os.path.join(folder_name, latency_csv_file_path)
        parse_log_to_latency_csv(file_path_stdout, latency_full_csv_path)


    # After all experiments have been run, we merge all csv files into one main csv file
    latency_concat_file_name = "latency_results_nebulastream_concat.csv"
    throughput_concat_file_name = "throughput_results_nebulastream_concat.csv"
    concatenate_csv_files(new_folders, throughput_concat_file_name, config_file, throughput_csv_file_path)
    concatenate_csv_files(new_folders, latency_concat_file_name, config_file, latency_csv_file_path)

    latency_abs_csv_path = os.path.abspath(latency_concat_file_name)
    throughput_abs_csv_path = os.path.abspath(throughput_concat_file_name)
    print(f"CSV Measurement file can be found in {latency_abs_csv_path}")
    print(f"CSV Measurement file can be found in {throughput_abs_csv_path}")
