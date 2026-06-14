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

"""All benchmark configuration constants for the e2e statistic benchmarks.

Sections (search by ``##``):
  ## General    — paths, cmake flags, run repetition count, query-config dir
  ## Worker     — single-node-worker startup parameters and timing
  ## Build      — build-query parameter sweep (build/accuracy share these)
  ## Accuracy   — accuracy-specific (CountMin internals, range cap)
  ## Probe      — probe-specific parameter sweep
  ## Datasets   — per-dataset metadata + statistic IDs
  ## Fieldnames — CSV columns for each result file
"""

import os

from scripts.benchmarking.common.config import (
    BUILD_DIR,
    NEBULI_EXECUTABLE,
    SINGLE_NODE_EXECUTABLE,
    THROUGHPUT_LISTENER_INTERVAL,
    WAIT_BETWEEN_COMMANDS_LONG,
    WAIT_CHECK_INTERVAL_S,
    WAIT_STABLE_CHECKS,
    WORKING_DIR,
)
from scripts.benchmarking.utils import get_vcpkg_dir


## General #####################################################################

# Re-exported from scripts.benchmarking.common.config for backwards compatibility with existing scripts.
build_dir = BUILD_DIR
working_dir = WORKING_DIR
output_dir = "."
cmake_flags = ("-G Ninja "
               "-DCMAKE_BUILD_TYPE=Release "
               f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
               "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
               "-DENABLE_LARGE_TESTS=1 "
               "-DNES_BUILD_NATIVE:BOOL=ON "
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
               "-DNES_BUILD_NATIVE:BOOL=ON")
NUM_RUNS_PER_EXPERIMENT = 3

QUERY_CONFIGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query-configs")


## Worker ######################################################################

single_node_executable = SINGLE_NODE_EXECUTABLE
nebuli_executable = NEBULI_EXECUTABLE

# Worker query-execution and runtime sweep parameters.
allExecutionModes = ["COMPILER"]                       # ["COMPILER", "INTERPRETER"]
allNumberOfWorkerThreads = ['1', '16']                       # ['1', '4', '16']
allJoinStrategies = ["HASH_JOIN"]
allPageSizes = [8192]
allBufferConfigs = [(100 * 1024, 200 * 1000)]          # (bufferSizeInBytes, buffersInGlobalBufferManager)
allStatisticStoreTypes = ["DEFAULT", "WINDOW", "SUB_STORES"]

# Throughput listener emits a measurement every X ms.
throughputListenerInterval = THROUGHPUT_LISTENER_INTERVAL

# WAIT_* constants re-exported from common.config; modules that already import them by these names
# (worker_lifecycle, runner_utils, accuracy/probe runners) keep working unchanged.
__all__ = [
    "WAIT_BETWEEN_COMMANDS_LONG",
    "WAIT_STABLE_CHECKS",
    "WAIT_CHECK_INTERVAL_S",
]


## Build #######################################################################

# Tumbling window sizes (sec) used by the build query.
allBuildWindowSizesSec = [1, 5, 10]

# Memory budgets (bytes) used to size each synopsis. The actual rows / cols /
# buckets / sample-size are derived inside the C++ logical functions'
# calculateConfigs() during lowering.
memoryBudgetConfig = [1 * 1024, 5 * 1024, 10 * 1024]
# memoryBudgetConfig =  [1 * 1024, 10 * 1024]

# Legacy histogram bounds; the runner now derives min/max from the data instead.
histogramMinValue = 0
histogramMaxValue = 100 * 1000

# Latency listener flag for build-throughput experiments. Disabled because we
# only care about throughput here.
enableLatencyForBuild = [False]


## Accuracy ####################################################################

# Mirrors the C++ constants in CountMinSketchLogicalFunction. If those change,
# update here too.
kCountMinRows = 3
kCountMinSeed = 42
kCountMinCounterBytes = 8

# Cap on the number of distinct values used to estimate a CountMin range. The
# range query is a sum of point estimates, one per value in [range_lo, range_hi];
# above this cap, Python uniformly sub-samples the range.
COUNTMIN_RANGE_MAX_VALUES = 100

enableLatencyForAccuracy = [False]


## Probe #######################################################################

# Number of build windows covered by a single probe window:
# probe_window_size = build_window_size_sec * build_windows_per_probe_window
allBuildWindowsPerProbeWindow = [1, 100]

# Base number of probe tuples per (statistic_id, repetition).
allNumProbeTuples = [1]

# Number of times the base tuple set is repeated so the probe query runs long
# enough for the throughput listener to capture measurements.
allNumProbeRepetitions = [1000]

# Number of distinct statistic IDs probed concurrently.
allNumStatisticIds = [1, 10]

# Latency listener for probe — enabled (we want both throughput and latency).
enableLatencyForProbe = [True]


## Datasets ####################################################################

# Each dataset lists which statistic types to benchmark.
# Templates are named {StatisticType}Build_{DatasetName}.yaml.template
allDatasets = [
    {
        "name": "Nexmark",
        "statistics": ["Reservoir", "EquiWidthHistogram", "CountMin", "Passthrough", "Sum"],
    },
    {
        "name": "ClusterMonitoring",
        "statistics": ["Reservoir", "EquiWidthHistogram", "CountMin", "Passthrough", "Sum"],
    },
]

# Statistic IDs used in build queries (must match the ID in the SQL template).
# Each statistic type gets its own ID range so multi-statistic builds within a
# single benchmark run don't collide in the StatisticStore.
STATISTIC_IDS = {
    "Reservoir": 100,
    "EquiWidthHistogram": 200,
    "CountMin": 300,
    "Passthrough": 400,
    "Sum": 500,
}

# Query types that don't populate the StatisticStore. They share the build
# pipeline (template + worker invocation) but ignore memory_budget and the
# statisticStoreType selection, so the runner pins those dimensions to a
# single value to avoid redundant runs.
STATISTIC_TYPES_WITHOUT_SYNOPSIS_PARAMS = {"Passthrough", "Sum"}

# Build dataset paths keyed by dataset name. Resolved relative to the build
# directory because the systest harness symlinks them under build_dir/nes-systests/testdata.
DATASET_PATHS = {
    "Nexmark": "nes-systests/testdata/large/nexmark/bid_6GB.csv",
    "ClusterMonitoring": "nes-systests/testdata/large/cluster_monitoring/google-cluster-data-original_1G.csv",
}


## Fieldnames ##################################################################

# Identity columns are present in all three result CSVs and form the join key
# for combining results: build × accuracy × probe.
IDENTITY_FIELDNAMES = [
    'dataset', 'statistic_type', 'memory_budget', 'build_window_size_sec',
    'executionMode', 'numberOfWorkerThreads', 'buffersInGlobalBufferManager', 'joinStrategy',
    'bufferSizeInBytes', 'pageSize', 'enableLatency', 'statisticStoreType', 'run_idx',
]

BUILD_FIELDNAMES = IDENTITY_FIELDNAMES + [
    'query_name', 'tuplesPerSecond_listener', 'build_duration_s',
    'issue',
]

ACCURACY_FIELDNAMES = IDENTITY_FIELDNAMES + [
    'point_column', 'point_value', 'range_lo', 'range_hi',
    'point_avg_relative_error', 'point_num_windows',
    'range_avg_relative_error', 'range_num_windows',
    'range_n_samples', 'accuracy_notes',
    'issue',
]

PROBE_FIELDNAMES = IDENTITY_FIELDNAMES + [
    'query_name',
    'num_statistic_ids', 'build_windows_per_probe_window',
    'num_probe_tuples', 'num_probe_repetitions',
    'probe_throughput_listener', 'probe_duration_s', 'probe_latency_listener',
    'build_throughput_listener', 'build_duration_s', 'build_latency_listener',
    'issue',
]
