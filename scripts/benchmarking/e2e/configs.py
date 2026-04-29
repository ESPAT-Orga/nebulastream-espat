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

from scripts.benchmarking.utils import get_vcpkg_dir


## General #####################################################################

build_dir = os.path.join(".", "build_dir")
working_dir = os.path.abspath(os.path.join(build_dir, "working_dir"))
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

single_node_executable = os.path.join(build_dir, "nes-single-node-worker/nes-single-node-worker")
nebuli_executable = [os.path.join(build_dir, "nes-frontend/apps/nes-cli"), "--debug"]

# Worker query-execution and runtime sweep parameters.
allExecutionModes = ["COMPILER"]                       # ["COMPILER", "INTERPRETER"]
allNumberOfWorkerThreads = ['1', '16']                       # ['1', '4', '16']
allJoinStrategies = ["HASH_JOIN"]
allPageSizes = [8192]
allBufferConfigs = [(100 * 1024, 200 * 1000)]          # (bufferSizeInBytes, buffersInGlobalBufferManager)
allStatisticStoreTypes = ["DEFAULT", "WINDOW", "SUB_STORES"]

# Throughput listener emits a measurement every X ms.
throughputListenerInterval = 100

# Seconds to wait between starting the worker and submitting the first query.
WAIT_BETWEEN_COMMANDS_LONG = 5

# A query that finishes naturally (file source exhausted) flips its status to
# "Stopped". We poll status WAIT_STABLE_CHECKS times, WAIT_CHECK_INTERVAL_S
# apart; only when all polls in the window report "Stopped" do we treat the
# query as done.
WAIT_STABLE_CHECKS = 5
WAIT_CHECK_INTERVAL_S = 0.1


## Build #######################################################################

# Tumbling window sizes (sec) used by the build query.
allBuildWindowSizesSec = [1, 60]

# Memory budgets (bytes) used to size each synopsis. The actual rows / cols /
# buckets / sample-size are derived inside the C++ logical functions'
# calculateConfigs() during lowering.
memoryBudgetConfig = [1 * 1024, 5 * 1024, 10 * 1024] # [1 * 1024, 10 * 1024]

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
allBuildWindowsPerProbeWindow = [1, 1000]

# Base number of probe tuples per (statistic_id, repetition).
NUM_PROBE_TUPLES = 10

# Number of times the base tuple set is repeated so the probe query runs long
# enough for the throughput listener to capture measurements.
allNumProbeRepetitions = [1, 100]

# Number of distinct statistic IDs probed concurrently.
allNumStatisticIds = [1]

# Latency listener for probe — enabled (we want both throughput and latency).
enableLatencyForProbe = [True]


## Datasets ####################################################################

# Each dataset lists which statistic types to benchmark.
# Templates are named {StatisticType}Build_{DatasetName}.yaml.template
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

# Statistic IDs used in build queries (must match the ID in the SQL template).
# Each statistic type gets its own ID range so multi-statistic builds within a
# single benchmark run don't collide in the StatisticStore.
STATISTIC_IDS = {
    "Reservoir": 100,
    "EquiWidthHistogram": 200,
    "CountMin": 300,
}

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
