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

"""All benchmark configuration constants for the statistic_build_probe benchmarks.

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
#NUM_RUNS_PER_EXPERIMENT = 3
NUM_RUNS_PER_EXPERIMENT = 10

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

# Legacy histogram bounds; the runner now derives min/max from the data instead.
histogramMinValue = 0
histogramMaxValue = 100 * 1000


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
    "Manufacturing": "nes-systests/testdata/large/manufacturing/manufacturing_1G.csv",
}


## Synopsis amortization #######################################################
#
# The amortization experiment runs ONE build query maintaining N synopses (one scan, one duration, one
# throughput) to measure how the per-tuple cost of N specialised synopses (CountMin / EquiWidthHistogram,
# one per field) grows relative to a single full-record Reservoir sample.
#
# There is no hardcoded schema here. synopsis_query_builder.py loads a per-dataset YAML skeleton
# (query-configs/SynopsisAmortization_<Dataset>.yaml.template) that carries the logical source schema, the
# Memory/Native source, and the event-time field (in its placeholder WINDOW clause). The builder parses that,
# derives the numeric fields, and writes a concrete query YAML on the fly. To add a dataset: drop a skeleton
# in query-configs/ and a path in DATASET_PATHS.

# Datasets benchmarked by the amortization runner (each needs a skeleton template + a DATASET_PATHS entry).
# They span the field-count axis the experiment varies over: Nexmark F=3 (narrow), ClusterMonitoring F=10
# (wide, mixed types), Manufacturing F=13 (widest, uniform INT16). Nexmark is 6 GB but cheap in trial count.
SYNOPSIS_AMORT_DATASETS = ["Nexmark", "ClusterMonitoring", "Manufacturing"]

# Statistic types the amortization runner sweeps. CountMin / EquiWidthHistogram
# scale N = 1..F (one synopsis per field); Reservoir / Sum / Passthrough are
# pinned to N = 1 (one full-record sample / one baseline aggregation / the raw
# copy ceiling).
SYNOPSIS_AMORT_SCALING_TYPES = ["CountMin", "EquiWidthHistogram"]
SYNOPSIS_AMORT_SINGLE_TYPES = ["Reservoir", "Sum", "Passthrough"]

# Number-of-synopses ladder for the scaling types. Clamped to each dataset's
# field count F and always augmented with F itself, then deduped + sorted.
SYNOPSIS_AMORT_N_LADDER = [1, 2, 4, 8]

# Tumbling window size(s), in seconds. The amortization experiment isolates per-tuple synopsis cost, so it
# defaults to a single size; pass --window-sizes to sweep several (e.g. 1 5 10) and compare across windowing.
SYNOPSIS_AMORT_WINDOW_SIZES = [1, 5, 10]

# Memory budgets (bytes) for the scaling/sample synopses. Sum/Passthrough ignore this.
SYNOPSIS_AMORT_MEMORY_BUDGETS = [1 * 1024, 10 * 1024, 100 * 1024]

# Worker thread counts.
SYNOPSIS_AMORT_WORKER_THREADS = ["1", "16"]

# Fixed EquiWidthHistogram bounds. Mirrors the effective behaviour of the
# existing single-synopsis build templates (which hardcode 0..100000 regardless
# of column); throughput — not accuracy — is what we measure, so wide static
# bounds avoid an expensive per-field scan of multi-GB inputs.
SYNOPSIS_AMORT_HISTOGRAM_MIN = 0
SYNOPSIS_AMORT_HISTOGRAM_MAX = 100 * 1000

# Base statistic IDs per synopsis kind; the i-th synopsis in a query uses base+i,
# so all ids within one build query stay distinct (the engine requires this).
SYNOPSIS_AMORT_BASE_IDS = {
    "CountMin": 1000,
    "EquiWidthHistogram": 2000,
    "Reservoir": 3000,
    "Sum": 4000,
    "Passthrough": 5000,
}


## Fieldnames ##################################################################

# Identity columns are present in all three result CSVs and form the join key
# for combining results: build × accuracy × probe.
IDENTITY_FIELDNAMES = [
    'dataset', 'statistic_type', 'memory_budget', 'build_window_size_sec',
    'executionMode', 'numberOfWorkerThreads', 'buffersInGlobalBufferManager', 'joinStrategy',
    'bufferSizeInBytes', 'pageSize', 'enableLatency', 'statisticStoreType', 'run_idx',
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

# Amortization results — the build-throughput experiment (one query, N synopses).
# Filtering num_synopses == 1 reproduces the single-synopsis build view; CountMin /
# EquiWidthHistogram rows with num_synopses > 1 carry the scaling data.
SYNOPSIS_AMORT_FIELDNAMES = IDENTITY_FIELDNAMES + [
    'num_synopses', 'query_name', 'tuplesPerSecond_listener', 'build_duration_s',
    'issue',
]
