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

"""Configuration for the statistic_overhead benchmark.

The experiment holds a fixed analytical workload (NUM_ANALYTICAL_QUERIES concurrent Nexmark Q8
windowed joins) and sweeps the number of statistic queries running alongside it, to show that
in-engine statistic collection is near-free. The statistic queries maintain equi-width histograms
over the join key (person.id / auction.seller) — i.e. the input a streaming optimizer needs for join
cardinality estimation.

Statistic queries are added in person/auction *pairs*, since estimating a join's cardinality needs a
histogram on both sides. So STATISTIC_QUERY_COUNTS' maximum of 2 * NUM_ANALYTICAL_QUERIES means
"every analytical query is covered by a full pair of join-key histograms".

Every constant is overridable from the environment so the shell wrapper can drive smoke runs.

Sections (search by ``##``):
  ## General    — paths, cmake flags, repetition count
  ## Workload   — the sweep itself
  ## Worker     — single-node-worker startup parameters
  ## Generator  — Nexmark TCP producer
  ## Fieldnames — CSV columns
"""

import os

from scripts.benchmarking.common.config import BUILD_DIR, WAIT_BETWEEN_COMMANDS_LONG
from scripts.benchmarking.utils import get_vcpkg_dir


def _int(name, default):
    return int(os.environ.get(name, default))


def _int_list(name, default):
    raw = os.environ.get(name)
    return [int(x) for x in raw.split(",") if x.strip()] if raw else default


## General #####################################################################

build_dir = BUILD_DIR
QUERY_CONFIGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query-configs")
TOPOLOGY_TEMPLATE = os.path.join(QUERY_CONFIGS_DIR, "topology.yaml.template")

def cmake_flags():
    """Lazy, because get_vcpkg_dir() raises on any host it does not know about — evaluating this at
    import time would make the module unimportable there, including for --skip-build runs."""
    return ("-G Ninja "
            "-DCMAKE_BUILD_TYPE=Release "
            f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
            "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
            # No large test data: this benchmark's input comes from the Nexmark TCP generator, so
            # enabling them would only download multi-GB CSVs nothing here reads.
            "-DENABLE_LARGE_TESTS=0 "
            "-DNES_BUILD_NATIVE:BOOL=ON "
            "-DNES_LOG_LEVEL:STRING=LEVEL_NONE")

NUM_RUNS = _int("NUM_RUNS", 5)
RESULTS_CSV = "results_statistic_overhead.csv"


## Workload ####################################################################

# Fixed analytical workload: N concurrent Nexmark Q8 joins.
NUM_ANALYTICAL_QUERIES = _int("NUM_ANALYTICAL_QUERIES", 10)

# The swept axis. 0 is the baseline arm the paper's claim is measured against.
STATISTIC_QUERY_COUNTS = _int_list("STATISTIC_QUERY_COUNTS", [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20])

# Tumbling window size (seconds) used by both the join and the histograms, so the statistic queries
# close windows on the same cadence as the workload they observe.
WINDOW_SIZE_SEC = _int("WINDOW_SIZE_SEC", 10)

# EQUIWIDTHHISTOGRAM's third argument is a memory budget in BYTES, not a bucket count:
# numBuckets = max(1, (budget - 8) / 24)   [EquiWidthHistogramLogicalFunction.cpp]
# 10 KiB therefore buys ~426 bins over the key domain.
MEMORY_BUDGET = _int("MEMORY_BUDGET", 10 * 1024)

# Statistic ids must be unique across all queries alive in one run.
STATISTIC_ID_BASE_PERSON = _int("STATISTIC_ID_BASE_PERSON", 2000)
STATISTIC_ID_BASE_AUCTION = _int("STATISTIC_ID_BASE_AUCTION", 3000)

# Seconds of steady state to measure, and how much of the run to discard first. Throughput windows
# whose start falls inside the warm-up are dropped, so query start-up and compilation don't count.
WARMUP_SECONDS = _int("WARMUP_SECONDS", 20)
MEASUREMENT_WINDOW_SECONDS = _int("MEASUREMENT_WINDOW_SECONDS", 60)

# Seconds between starting the worker and submitting the topology.
WAIT_AFTER_WORKER_START = WAIT_BETWEEN_COMMANDS_LONG


## Worker ######################################################################

WORKER_THREADS = _int("WORKER_THREADS", 16)
EXECUTION_MODE = os.environ.get("EXECUTION_MODE", "COMPILER")
JOIN_STRATEGY = os.environ.get("JOIN_STRATEGY", "HASH_JOIN")
PAGE_SIZE = _int("PAGE_SIZE", 8192)

# Many small buffers, NOT few large ones. The binding resource for concurrent windowed joins is the
# buffer COUNT, not the pool's size in bytes: a join takes a buffer per unit of state however full it
# gets, so a large operator_buffer_size just wastes most of each allocation. Measured at 10 Q8 joins,
# N=0, 200k tup/s/query (all "fail" = 1 healthy query, the rest starved with BUFFER_EXHAUSTION):
#
#   buffer    count   pool    5 joins   10 joins
#   100 KiB   200k    20 GB   fail      fail
#    32 KiB   300k   9.8 GB   -         fail
#     8 KiB     1M    8 GB    -         ok, sustained 99.99% of offered load
#
# Note the 20 GB pool failing where the 8 GB pool works. Do not "fix" a starved run by enlarging
# buffers; raise the count. The sibling statistic_build_probe values (100 KiB x 200k) are fine for
# its single-query file replay and wrong here.
BUFFER_SIZE_IN_BYTES = _int("BUFFER_SIZE_IN_BYTES", 8 * 1024)
BUFFERS_IN_GLOBAL_BUFFER_MANAGER = _int("BUFFERS_IN_GLOBAL_BUFFER_MANAGER", 1000 * 1000)
STATISTIC_STORE_TYPE = os.environ.get("STATISTIC_STORE_TYPE", "SUB_STORES")
ENABLE_LATENCY = os.environ.get("ENABLE_LATENCY", "false").lower() == "true"
THROUGHPUT_LISTENER_INTERVAL_MS = _int("THROUGHPUT_LISTENER_INTERVAL_MS", 200)

# systemd-run --user --scope wraps the worker so cgroup teardown catches stragglers. Disable on
# hosts without a user systemd instance.
USE_SYSTEMD_RUN = os.environ.get("USE_SYSTEMD_RUN", "true").lower() == "true"

# Not configurable: the topology template hardwires localhost:8080 / localhost:9090 into every
# sink, physical source and worker entry, so moving the worker means editing the template too.


## Submission mode ############################################################
#
# "shared"   — one nes-repl per analytical query; the histograms ride along as companion statistics,
#              spliced onto the join's own source operators (SpliceToRunningSourceTrait), so they add
#              NO extra TCP connections. 10 joins x 2 sources = 20 connections at every N. This is
#              the arm that measures the *marginal* cost of maintaining a synopsis.
# "isolated" — one nes-cli topology with every query submitted separately; each statistic query gets
#              its own source and re-ingests the stream. 20 + N connections. Measures the cost of
#              monitoring queries *including* their ingestion.
MODE = os.environ.get("MODE", "shared")

# nes-repl companion flags. MinVal/MaxVal/Selectivity all map to Equi_Width_Histogram
# (DefaultStatisticQueryGenerator::toStatisticType); the histogram bounds options are documented
# against MinVal, so that is what we ask for.
COMPANION_METRIC = os.environ.get("COMPANION_METRIC", "MinVal")
# Field names are resolved inside the build branch, whose plan holds exactly one source, so the
# unqualified name is unambiguous there. Uppercase because the build chain does not normalise the
# event-time field the way it does the aggregation field.
COMPANION_EVENT_TIME_FIELD = os.environ.get("COMPANION_EVENT_TIME_FIELD", "TIMESTAMP")


## Generator ###################################################################

NEXMARK_GENERATOR_IMAGE = os.environ.get("NEXMARK_GENERATOR_IMAGE", "nes-bench-nexmark-gen:local")
NEXMARK_GENERATOR_CONTAINER = os.environ.get("NEXMARK_GENERATOR_CONTAINER", "nes-nexmark-gen")
NEXMARK_BUILD_CONTEXT = "rust-nexmark-generator"

# port_base+0 serves `person`, port_base+1 serves `auction`.
NEXMARK_PORT_BASE = _int("NEXMARK_PORT_BASE", 9200)
GENERATOR_SEED = _int("GENERATOR_SEED", 42)

# Virtual clock. Decides how many tuples fall into one window, hence how much join state is live:
#   persons_per_window  = EVENTS_PER_SEC * WINDOW_SIZE_SEC / 50
#   auctions_per_window = 3 * persons_per_window
# Lower it if a run hits BUFFER_EXHAUSTION.
EVENTS_PER_SEC = _int("EVENTS_PER_SEC", 100_000)

# Join-key domain: person.id cycles through [0, PERSON_DOMAIN) and auction.seller is drawn uniformly
# from it. Also the histogram's max_value. Set to persons-per-window for ~1 matching person per
# auction — which is what the default (100_000 * 10 / 50 = 20_000) does.
PERSON_DOMAIN = _int("PERSON_DOMAIN", 20_000)

# Offered load, in tuples/sec delivered to ONE query reading both streams (person + auction).
# 0 = unlimited. The generator converts this to an event-time replay speed internally and paces every
# stream against that one clock; see rust-nexmark-generator/src/main.rs.
#
# Two measured failures forced this to be an event-time knob rather than a tuples/sec one:
#
#  1. Unlimited: NebulaStream's buffer pool is global with no per-query fairness, so the first
#     query's sources drain it. With 10 concurrent Q8 joins exactly one query stayed healthy
#     (314 throughput windows) while the other nine got 1-8 each, and BUFFER_EXHAUSTION appeared
#     one line after the first throughput line.
#  2. Paced per connection in tuples/sec: Nexmark puts 1 person and 3 auctions in every 50 events,
#     so equal tuple rates advance person's event time 3x faster than auction's. The windowed join
#     then buffers the fast side forever waiting for the slow side's watermark — at only 500k tup/s
#     offered, ZERO of 10 queries produced any throughput.
#
# Pacing on event time keeps every stream on one clock whatever its share of the mix. The offered
# load is then fixed and the experiment reports sustained throughput against it; pick the operating
# point at the calibration knee via --tuples-per-sec at N=0.
#
# Measured on amd7950x3d (10 Q8 joins, 16 worker threads, 8 KiB x 1M buffers): the ceiling is
# ~2.17 Mtup/s aggregate, i.e. ~217k per query. 200_000 offers 2.00 Mtup/s and sustains 100% of it,
# which is 92% of the ceiling — loaded, with enough headroom not to sit on the knee.
TUPLES_PER_SEC_PER_QUERY = _int("TUPLES_PER_SEC_PER_QUERY", 200_000)
TUPLES_PER_SEC_LIST = _int_list("TUPLES_PER_SEC_LIST", [TUPLES_PER_SEC_PER_QUERY])


def offered_tps(tuples_per_sec_per_query, num_statistic_queries):
    """Tuples/sec the generator offers across every connection of one run.

    A join reads both streams and so sees the full per-query rate; a histogram reads one, and the
    1:3 person:auction mix splits that rate 1/4 : 3/4. Statistic queries alternate person/auction.
    """
    quarter = tuples_per_sec_per_query / 4.0
    person_stats = (num_statistic_queries + 1) // 2
    auction_stats = num_statistic_queries // 2
    return (tuples_per_sec_per_query * NUM_ANALYTICAL_QUERIES
            + person_stats * quarter
            + auction_stats * 3 * quarter)

# The container joins the host network so published-port NAT never becomes the throughput ceiling.
# On a host where that is unavailable, swap for -p 127.0.0.1:<port>:<port>.
GENERATOR_DOCKER_NETWORK = os.environ.get("GENERATOR_DOCKER_NETWORK", "host")
GENERATOR_READY_TIMEOUT_S = _int("GENERATOR_READY_TIMEOUT_S", 30)

# TCP source buffering. Under unlimited ingestion the buffer fills long before the interval elapses,
# so this only bounds how long a partially-filled buffer waits.
TCP_FLUSH_INTERVAL_MS = _int("TCP_FLUSH_INTERVAL_MS", 100)

# The topology declares NUM_ANALYTICAL_QUERIES + max(STATISTIC_QUERY_COUNTS) queries at once.
MAX_OPERATORS = _int("MAX_OPERATORS", 100_000)


## Fieldnames ##################################################################

FIELDNAMES = [
    'num_statistic_queries', 'run_idx', 'num_analytical_queries',
    'num_worker_threads', 'window_size_sec', 'memory_budget',
    'person_domain', 'events_per_sec', 'tuples_per_sec_per_query', 'mode',
    'offered_tps',  # the load the engine was asked to sustain, see config.offered_tps()
    'analytical_throughput_tps',         # sum over the analytical queries — the headline metric
    'analytical_throughput_median_tps',  # per-query median, for variance
    'num_analytical_measured',           # how many analytical queries actually reported throughput
    'statistic_throughput_tps',
    'num_statistic_measured',
    'issue',
]

PER_QUERY_FIELDNAMES = [
    'query_id', 'role', 'window_start_ms', 'window_start_ms_normalized', 'throughput_tps',
]
