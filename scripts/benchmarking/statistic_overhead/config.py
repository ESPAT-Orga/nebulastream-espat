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

The experiment holds a fixed analytical workload (NUM_ANALYTICAL_QUERIES concurrent ClusterMonitoring
Q2 sliding-window aggregations) and sweeps how many of them carry a spliced equi-width histogram over
their GROUP BY key `jobId` — the input an optimizer needs for group-cardinality estimation,
aggregation hash-table sizing and skew detection.

One histogram per analytical query, so STATISTIC_QUERY_COUNTS runs 0..NUM_ANALYTICAL_QUERIES. The
histogram is a *companion*: it splices onto the query's own source and adds no ingestion, so the
offered load is identical at every point of the sweep. That is what makes a throughput change
attributable to the synopsis rather than to extra input.

Every constant is overridable from the environment so the shell wrapper can drive smoke runs.

Sections (search by ``##``):
  ## General    — paths, cmake flags, repetition count
  ## Workload   — the sweep itself
  ## Worker     — single-node-worker startup parameters
  ## Generator  — ClusterMonitoring TCP producer
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
def cmake_flags():
    """Lazy, because get_vcpkg_dir() raises on any host it does not know about — evaluating this at
    import time would make the module unimportable there, including for --skip-build runs."""
    return ("-G Ninja "
            "-DCMAKE_BUILD_TYPE=Release "
            f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
            "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
            # No large test data: this benchmark's input comes from the TCP generator, so enabling
            # them would only download multi-GB CSVs nothing here reads.
            "-DENABLE_LARGE_TESTS=0 "
            "-DNES_BUILD_NATIVE:BOOL=ON "
            "-DNES_LOG_LEVEL:STRING=LEVEL_NONE")

NUM_RUNS = _int("NUM_RUNS", 5)
RESULTS_CSV = "results_statistic_overhead.csv"


## Workload ####################################################################

# Fixed analytical workload: N concurrent ClusterMonitoring Q2 aggregations.
NUM_ANALYTICAL_QUERIES = _int("NUM_ANALYTICAL_QUERIES", 10)

# The swept axis: how many of those queries carry a histogram. 0 is the baseline arm.
STATISTIC_QUERY_COUNTS = _int_list("STATISTIC_QUERY_COUNTS", list(range(0, 11)))

# ClusterMonitoring Q2's window: SLIDING(creationTS, SIZE 60 SEC, ADVANCE BY 1 SEC). Sixty windows
# are open at once, but Q2 aggregates, so the state is per GROUP rather than per tuple —
# ~60 * JOB_DOMAIN * 24 B per query. JOB_DOMAIN is the knob if that gets tight.
WINDOW_SIZE_SEC = _int("WINDOW_SIZE_SEC", 60)
WINDOW_ADVANCE_SEC = _int("WINDOW_ADVANCE_SEC", 1)

# The companion histogram's own window. It need not match the analytical query's — a tumbling window
# keeps the synopsis cheap and the store bounded.
COMPANION_WINDOW_SIZE_SEC = _int("COMPANION_WINDOW_SIZE_SEC", 10)

# Seconds of steady state to measure, and how much of the run to discard first. Throughput windows
# whose start falls inside the warm-up are dropped, so query start-up and compilation don't count.
WARMUP_SECONDS = _int("WARMUP_SECONDS", 10)
MEASUREMENT_WINDOW_SECONDS = _int("MEASUREMENT_WINDOW_SECONDS", 30)

# Seconds between starting the worker and submitting the topology.
WAIT_AFTER_WORKER_START = WAIT_BETWEEN_COMMANDS_LONG


## Isolated grid ###############################################################
#
# A second experiment: k analytical queries and j statistic queries, all INDEPENDENT — each with its
# own TCP source. No companions, so j is not capped by k, and what is measured is the cost of
# monitoring queries *including their ingestion*. That is a different claim from the shared sweep's
# marginal-synopsis cost; the two belong side by side, since the gap between them is what in-engine
# sharing buys.
#
# Deliberately run BELOW saturation. A saturated system invites the objection that we are measuring
# how capacity gets divided rather than what statistics cost; at a fixed sustainable rate the
# question is sharp — does monitoring disturb a workload that was comfortably keeping up?
GRID_ANALYTICAL_COUNTS = _int_list("GRID_ANALYTICAL_COUNTS", [1, 5, 10])
GRID_STATISTIC_COUNTS = _int_list("GRID_STATISTIC_COUNTS", [0, 10, 2, 50, 60, 70, 80, 90, 100, 150, 200])

# Against the ~22.5 Mtup/s ceiling this puts the whole grid under a third of capacity
# (k=10,j=0 -> 4%; k=10,j=50 -> 27%), so the surface will very likely be flat: "50 statistic queries
# do not disturb 10 analytical ones". That is a real result, but it invites the mirror objection
# that 73% headroom explains it. Raise this to ~350_000 to span 1.5%..93% and show where the free
# lunch actually ends. Always report the load as a fraction of capacity alongside the figure.
#
# Aggregate offered load at the top corner (k=10), against the measured 28.8 Mtup/s ceiling for this
# mix. Read this before changing either knob: j=100 at 350k is ABOVE capacity, so a throughput drop
# there is ingestion saturating, not what a statistic query costs.
#
#   j =            0        10        25        50       100
#   100k/query   1.0M/3%   2.0M/7%  3.5M/12%  6.0M/21%  11.0M/38%   <- sub-saturation throughout
#   350k/query   3.5M/12%  7.0M/24% 12.3M/43% 21.0M/73%  38.5M/134% <- j=100 is over the ceiling
#
# A LIST: the grid sweeps every rate, so one run produces both the sub-saturation arm and the
# over-capacity arm and they are directly comparable (same session, same machine state). Override
# with a comma-separated env var, e.g. GRID_TUPLES_PER_SEC_PER_QUERY=100000,350000.
# GRID_TUPLES_PER_SEC_PER_QUERY = _int_list("GRID_TUPLES_PER_SEC_PER_QUERY", [100_000, 350_000])
GRID_TUPLES_PER_SEC_PER_QUERY = _int_list("GRID_TUPLES_PER_SEC_PER_QUERY", [200_000])

# EQUIWIDTHHISTOGRAM's third argument is a memory budget in BYTES, not a bucket count:
#   numBuckets = max(1, (budget - 8) / 24)   [EquiWidthHistogramLogicalFunction.cpp]
# 10 KiB therefore buys ~426 bins over JOB_DOMAIN.
MEMORY_BUDGET = _int("MEMORY_BUDGET", 10 * 1024)

# Statistic ids must be unique across every query alive in one run.
STATISTIC_ID_BASE = _int("STATISTIC_ID_BASE", 2000)

# The statistic query's own tumbling window. Independent of the analytical query's sliding window.
GRID_STATISTIC_WINDOW_SEC = _int("GRID_STATISTIC_WINDOW_SEC", 10)


def grid_offered_tps(tuples_per_sec_per_query, num_analytical, num_statistic):
    """Total tuples/sec offered across every connection of one grid point.

    Unlike the shared experiment, statistic queries DO add ingestion here: each owns a source and
    pulls its own copy of the stream. That is the whole difference being measured.
    """
    return tuples_per_sec_per_query * (num_analytical + num_statistic)


def grid_analytical_offered_tps(tuples_per_sec_per_query, num_analytical):
    """The analytical share only — the denominator for "did the workload keep up?"."""
    return tuples_per_sec_per_query * num_analytical


GRID_RESULTS_CSV = "results_statistic_grid.csv"

GRID_FIELDNAMES = [
    'num_analytical_queries', 'num_statistic_queries', 'run_idx',
    'num_worker_threads', 'window_size_sec', 'window_advance_sec',
    'statistic_window_sec', 'job_domain', 'events_per_sec', 'memory_budget',
    'tuples_per_sec_per_query', 'offered_tps', 'analytical_offered_tps',
    'analytical_throughput_tps',         # sum over the analytical queries (diagnostic)
    'analytical_throughput_median_tps',  # per-query median — the headline scales this by k
    'num_analytical_measured',
    'statistic_throughput_tps',          # real data here: each statistic query owns a source
    'num_statistic_measured',
    'issue',
]


## Worker ######################################################################

WORKER_THREADS = _int("WORKER_THREADS", 16)
EXECUTION_MODE = os.environ.get("EXECUTION_MODE", "COMPILER")
JOIN_STRATEGY = os.environ.get("JOIN_STRATEGY", "HASH_JOIN")
PAGE_SIZE = _int("PAGE_SIZE", 8192)

# Many small buffers, NOT few large ones. The binding resource for concurrent windowed joins is the
# buffer COUNT, not the pool's size in bytes: a join takes a buffer per unit of state however full it
# gets, so a large operator_buffer_size just wastes most of each allocation. Measured on the earlier
# Nexmark Q8 workload (10 concurrent joins),
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


## Companion statistics ########################################################

# nes-repl companion flags. MinVal/MaxVal/Selectivity all map to Equi_Width_Histogram
# (DefaultStatisticQueryGenerator::toStatisticType); the histogram bounds options are documented
# against MinVal, so that is what we ask for.
COMPANION_METRIC = os.environ.get("COMPANION_METRIC", "MinVal")
# Resolved inside the build branch, whose plan holds exactly one source, so the unqualified name is
# unambiguous there. Uppercase because the build chain does not normalise the event-time field the
# way it does the aggregation field.
COMPANION_EVENT_TIME_FIELD = os.environ.get("COMPANION_EVENT_TIME_FIELD", "CREATIONTS")

# Bin-count threshold for the companion's gated probe. Deliberately unreachable: a histogram bin
# never holds 1e12 tuples, so the trigger never fires and no workload switch ever happens. See
# _companion_args in shared_submission.py for why the switch machinery is engaged at all.
COMPANION_NEVER_FIRE_THRESHOLD = _int("COMPANION_NEVER_FIRE_THRESHOLD", 1_000_000_000_000)


## Generator ###################################################################

# Producer of the real ClusterMonitoring schema, as opposed to the synthetic 3-field stream from
# rust-tcp-generator.
GENERATOR_IMAGE = os.environ.get("GENERATOR_IMAGE", "nes-bench-gen:local")
GENERATOR_CONTAINER = os.environ.get("GENERATOR_CONTAINER", "nes-bench-gen")
GENERATOR_BUILD_CONTEXT = "rust-benchmark-generator"

# --streams cluster binds a single port at port_base+0.
GENERATOR_PORT_BASE = _int("GENERATOR_PORT_BASE", 9200)
CLUSTER_PORT = GENERATOR_PORT_BASE
GENERATOR_SEED = _int("GENERATOR_SEED", 42)

# Virtual clock: events per second of EVENT time. With one tuple per event for the cluster stream,
# a window of WINDOW_SIZE_SEC holds EVENTS_PER_SEC * WINDOW_SIZE_SEC tuples of event time.
EVENTS_PER_SEC = _int("EVENTS_PER_SEC", 100_000)

# Grouping-key domain: jobId cycles through [0, JOB_DOMAIN), which is also the histogram's
# max_value and the number of groups the aggregation tracks per open window.
JOB_DOMAIN = _int("JOB_DOMAIN", 10_000)

# Offered load, in tuples/sec delivered to ONE query reading both streams (person + auction).
# 0 = unlimited. The generator converts this to an event-time replay speed internally and paces every
# stream against that one clock; see rust-benchmark-generator/src/main.rs.
#
# Two measured failures forced this to be an event-time knob rather than a tuples/sec one:
#
#  1. Unlimited: NebulaStream's buffer pool is global with no per-query fairness, so the first
#     query's sources drain it. With 10 concurrent Q8 joins exactly one query stayed healthy
#     (314 throughput windows) while the other nine got 1-8 each, and BUFFER_EXHAUSTION appeared
#     one line after the first throughput line.
# Pacing is therefore on event time, and the offered load is fixed; the experiment reports sustained
# throughput against it. Pick the operating point at the calibration knee via --tuples-per-sec at
# N=0 — a flat overhead curve measured while the engine idles proves nothing.
#
# Measured on amd7950x3d (10 ClusterMonitoring Q2 queries, 16 worker threads, 8 KiB x 1M buffers):
# ~2.17 Mtup/s aggregate, i.e. ~217k per query. 200_000 offers 2.00 Mtup/s and sustains 100% of it,
# which is 92% of the ceiling — loaded, with enough headroom not to sit on the knee.
# Calibrated on amd7950x3d, 10 ClusterMonitoring Q2 queries, 16 worker threads:
#   1.0 M/query -> 10 M offered, 100.0% sustained
#   1.5 M/query -> 15 M offered, 100.0% sustained
#   2.0 M/query -> 20 M offered, 100.0% sustained   <- operating point, 89% of ceiling
#   2.3 M/query -> 23 M offered,  97.7% sustained   <- knee
# Unpaced ceiling ~22.5 Mtup/s. Running far below this is the "flat because idling" trap: an
# overhead curve measured at 200k/query would look flat no matter what the synopsis costs.
TUPLES_PER_SEC_PER_QUERY = _int("TUPLES_PER_SEC_PER_QUERY", 2_000_000)
TUPLES_PER_SEC_LIST = _int_list("TUPLES_PER_SEC_LIST", [TUPLES_PER_SEC_PER_QUERY])


def offered_tps(tuples_per_sec_per_query, num_statistic_queries):
    """Tuples/sec the generator offers across every connection of one run.

    Deliberately independent of num_statistic_queries: the histograms are spliced onto their query's
    existing source, so they open no connection and pull no extra tuples. Identical offered load at
    every point of the sweep is what makes a throughput difference attributable to the synopsis.
    """
    del num_statistic_queries  # by design: companions add no ingestion
    return tuples_per_sec_per_query * NUM_ANALYTICAL_QUERIES

# The container joins the host network so published-port NAT never becomes the throughput ceiling.
# On a host where that is unavailable, swap for -p 127.0.0.1:<port>:<port>.
GENERATOR_DOCKER_NETWORK = os.environ.get("GENERATOR_DOCKER_NETWORK", "host")
GENERATOR_READY_TIMEOUT_S = _int("GENERATOR_READY_TIMEOUT_S", 30)

# TCPSource's CONNECT_TIMEOUT. Note this is NOT only the connect deadline: TCPSource.cpp sets it as
# SO_RCVTIMEO/SO_SNDTIMEO too, so it is also the receive timeout for the socket's entire lifetime —
# any gap longer than this in incoming data kills the source. A companion's data source can sit
# deferred waiting for its build branch to splice in, so a short value here is a hazard.
TCP_CONNECT_TIMEOUT_SECONDS = _int("TCP_CONNECT_TIMEOUT_SECONDS", 300)

# TCP source buffering. Under unlimited ingestion the buffer fills long before the interval elapses,
# so this only bounds how long a partially-filled buffer waits.
TCP_FLUSH_INTERVAL_MS = _int("TCP_FLUSH_INTERVAL_MS", 100)

# The topology declares NUM_ANALYTICAL_QUERIES + max(STATISTIC_QUERY_COUNTS) queries at once.
MAX_OPERATORS = _int("MAX_OPERATORS", 100_000)


## Fieldnames ##################################################################

FIELDNAMES = [
    'num_statistic_queries', 'run_idx', 'num_analytical_queries',
    'num_worker_threads', 'window_size_sec', 'window_advance_sec',
    'job_domain', 'events_per_sec', 'tuples_per_sec_per_query',
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
