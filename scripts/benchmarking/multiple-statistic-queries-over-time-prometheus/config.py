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
Configuration for run_prometheus_benchmark.py.

Each run in RUNS submits a new query every QUERY_LAUNCH_INTERVAL_SECONDS,
up to TOTAL_QUERIES_PER_RUN, then lets the queries run for
MEASUREMENT_WINDOW_SECONDS before stopping them and parsing logs.

The cadence knobs, RUNS, and BUILD_DIR can be overridden via environment
variables (same names) for smoke tests, e.g.:
  TOTAL_QUERIES_PER_RUN=2 MEASUREMENT_WINDOW_SECONDS=5 RUNS=statistic_build \
      python3 -m scripts.benchmarking.multiple-statistic-queries-over-time-prometheus.run_prometheus_benchmark
"""

import math
import os

### Submission cadence (applied identically to both runs)
QUERY_LAUNCH_INTERVAL_SECONDS = int(os.environ.get("QUERY_LAUNCH_INTERVAL_SECONDS", 3))     ### n: how often a new query is started
MEASUREMENT_WINDOW_SECONDS = int(os.environ.get("MEASUREMENT_WINDOW_SECONDS", 60))          ### time queries run after the last is submitted

### Number of queries each run submits. Edit NUM_QUERIES here to change it; the TOTAL_QUERIES_PER_RUN
### env var overrides this default at launch, and TOTAL_QUERIES_STATISTIC_BUILD / _PROMETHEUS_SINK
### override it per run (useful for smoke tests).
NUM_QUERIES = 100
_default_total = int(os.environ.get("TOTAL_QUERIES_PER_RUN", NUM_QUERIES))
TOTAL_QUERIES_PER_RUN = {
    "statistic_build": int(os.environ.get("TOTAL_QUERIES_STATISTIC_BUILD", _default_total)),
    "prometheus_sink": int(os.environ.get("TOTAL_QUERIES_PROMETHEUS_SINK", _default_total)),
}

### Worker configuration. NUMBER_OF_BUFFERS was 4_000_000 (~32 GB at 8KB/buffer) which
### exceeds any realistic container memory budget; 200_000 (~1.6 GB) keeps the buffer
### pool inside the sensor/edge memory budgets while still leaving headroom for actual work.
EXECUTION_MODE = "COMPILER"
JOIN_STRATEGY = "HASH_JOIN"
PAGE_SIZE = 8192
BUFFER_SIZE_BYTES = 8192
NUMBER_OF_BUFFERS = 200_000
FLUSH_INTERVAL_MS = 10

### CSV output. The latency listener emits one sample per completed task (per query); at multi-MTup/s
### that is tens of millions of rows, so the raw latency CSV can reach gigabytes. Each query's samples
### are uniformly strided down to at most LATENCY_MAX_SAMPLES_PER_QUERY rows in the written CSV. Uniform
### striding preserves the distribution and the time spread, so the CDF / p99 / p99-over-time plots stay
### representative. Set to 0 to write every sample (original behaviour).
LATENCY_MAX_SAMPLES_PER_QUERY = int(os.environ.get("LATENCY_MAX_SAMPLES_PER_QUERY", 50_000))

### Tuple production is done by a sidecar Rust producer (rust-tcp-generator/) running in its
### own container without resource caps. The worker dials 127.0.0.1:<TCP_PORT_BASE + query_index>
### via the TCPSource. See run_prometheus_benchmark.py / SingleNodeWorkerWithPrometheus.dockerfile.
TCP_GENERATOR_IMAGE = os.environ.get("TCP_GENERATOR_IMAGE", "nes-bench-tcp-gen:local")
TCP_PORT_BASE = int(os.environ.get("TCP_PORT_BASE", 9100))

### Statistic build run knobs
STATISTIC_QUERY_TEMPLATES = [
    "scripts/benchmarking/multiple-statistic-queries-over-time-prometheus/query-configs/statistic/reservoir_query.yaml.template",
    "scripts/benchmarking/multiple-statistic-queries-over-time-prometheus/query-configs/statistic/equi_width_histogram_query.yaml.template",
]
RESERVOIR_SIZE = 1000
EQUI_WIDTH_HISTOGRAM = {"num_buckets": 100, "min_value": 0, "max_value": 1_000_000, "counter_type": "uint64"}

### Prometheus sink run knobs
PROMETHEUS_VERSION = "2.53.5"
PROMETHEUS_LISTEN_PORT = 9090         ### Prometheus server HTTP port
PROMETHEUS_SCRAPE_INTERVAL_SECONDS = 1
SINK_PORT_BASE = 8800                 ### each query exposes /metrics on SINK_PORT_BASE + i

### Worker data plane port. Must NOT collide with PROMETHEUS_LISTEN_PORT (9090);
### the prometheus runner shares the host with the Prometheus subprocess.
WORKER_DATA_PORT = 9091
PROMETHEUS_QUERY_TEMPLATE = (
    "scripts/benchmarking/multiple-statistic-queries-over-time-prometheus/query-configs/prometheus/prometheus_sink_query.yaml.template"
)

### Build outputs to here so paths match start_single_node_worker
BUILD_DIR = os.environ.get("BUILD_DIR", "./build_dir")

### Combined-container deployment. Both nes-single-node-worker and prometheus run inside the same
### Docker container so they share a single cgroup (--cpus / --memory). Each condition gets its
### own (cpus, memory) budget; cloud has no caps. The three tiers map to a sensor→edge→cloud
### deployment story: a Pi-class device, a NUC/workstation, and a server-class VM.
WORKER_DOCKER_IMAGE = os.environ.get("WORKER_DOCKER_IMAGE", "nes-bench-prom-combined:local")
### Memory caps are OFF by default (None): the 8 KiB x NUMBER_OF_BUFFERS pool (~1.5 GiB) plus per-query
### state exceeds the tighter caps (notably sensor's 1 GiB) and OOM-kills the worker mid-run. Only the
### CPU caps define the tiers for now. Re-enable a memory cap per tier by setting EDGE_HEAVY_MEMORY /
### EDGE_LIGHT_MEMORY / SENSOR_MEMORY (e.g. "1g").
CONDITION_BUDGETS = {
    "cloud":      {"cpus": None,                                   "memory": None},
    "edge_heavy": {"cpus": os.environ.get("EDGE_HEAVY_CPUS", "8"), "memory": os.environ.get("EDGE_HEAVY_MEMORY")},
    "edge_light": {"cpus": os.environ.get("EDGE_LIGHT_CPUS", "4"), "memory": os.environ.get("EDGE_LIGHT_MEMORY")},
    "sensor":     {"cpus": os.environ.get("SENSOR_CPUS", "1"),     "memory": os.environ.get("SENSOR_MEMORY")},
}

### Worker threads track the tier's CPU budget so the engine isn't oversubscribed: a capped tier uses
### ceil(cpus), while the uncapped cloud tier uses CLOUD_WORKER_THREADS. Set WORKER_THREADS to pin a
### single count across every tier (useful for isolating thread-count effects in an A/B).
CLOUD_WORKER_THREADS = int(os.environ.get("CLOUD_WORKER_THREADS", 12))
_WORKER_THREADS_OVERRIDE = os.environ.get("WORKER_THREADS")


def worker_threads_for(condition):
    """Number of query-engine worker threads for a deployment tier (ceil(cpus), or CLOUD_WORKER_THREADS
    when the tier is uncapped). WORKER_THREADS, if set, forces this count for every tier."""
    if _WORKER_THREADS_OVERRIDE is not None:
        return max(1, int(_WORKER_THREADS_OVERRIDE))
    cpus = CONDITION_BUDGETS.get(condition, {}).get("cpus")
    if cpus is None:  ### uncapped (cloud)
        return CLOUD_WORKER_THREADS
    return max(1, math.ceil(float(cpus)))


### Which experiments to run; remove an entry to skip. Override via comma-separated env var, e.g. RUNS=statistic_build
RUNS = [r.strip() for r in os.environ.get("RUNS", "statistic_build,prometheus_sink").split(",") if r.strip()]
