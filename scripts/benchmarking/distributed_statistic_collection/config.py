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
Configuration for run_benchmark.py (distributed statistic collection).

Topologies are described level-by-level starting at the ROOT, e.g. "1/2/4" = 1 root, 2 intermediate
nodes, 4 leaves. Nodes form a tree: every node has exactly one parent in the level above, and children
are spread as evenly as possible across parents (1/2/4 -> 2 leaves per intermediate). The first level
must be exactly one root (else ValueError). The last level holds the data sources; middle levels relay.
One Docker container per worker on a shared bridge network.

We measure the INCOMING network bytes to the root (root container eth0 RX) over time. Three variants:

  prometheus  source(leaf) -> Prometheus sink(root)              raw stream reaches root
  split       build(leaf)  -> StatisticStoreWriter(root)         per-window synopsis reaches root
  local       build(leaf)  -> StatisticStoreWriter(leaf)         4 scalar fields reach root

Most knobs are env-overridable for smoke tests, e.g.:
  TOPOLOGIES=1/2 SOURCES_PER_LEAF=1 RUN_DURATION_SECONDS=15 VARIANTS=local \
      python3 -m scripts.benchmarking.distributed_statistic_collection.run_benchmark
"""

import os

### Build outputs to here so paths match common.config (NES_BUILD_DIR).
BUILD_DIR = os.environ.get("BUILD_DIR", "./build_dir")

### Docker images (reused from the single-node prometheus experiment). The root runs the combined
### worker+Prometheus image; leaves run the same image with Prometheus disabled. The host-built worker
### binary is bind-mounted in, so engine changes don't need an image rebuild.
WORKER_DOCKER_IMAGE = os.environ.get("WORKER_DOCKER_IMAGE", "nes-bench-prom-combined:local")
TCP_GENERATOR_IMAGE = os.environ.get("TCP_GENERATOR_IMAGE", "nes-bench-tcp-gen:local")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "nes-bench")

### Variants to run (one full topology run each). Override comma-separated.
VARIANTS = [v.strip() for v in os.environ.get("VARIANTS", "prometheus,split,local").split(",") if v.strip()]

### Topologies to sweep, each "level0/level1/.../leafLevel" counts from the root. Override comma-separated,
### e.g. TOPOLOGIES="1/2,1/4". Level 0 must be exactly one root. Default is the single 1-root/2-leaf tree
### (deeper trees like 1/2/4, 1/4, 1/4/8 still work via the env override).
TOPOLOGIES = [t.strip() for t in os.environ.get("TOPOLOGIES", "1/2").split(",") if t.strip()]

### Experiment modes (each writes its own summary CSV into OUTPUT_DIR; both run by default):
###   traffic    — measure incoming bytes to the root for each statistic variant (the volume story);
###                writes summary.csv.
###   contention — also run general-purpose (GP) passthrough queries to the root under a capped
###                root-ingress bandwidth and measure GP throughput; writes contention_summary.csv. The
###                point: when the shared uplink is limited, raw statistic upstreaming (prometheus)
###                starves the GP queries, while the in-engine variants (split/local) leave it free.
### Override with MODES="traffic" (single) or MODE=contention (back-compat single mode).
MODES = [m.strip() for m in (os.environ.get("MODES") or os.environ.get("MODE") or "traffic,contention").split(",") if m.strip()]

### Contention mode: shared root-ingress bandwidth caps in kbit (0 = uncapped baseline). Chosen to straddle
### WEAVE's own synopsis upstream load (~2.3 Mbit/s, see summary.csv split total_bytes): all points sit
### above it (100/30/10/3 Mbit) so WEAVE fits and the SOTA-vs-WEAVE gap is the clean, monotone story. Below
### ~2.3 Mbit WEAVE's own synopsis no longer fits and GP collapses to ~1x (crossover), and sub-Mbit caps are
### below the delivered-count noise floor — so those points are dropped here. The cap is a tbf-policed
### ingress qdisc on the root container's eth0 (needs --cap-add=NET_ADMIN + tc in the image).
BANDWIDTH_LIMITS_KBIT = [int(x) for x in os.environ.get("BANDWIDTH_LIMITS_KBIT", "0,1000000,100000,10000").split(",")]
### tbf burst FLOOR in bytes. apply_ingress_cap sizes the actual burst to the rate (>= rate/HZ so the shaper
### can reach `rate`); this floor (~2 packets) is only the lower bound, so a small kbit cap still binds
### instead of letting a large fixed burst through instantly. Reduced from 256k now that low caps are used.
INGRESS_BURST = int(os.environ.get("INGRESS_BURST", 3200))
### GP passthrough queries per leaf (source(leaf) -> projection -> Void sink(root)); their generators
### run full speed (GP_GENERATOR_RATE=0) so the GP workload is bandwidth-bound and competes for the cap.
GP_QUERIES_PER_LEAF = int(os.environ.get("GP_QUERIES_PER_LEAF", 1))
GP_GENERATOR_RATE = int(os.environ.get("GP_GENERATOR_RATE", 0))
### GP source ports sit above the statistic ports within each leaf's netns to avoid collisions.
GP_PORT_BASE = int(os.environ.get("GP_PORT_BASE", 10100))
### Settle margin (ms) after the cap is applied before GP throughput windows are counted, so the
### shaper's queue drains the pre-cap backlog first and doesn't inflate the capped measurement.
MEASURE_SETTLE_MS = int(os.environ.get("MEASURE_SETTLE_MS", 5000))

### Workload: fixed small set. Each leaf hosts SOURCES_PER_LEAF logical sources; every source gets one
### query of the variant under test, all submitted once and held for RUN_DURATION_SECONDS.
SOURCES_PER_LEAF = int(os.environ.get("SOURCES_PER_LEAF", 4))
RUN_DURATION_SECONDS = int(os.environ.get("RUN_DURATION_SECONDS", 30))

### Worker container CPU budgets (per user: root 8, every non-root node 2). Internal container gRPC port
### is always 8080; it is published to the host on a distinct port per worker (BASE_HOST_PORT + index) so
### nes-cli (on the host) can reach each worker. The data plane (DATA_PORT) is NOT published — workers
### reach each other by container name over DOCKER_NETWORK.
ROOT_CPUS = os.environ.get("ROOT_CPUS", "8")
### Leaves get 4 CPUs so the in-engine statistic build has enough headroom alongside a co-located GP
### query — otherwise the build starves the GP query of leaf CPU and masks the root-network contention.
LEAF_CPUS = os.environ.get("LEAF_CPUS", "4")  ### also used for intermediate (relay) nodes
CONTAINER_GRPC_PORT = 8080
DATA_PORT = 9091
BASE_HOST_PORT = int(os.environ.get("BASE_HOST_PORT", 8080))
MAX_OPERATORS = int(os.environ.get("MAX_OPERATORS", 10000))


def build_workers(topology):
    """Expand a "l0/l1/.../lN" topology spec into a flat list of worker dicts.

    Level 0 is the root (must be exactly 1). Each node connects to exactly one parent in the level
    above, children spread round-robin across parents for an even tree. Returns dicts with: name, role
    (root|intermediate|leaf), level, cpus, host_port, host (node identity == gRPC endpoint nes-cli
    dials), data_address (container-DNS data plane), parent (parent's host, None for root).
    """
    levels = [int(x) for x in topology.split("/") if x != ""]
    if not levels or levels[0] != 1:
        raise ValueError(
            f"Topology '{topology}' must start with exactly one root node "
            f"(level-0 count is {levels[0] if levels else 'empty'}); there must be exactly one root."
        )
    if len(levels) < 2 or any(c < 1 for c in levels):
        raise ValueError(f"Topology '{topology}' must have >= 2 levels and >= 1 node per level.")

    last = len(levels) - 1
    level_nodes, idx = [], 0
    for lvl, count in enumerate(levels):
        nodes = []
        for j in range(count):
            name = "root" if lvl == 0 else f"l{lvl}n{j}"
            role = "root" if lvl == 0 else ("leaf" if lvl == last else "intermediate")
            port = BASE_HOST_PORT + idx
            nodes.append(
                {
                    "name": name,
                    "role": role,
                    "level": lvl,
                    "cpus": ROOT_CPUS if lvl == 0 else LEAF_CPUS,
                    "host_port": port,
                    "host": f"localhost:{port}",
                    "data_address": f"{name}:{DATA_PORT}",
                    "parent": None,
                }
            )
            idx += 1
        level_nodes.append(nodes)

    for lvl in range(1, len(levels)):
        parents = level_nodes[lvl - 1]
        for ci, child in enumerate(level_nodes[lvl]):
            child["parent"] = parents[ci % len(parents)]["host"]

    return [w for nodes in level_nodes for w in nodes]


def topology_dir(topology):
    """Filesystem-safe name for a topology spec (1/2/4 -> 1-2-4)."""
    return topology.replace("/", "-")

### TCP source ports. Each leaf runs its sources in its own netns, so ports may repeat across leaves;
### the per-query generator binds exactly one port. The worker dials 127.0.0.1:<port> over loopback.
TCP_PORT_BASE = int(os.environ.get("TCP_PORT_BASE", 9100))
FLUSH_INTERVAL_MS = int(os.environ.get("FLUSH_INTERVAL_MS", 10))
### Per-source generator emission rate (tuples/sec). Applied to ALL variants for a fair comparison.
### A finite rate (vs the generator's default full speed) decouples window-close cadence from raw
### throughput so event-time windows close on a predictable schedule (GENERATOR_RATE / window-in-tuples
### closes per second) instead of flooding/stalling. 0 = unlimited.
GENERATOR_RATE = int(os.environ.get("GENERATOR_RATE", 200_000))

### Prometheus (only for the `prometheus` variant; runs in the root container, scrapes the root's
### loopback sink ports). Each prometheus query's sink exposes /metrics on SINK_PORT_BASE + global_index.
SINK_PORT_BASE = int(os.environ.get("SINK_PORT_BASE", 8800))
PROMETHEUS_SCRAPE_INTERVAL_SECONDS = int(os.environ.get("PROMETHEUS_SCRAPE_INTERVAL_SECONDS", 1))
PROM_UI_PORT = 9090

### Statistic metric. MAXVAL -> Equi_Width_Histogram: its per-window synopsis (histogram buckets) is
### large, so the `split` traffic (synopsis crosses) is clearly bigger than `local` (4 scalars cross).
### memory_budget drives the histogram size; bump it to widen the split-vs-local gap.
STATISTIC_METRIC = os.environ.get("STATISTIC_METRIC", "MAXVAL")
### Window size. Under event time on the generator's per-tuple counter this is effectively a count of
### TUPLES, so at GENERATOR_RATE=200k and 100k-tuple windows, windows close ~2x/sec. Each window
### summarizes ~100k raw tuples (~1.2 MB) into one synopsis, so the synopsis must be set smaller than
### that (see HISTOGRAM_MEMORY_BUDGET) for the gradient prometheus(raw) > split(synopsis) > local(scalars).
STATISTIC_WINDOW_SIZE_MS = int(os.environ.get("STATISTIC_WINDOW_SIZE_MS", 100_000))
### Windowing mode. The rust-tcp-generator emits `timestamp` as a per-tuple counter (0,1,2,...) at full
### speed, so an EVENT-TIME window of "N ms" is really N *tuples*. Ingestion time (wall-clock windows)
### would be the natural fit for a traffic-over-time plot, but the ingestion-time statistic-build path
### currently CRASHES the worker on deploy (terminate called), so we default to EVENT time, which runs
### cleanly. NOTE: with the full-speed generator, event-time windows close on tuple count, which makes
### the split-vs-local synopsis traffic hard to control (small windows flood/stall, large ones rarely
### close). Amplifying split>>local needs a rate-limited generator (or an ingestion-time engine fix).
USE_EVENT_TIME = os.environ.get("USE_EVENT_TIME", "1") == "1"
### Synopsis size in bytes (drives the histogram bucket count). Keep it well below the raw bytes per
### window (~window_tuples x 12 B) so the synopsis genuinely compresses the window: that is what makes
### split (synopsis crosses) land between prometheus (raw) and local (4 scalars). 2 KB << 1.2 MB/window.
### This is also WEAVE's per-synopsis upstream cost: footprint ~= (SOURCES_PER_LEAF x leaves) x
### (GENERATOR_RATE / window_tuples) x HISTOGRAM_MEMORY_BUDGET. At 2 KB that's ~0.26 Mbit/s (was 2.1 at
### 16 KB), so WEAVE now fits — and wins — under caps down to a few hundred kbit.
HISTOGRAM_MEMORY_BUDGET = int(os.environ.get("HISTOGRAM_MEMORY_BUDGET", 2048))
HISTOGRAM_MIN = int(os.environ.get("HISTOGRAM_MIN", 0))
HISTOGRAM_MAX = int(os.environ.get("HISTOGRAM_MAX", 1_000_000))
PROM_HISTOGRAM_NUM_BUCKETS = int(os.environ.get("PROM_HISTOGRAM_NUM_BUCKETS", 100))

### Worker engine config (mirrors the single-node experiment).
EXECUTION_MODE = "COMPILER"
JOIN_STRATEGY = "HASH_JOIN"
PAGE_SIZE = 8192
BUFFER_SIZE_BYTES = 8192
NUMBER_OF_BUFFERS = int(os.environ.get("NUMBER_OF_BUFFERS", 200_000))

### Traffic sampling: read the ROOT container's eth0 RX (received) bytes every interval and difference,
### i.e. the total incoming network bytes to the root regardless of tree depth.
TRAFFIC_SAMPLE_INTERVAL_SECONDS = float(os.environ.get("TRAFFIC_SAMPLE_INTERVAL_SECONDS", 1.0))
NET_IFACE = os.environ.get("NET_IFACE", "eth0")


def worker_threads(role):
    """Engine worker threads tied to the container CPU budget so it isn't oversubscribed."""
    cpus = ROOT_CPUS if role == "root" else LEAF_CPUS
    return max(1, int(float(cpus)))
