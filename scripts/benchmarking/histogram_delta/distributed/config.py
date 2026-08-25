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
Configuration for run_benchmark.py (distributed histogram delta compression).

Forked from scripts/benchmarking/distributed_statistic_collection, which stays on its original
synthetic-generator defaults so its published numbers remain reproducible. This copy adds the
`delta`/`*_zstd` variants, the real-trace replay dataset, and the throughput mode, and changes the
defaults accordingly (DATASET, VARIANTS, STATISTIC_WINDOW_SIZE_MS, HISTOGRAM_MIN/MAX). Setting
DATASET=synthetic VARIANTS=prometheus,split,local reproduces the parent suite's configuration.

NOTE the two suites' wire-byte numbers are NOT comparable across the NetworkSink child-buffer fix
(commit 621d925867 "send child buffers at their used size, not their capacity"): before it, every
variable-sized value carried up to operator_buffer_size of padding on the wire.

Topologies are described level-by-level starting at the ROOT, e.g. "1/2/4" = 1 root, 2 intermediate
nodes, 4 leaves. Nodes form a tree: every node has exactly one parent in the level above, and children
are spread as evenly as possible across parents (1/2/4 -> 2 leaves per intermediate). The first level
must be exactly one root (else ValueError). The last level holds the data sources; middle levels relay.
One Docker container per worker on a shared bridge network.

We measure the INCOMING network bytes to the root (root container eth0 RX) over time. Six variants:

  prometheus  source(leaf) -> Prometheus sink(root)              raw stream reaches root
  split       build(leaf)  -> StatisticStoreWriter(root)         per-window synopsis reaches root
  split_zstd  as split, blob zstd'd across the cut               compressed synopsis reaches root
  delta       GEN(leaf)    -> RESOLVER+Writer(root)              per-window DELTA reaches root
  delta_zstd  as delta, blob zstd'd across the cut               compressed delta reaches root
  local       build(leaf)  -> StatisticStoreWriter(leaf)         4 scalar fields reach root

`delta` is `split` with histogram delta compression on, so the two are the directly comparable
pair: identical topology, identical windows, identical synopsis size -- only the payload
representation differs. Per window `split` produces a full synopsis (8 + bins*24 bytes) while
`delta` produces a sparse blob (24 + changedBins*16), except on keyframe windows (every
HISTOGRAM_DELTA_KEYFRAME_INTERVAL-th) which carry every bin.

MEASURED on the REAL trace (DATASET=cluster_monitoring, 682 bins over taskId's true [0,20009] range,
60 s event-time windows, N=10, topology 1/1, 60 s hold, buffer 8192). All six variants ingested
11.96-12.00 M tuples (spread 0.31 %) over ~2950 windows, so the byte counts are directly comparable:

  variant      root RX     B/window   vs split
  prometheus   115.28 MB          -   2.1x MORE   (the raw stream: cost tracks tuples, not windows)
  split         55.03 MB     18,619   1.00x       (16,376 B synopsis + record + framing)
  split_zstd    27.91 MB      9,442   1.97x
  delta         12.99 MB      4,395   4.24x       (~200 of 682 bins change per window)
  delta_zstd     6.74 MB      2,281   8.16x       (the two compose: delta then zstd)
  local          0.71 MB        240   77.4x       (no variable-sized field crosses at all)

Two results worth keeping. Delta beats generic compression of the same synopsis by 2.1x at this bin
count, as predicted (delta is O(changed bins), compression O(bins)). And the two COMPOSE, with
delta_zstd the best statistic variant measured: compression is half the win for none of the delta
machinery, and it still pays on top -- ~4 kB of structured binIndex/counter pairs compresses ~1.9x.

GOTCHA: the query MUST pass `min`/`max`. The request generator defaults the histogram range to
[0,1000] and EquiWidthHistogramPhysicalFunction clamps out-of-range values into the UPPERMOST bin, so
a range that does not cover the data puts nearly every tuple in one bin. The synopsis stays full size
while the delta collapses to a single changed bin, and the run reports a spectacular ratio that
measures only the misconfiguration. See HISTOGRAM_MIN/HISTOGRAM_MAX below.

SYMPTOM to watch for at high bin counts: if a run reports near-zero bytes for delta, check that the
leaf's generator logged an `accept` line. Near-zero means the leaf never started, usually because the
query did not finish compiling inside the hold.

Reading the traffic-over-time CSVs on real data: the curve is NOT flat, and that is the trace, not
the engine. Statistic traffic is proportional to windows CLOSED per second = ingest rate / rows per
window, and the trace's density varies (~1000 rows per 60 s window early, ~3250 late), so the
statistic variants dip in the middle of the run and recover. `prometheus`, whose cost tracks tuples
rather than windows, stays flat across the same stretch -- which is a good check that the dip is the
data and not a stall.

Most knobs are env-overridable for smoke tests, e.g.:
  TOPOLOGIES=1/2 SOURCES_PER_LEAF=1 RUN_DURATION_SECONDS=15 VARIANTS=local \
      python3 -m scripts.benchmarking.histogram_delta.distributed.run_benchmark
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
VARIANTS = [v.strip() for v in os.environ.get("VARIANTS", "prometheus,split,delta,local").split(",") if v.strip()]

### Keyframe interval for the `delta` variant: every Nth window ships a full synopsis so the RESOLVER has
### a baseline, the rest ship sparse deltas against it. Passed to EVERY worker, which matters: the option
### is consumed during lowering and resolved per worker, so GEN and RESOLVER must be given the same value
### or they group windows into different intervals. 1 = every window full (no compression).
HISTOGRAM_DELTA_KEYFRAME_INTERVAL = int(os.environ.get("HISTOGRAM_DELTA_KEYFRAME_INTERVAL", 10))

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

### Contention mode: shared root-ingress bandwidth caps in kbit (0 = uncapped baseline). Points sweep
### 1000/100/10/1 Mbit, all above WEAVE's own synopsis upstream load (~0.14 Mbit/s at the 1 KiB budget;
### the 2.3 Mbit/s in summary.csv split total_bytes was measured at 16 KB and scales /16 with the budget),
### so WEAVE fits at every cap and the SOTA-vs-WEAVE gap stays the clean, monotone story. 1 Mbit is the
### tightest cap in the sweep — sub-Mbit caps sit near the delivered-count noise floor, so they're left
### out. The cap is a tbf-policed ingress qdisc on the root container's eth0 (needs --cap-add=NET_ADMIN
### + tc in the image).
BANDWIDTH_LIMITS_KBIT = [int(x) for x in os.environ.get("BANDWIDTH_LIMITS_KBIT", "0,1000000,100000,10000,1000").split(",")]
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

### --- Dataset ------------------------------------------------------------------------------------
### Which data the leaf generators emit. `cluster_monitoring` (the default) replays the REAL 1 GB
### Google-cluster-monitoring trace -- the same `ENABLE_LARGE_TESTS` dataset the systests and the
### single-node histogram-delta throughput benchmark use, so the wire numbers here and the throughput
### numbers there describe the same workload. `synthetic` is the original uniform generator, kept as a
### control: it is the distribution the delta compression looks best on (uniform draws touch every bin
### evenly), so a real-trace number that holds up is the meaningful one.
###
### The trace is projected to `value,timestamp` by histogram_delta/prepare_cluster_monitoring.py:
###   value     = taskId     (range 0..20009; the raw userId is anonymised to a constant and would
###                           collapse the histogram to one bucket)
###   timestamp = creationTS (the real event time in ms, raw epoch, ~4.3 days over 18.65 M rows)
### The generator replays it in file order and never loops (a wrapped timestamp would move event time
### backwards); `id` stays synthetic since no statistic query reads it.
DATASET = os.environ.get("DATASET", "cluster_monitoring")

### Per-dataset defaults. `value_min`/`value_max` MUST bracket the actual value range: an equi-width
### histogram spreads its bins over exactly this interval, so leaving the synthetic [0,1000000) on a
### trace whose values stop at 20009 would park all the data in the first 2 % of the bins and flatter
### the delta (almost no bin ever changes). `window_ms` is the tumbling window: real ms for the trace,
### but a TUPLE COUNT for the synthetic generator, whose `timestamp` is a per-tuple counter.
DATASET_DEFAULTS = {
    "synthetic": {"value_min": 0, "value_max": 1_000_000, "window_ms": 100_000},
    ### 60 s windows hold ~3.0 k tuples each (18.65 M rows over ~6.2 k windows); the 1 s windows of
    ### this trace are far sparser (~29 k of them empty).
    "cluster_monitoring": {"value_min": 0, "value_max": 20_009, "window_ms": 60_000},
}
if DATASET not in DATASET_DEFAULTS:
    raise ValueError(f"unknown DATASET '{DATASET}'; known: {sorted(DATASET_DEFAULTS)}")
_DS = DATASET_DEFAULTS[DATASET]

### How the statistic sources get their data:
###   tcp    (default) a rust-tcp-generator sidecar in the leaf's netns streams CSV over loopback, so
###          every tuple is transported and parsed from text on the query's hot path.
###   memory the worker's own Memory source: MemorySource::setup() runs parseCsvFileIntoBuffers() over
###          the same CSV BEFORE the query starts and then hands out native TupleBuffers zero-copy, so
###          transport and parsing leave the measurement entirely.
###
### `memory` is for THROUGHPUT runs only, and forces one: the source has no rate limiter (GENERATOR_RATE
### is ignored), so it cannot equalise ingest across variants the way the wire-bytes run needs. It also
### holds the whole parsed dataset in the buffer pool at once -- ~24 B/tuple for the 3-field schema, so
### ~450 MB for the full trace -- and parses it during deploy, which makes query submission slow.
### GP contention sources stay on TCP regardless: they must run unbounded, and a Memory source ends at EOS.
SOURCE_TYPE = os.environ.get("SOURCE_TYPE", "tcp")
if SOURCE_TYPE not in {"tcp", "memory"}:
    raise ValueError(f"unknown SOURCE_TYPE '{SOURCE_TYPE}'; known: memory, tcp")

### Where the prepared trace is cached on the HOST. Bind-mounted read-only into each generator container
### (SOURCE_TYPE=tcp) or into each worker container (SOURCE_TYPE=memory).
### Downloaded + projected on first use, then reused; ~1.5 GB raw + 342 MB projected.
DATA_DIR = os.environ.get(
    "NES_BENCH_DATA_DIR", os.path.join(os.environ.get("XDG_CACHE_HOME", os.path.expanduser("~/.cache")), "nes-bench-data")
)
### Rows each generator loads from the trace (0 = all 18.65 M, ~300 MB resident). A run only consumes
### GENERATOR_RATE x hold seconds, so cap this when running many sources on one host.
GENERATOR_MAX_ROWS = int(os.environ.get("GENERATOR_MAX_ROWS", 0))

### Replay the trace this many times back-to-back, each copy shifted forward by a whole number of
### windows covering the trace's own span (see prepare_cluster_monitoring.replicate). 1 = the raw trace.
### This exists for SOURCE_TYPE=memory throughput runs: the Memory source drains 18.65 M rows in ~3.5 s,
### so the measured span is short enough that warm-up is a large share of it. N copies multiply the span
### by N without inventing data or altering any window's contents. Costs N x the parse time at deploy and
### N x ~300 MB of parsed buffers, so raise NUMBER_OF_BUFFERS to match.
DATASET_COPIES = int(os.environ.get("DATASET_COPIES", 1))

### TCP source ports. Each leaf runs its sources in its own netns, so ports may repeat across leaves;
### the per-query generator binds exactly one port. The worker dials 127.0.0.1:<port> over loopback.
TCP_PORT_BASE = int(os.environ.get("TCP_PORT_BASE", 9100))
FLUSH_INTERVAL_MS = int(os.environ.get("FLUSH_INTERVAL_MS", 10))
### Per-source generator emission rate (tuples/sec). Applied to ALL variants for a fair comparison.
### A finite rate (vs the generator's default full speed) decouples window-close cadence from raw
### throughput so event-time windows close on a predictable schedule (GENERATOR_RATE / window-in-tuples
### closes per second) instead of flooding/stalling. 0 = unlimited.
###
### This knob decides WHICH of the two measurements a run produces, and they cannot be taken together:
###   rate-limited (default) -> WIRE BYTES. Every variant ingests the same tuples, so the byte counts
###                             are comparable. The mean_tps column just reports the limiter back.
###   GENERATOR_RATE=0       -> THROUGHPUT. Each variant runs at its own ceiling, so mean_tps
###                             discriminates -- but they now ingest different amounts and the byte
###                             counts are NOT comparable across variants. Plot with
###                             `plot_results.py --charts throughput`.
### Caveat on the absolute figure: sources are TCP + CSV (one loopback socket per source, parsed from
### text), so mean_tps is a whole-pipeline number that includes transport and parsing, not the cost of
### the statistic build alone. That overhead is identical across variants, so it compresses the
### relative differences rather than biasing any one variant.
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
### Window size, defaulted per dataset (see DATASET_DEFAULTS). On the REAL trace this is real event
### time: 60 s windows hold ~3.0 k tuples, so at GENERATOR_RATE=200k windows close ~66x/sec. On the
### synthetic generator, whose `timestamp` is a per-tuple counter, "N ms" is really N TUPLES: at
### GENERATOR_RATE=200k and 100k-tuple windows, windows close ~2x/sec. Either way each window
### summarizes many raw tuples into one synopsis, so the synopsis must be set smaller than the raw
### window (see HISTOGRAM_MEMORY_BUDGET) for the gradient prometheus(raw) > split(synopsis) > local(scalars).
STATISTIC_WINDOW_SIZE_MS = int(os.environ.get("STATISTIC_WINDOW_SIZE_MS", _DS["window_ms"]))
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
### split (synopsis crosses) land between prometheus (raw) and local (4 scalars). 1 KiB << 1.2 MB/window.
### This is also WEAVE's per-synopsis upstream cost: footprint ~= (SOURCES_PER_LEAF x leaves) x
### (GENERATOR_RATE / window_tuples) x HISTOGRAM_MEMORY_BUDGET. At 1 KiB that's ~0.13 Mbit/s (was 2.1 at
### 16 KB), so WEAVE now fits — and wins — under caps down to a few hundred kbit.
HISTOGRAM_MEMORY_BUDGET = int(os.environ.get("HISTOGRAM_MEMORY_BUDGET", 1024))
### Bin range, defaulting to the dataset's value range (see DATASET_DEFAULTS) and passed into the
### query's SET clause. Getting this wrong silently invalidates every delta number: the request
### generator defaults the range to [0,1000] and EquiWidthHistogramPhysicalFunction clamps anything
### above maxValue into the UPPERMOST bin, so a workload whose values run past the range piles nearly
### every tuple into one bin. The synopsis stays full size (it always carries all bins) while the delta
### shrinks to a single changed bin -- a ratio that measures the misconfiguration, not the compression.
HISTOGRAM_MIN = int(os.environ.get("HISTOGRAM_MIN", _DS["value_min"]))
HISTOGRAM_MAX = int(os.environ.get("HISTOGRAM_MAX", _DS["value_max"]))
PROM_HISTOGRAM_NUM_BUCKETS = int(os.environ.get("PROM_HISTOGRAM_NUM_BUCKETS", 100))

### Throughput-listener sampling period. mean_tps is the mean over the samples between the first and
### last non-zero one, so this sets the resolution of the active span: a run that only sustains a few
### seconds (a Memory source drains the trace in ~4 s) gets very few 200 ms samples, and one sample of
### span error is then several percent -- more than the differences between variants. Drop it to 50 ms
### for short throughput runs. It does not change the system under test, only how finely it is watched.
THROUGHPUT_LISTENER_INTERVAL_MS = int(os.environ.get("THROUGHPUT_LISTENER_INTERVAL_MS", 200))

### Worker engine config (mirrors the single-node experiment).
EXECUTION_MODE = "COMPILER"
JOIN_STRATEGY = "HASH_JOIN"
PAGE_SIZE = 8192
### Operator buffer size. Env-overridable so a run can sweep buffer geometry independently of
### HISTOGRAM_MEMORY_BUDGET.
BUFFER_SIZE_BYTES = int(os.environ.get("BUFFER_SIZE_BYTES", 8192))
NUMBER_OF_BUFFERS = int(os.environ.get("NUMBER_OF_BUFFERS", 200_000))

### Traffic sampling: read the ROOT container's eth0 RX (received) bytes every interval and difference,
### i.e. the total incoming network bytes to the root regardless of tree depth.
TRAFFIC_SAMPLE_INTERVAL_SECONDS = float(os.environ.get("TRAFFIC_SAMPLE_INTERVAL_SECONDS", 1.0))
NET_IFACE = os.environ.get("NET_IFACE", "eth0")


def worker_threads(role):
    """Engine worker threads tied to the container CPU budget so it isn't oversubscribed."""
    cpus = ROOT_CPUS if role == "root" else LEAF_CPUS
    return max(1, int(float(cpus)))
