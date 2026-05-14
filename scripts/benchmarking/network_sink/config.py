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

"""Configuration for the network-sink congestion experiment.

The experiment renders a 2-D grid per cap:
  * rows    — HIGH staircase shape (square-wave alternating between two rates, fractions of cap_tps)
  * columns — LOW-priority offered emit rate (cooperative vs. greedy, fractions of cap_tps)

with the cap dimension swept independently (one twin figure per cap; the link being the
bottleneck is the assumed precondition for the experiment to be meaningful).

The experimental design and policy taxonomy draw from:
  * Strict priority queueing (DiffServ EF, RFC 2475/2598) — the existing ADAPTIVE_DIFFERENT_PRIO
    policy; brittle under sustained HIGH oversubscription (LOW starves).
  * Weighted Fair Queueing (WFQ) and Generalized Processor Sharing (GPS) — Demers, Keshav, Shenker
    (SIGCOMM 1989), Parekh, Gallager (IEEE/ACM ToN 1993). Foundational for class-weighted
    bandwidth share.
  * Hierarchical Token Bucket (HTB; Devera, Linux `tc qdisc htb`) — class-weighted scheduling
    with work conservation. Closest formal model for the in-engine bandwidth-aware scheduler.
  * DiffServ Assured Forwarding (RFC 2597) — minimum-rate guarantee under congestion. The
    motivation for replacing strict priority with weighted scheduling: LOW gets a guaranteed
    liveness floor instead of starving.
  * Apache Flink credit-based backpressure (Carbone et al.; Flink blog, "A Deep-Dive into
    Flink's Network Stack", 2019) — engineering foundation for per-channel pending-acks credit
    windows.
  * Cameo (Xu et al., NSDI 2021) — fine-grained per-message priority scheduling in stream
    processing using deadlines; closest streaming-system prior art.
  * Henge (Kalim et al., SoCC 2018) — intent-driven multi-tenant stream processing with SLOs;
    operates at scheduler/SLO layer (orthogonal to per-channel send arbitration).
  * Eddies (Avnur, Hellerstein, SIGMOD 2000) and rate-based AQP for streaming sources (Viglas,
    Naughton, SIGMOD 2002) — adaptive query processing; motivation for integrating statistics
    collection within the engine so adaptive optimizations like priority arbitration can use
    engine-internal state.

For every (HIGH config, LOW config) cell the driver runs one trial per strategy:
  * ALWAYS_SEND              — TCP fair-share baseline; HIGH halved when LOW competes.
  * ADAPTIVE_DIFFERENT_PRIO  — engine-internal priority gate; HIGH protected; LOW yields.
  * HIGH_ALONE               — sentinel: no LOW queries, used as the no-harm overlay reference per row
                               (independent of the LOW dimension — one HIGH_ALONE trial per HIGH config).

The notebook then renders two figures (twin grid):
  * congestion_grid_adaptive.pdf      — ADAPTIVE_DIFFERENT_PRIO across all (HIGH, LOW) cells.
  * congestion_grid_always_send.pdf   — ALWAYS_SEND across the same cells.

with HIGH-alone overlaid as a faint dashed reference per row in both figures, so the no-harm and
work-conservation properties are visually compared cell-by-cell across policies.
"""

import os


## Worker port assignment ######################################################

# Topology (matches query-templates/*.yaml):
#   worker-2 hosts the GeneratorSource and the auto-inserted NetworkSink (egress side).
#   worker-1 hosts the auto-inserted NetworkSource and the VoidSink (ingress side, downstream of `lo` tc).
# We submit queries via nes-cli to worker-1 (the coordinator role).
# Listener data sources:
#   worker-2.log → source-side throughput listener (attempt rate; can exceed cap during bursts)
#                  + sender-side latency listener (queue-time only).
#   worker-1.log → receive-side throughput listener (post-throttle delivered rate; bounded by cap).
WORKER_1_GRPC = "127.0.0.1:18080"
WORKER_1_DATA = "127.0.0.1:19090"
WORKER_2_GRPC = "127.0.0.1:18081"
WORKER_2_DATA = "127.0.0.1:19091"


## Experiment parameters #######################################################

# tc rates applied to the data ports. Each cap produces its own pair of twin-grid figures in the
# notebook (so the policy difference can be compared at multiple wire capacities). HIGH/LOW rates
# in the grid configs below are expressed as fractions of cap_tps and resolved per-cap by the
# driver, so the same workload shape is replayed at every cap.
# SMOKE: single headline cap (1mbit) — restore ["500kbit", "1mbit", "2mbit"] for the full sweep.
EXPERIMENT_CAPS = ["1mbit"]

# Grid row axis: HIGH staircase configs. `label` becomes a CSV column and the subplot row title.
# Each entry parameterizes the STEP GeneratorRate of the HIGH query — a 2-rate square-wave
# alternating between high_frac and low_frac (both as fractions of cap_tps).
#
# Cell-level intent (with HIGH_NEAR_THRESHOLD-style gating in place). Labels follow standard
# queueing-theory vocabulary (ρ<1 / ρ≈1 / ρ>1) for portability across papers:
#   underloaded: HIGH well below cap throughout. Negative control — policy difference should be
#                minimal; demonstrates the policy doesn't cause harm at low load.
#   balanced:    HIGH at cap during the active phase, half cap during the idle phase. Headline
#                row — ALWAYS_SEND collapses HIGH during active phase, ADAPTIVE preserves it.
#   overloaded:  HIGH always above cap (1.5× cap). Appendix row — exposes the strict-priority
#                starvation regime (LOW = 0 under ADAPTIVE) so the property is visible in the
#                artifact and not only in prose.
HIGH_STEP_CONFIGS = [
    {"label": "underloaded", "high_frac": 0.50, "low_frac": 0.25, "period_high": 5.0, "period_low": 5.0},
    {"label": "balanced",    "high_frac": 1.00, "low_frac": 0.50, "period_high": 5.0, "period_low": 5.0},
    {"label": "overloaded",  "high_frac": 1.50, "low_frac": 1.50, "period_high": 5.0, "period_low": 5.0},
]

# Grid column axis: LOW emit rate as a fraction of cap_tps.
#   cooperative: LOW emits below cap; LOW is input-bound and cannot oversubscribe by itself.
#                Total demand can still exceed cap when HIGH is at cap.
#   greedy:      LOW emits above cap; LOW saturates the wire on its own. Headline column —
#                only here is the policy mechanism actually load-bearing.
# SMOKE: greedy-only column — restore the cooperative entry for the full sweep.
LOW_EMIT_RATES = [
    # {"label": "cooperative", "frac_of_cap": 0.30},
    {"label": "greedy",      "frac_of_cap": 2.00},
]

# Wall-clock seconds the trial keeps running after the last query has been submitted, so steady-
# state-at-peak-load behavior is captured before teardown. With NUM_LOW_QUERIES=1 (the LOW-0 spawn
# fires immediately after HIGH at t≈0), this is the trial's full measured duration.
# Sized so the HIGH staircase (5 s high + 5 s low = 10 s cycle) completes ≥ 3 full cycles inside
# the [TRIM_HEAD, trial_end - TRIM_TAIL] window — 40 s − 5 s − 5 s = 30 s = 3 cycles. Three
# cycles is the minimum that lets the reader see the periodicity in plots like the blocked /
# gated time-series without mistaking it for one-off transient behavior.
POST_SPAWN_TRIAL_DURATION_SEC = 40.0

# Wall-clock seconds between LOW-query spawns when NUM_LOW_QUERIES > 1. Also the minimum runtime
# guaranteed for the last LOW before the trial ends. With the 1-LOW grid (the new design varies
# LOW *rate*, not *count*), this only matters when somebody overrides --num-low-queries.
LOW_SPAWN_INTERVAL_SEC = 10.0

# Exact number of LOW queries spawned per trial. The grid varies LOW emit rate via LOW_EMIT_RATES;
# the *count* of LOW queries per trial is fixed at 1. Set to 0 to skip LOW spawning entirely
# (alternative way to run HIGH-alone trials).
NUM_LOW_QUERIES = 1

# Used only when NUM_LOW_QUERIES == 0 (single-query mode): trial wall-clock seconds during which
# HIGH runs alone with no LOW contenders. Matched to POST_SPAWN_TRIAL_DURATION_SEC so HIGH_ALONE
# reference trials cover the same ≥ 3 staircase cycles inside the trim window.
HIGH_ONLY_TRIAL_DURATION_SEC = 40.0

# Strategies the experiment iterates over. All non-ALWAYS_SEND strategies share the same
# AdaptiveSendingScheduler implementation; the difference is the (HIGH, LOW) weight tuple:
#   "ALWAYS_SEND"        — TCP fair-share baseline; the scheduler is bypassed entirely (no
#                          per-channel contingent). HIGH halved when LOW competes.
#   "WEIGHTED_STRICT"    — scheduler with weights (HIGH=1.0, LOW=0.0). Strict-priority limit of
#                          HTB: HIGH gets the full wire, LOW silenced. Bench-level alias that
#                          resolves to WEIGHTED_PRIO on the worker with weight overrides.
#   "WEIGHTED_PRIO"      — scheduler with the default weights (SCHEDULER_HIGH_WEIGHT /
#                          SCHEDULER_LOW_WEIGHT, currently 0.95 / 0.05). HIGH bounded at its share,
#                          LOW guaranteed a liveness floor — no starvation under sustained HIGH.
#   "HIGH_ALONE"         — sentinel: no LOW queries spawned; runs ALWAYS_SEND on the worker.
#                          One HIGH_ALONE trial per (cap, HIGH staircase) tuple, used as the
#                          no-harm reference overlay in every cell of the same row.
#
# WEIGHTED_STRICT vs WEIGHTED_PRIO isolates the cost of the strict-priority policy alone (with
# the same source-bp-cycle-free implementation); ALWAYS_SEND vs WEIGHTED_PRIO is the headline
# (engine arbitration vs TCP fair-share).
EXPERIMENT_STRATEGIES = ["ALWAYS_SEND", "WEIGHTED_STRICT", "WEIGHTED_PRIO", "HIGH_ALONE"]

# Repetitions of the full (HIGH × LOW × strategy) sweep.
NUM_RUNS_PER_EXPERIMENT = 1

# Worker-thread count forwarded as `--worker.query_engine.number_of_worker_threads` to every spawned
# single-node-worker. The query engine defaults to 4 (nes-query-engine/interface/QueryEngineConfiguration.hpp).
# Bumped from 2 to 6 so HIGH and LOW pipelines don't contend for the same worker threads — at 2
# threads the LOW source/operator stages took CPU even when the network send was gated, dropping
# HIGH-with-LOW under ADAPTIVE noticeably below HIGH-alone. With 6, each query has dedicated
# capacity and the no-harm property holds for ADAPTIVE. Override on the CLI with --num-worker-threads N.
NUM_WORKER_THREADS = 6

# Network-layer tuning at the worker level — these are the bottleneck below which throughput sits
# well under the cap, even after the per-query backpressure thresholds are loosened. Defaults are
# 1 IO thread per side (serializes all NetworkSink/Source ops across queries) and 4 KB
# operator_buffer_size (small buffers → more ACK round-trips per byte on the wire). Bumped to
# allow the experiment to actually saturate the wire.
SENDER_IO_THREADS = 4
RECEIVER_IO_THREADS = 4
OPERATOR_BUFFER_SIZE = 8192    # bytes; matches the BufferManager default tuple-buffer size

# WEIGHTED_PRIO scheduler knobs forwarded to every spawned worker. See the AdaptiveSendingScheduler
# class in nes-executable. Defaults match the C++ side; override per-experiment if needed.
SCHEDULER_TICK_MS = 100
SCHEDULER_HIGH_WEIGHT = 0.95
SCHEDULER_LOW_WEIGHT = 0.05
# Bootstrap capacity is set per-trial to the trial's tc cap (so the scheduler reaches steady state
# in the first tick); this value is the fallback for trials without a cap.
SCHEDULER_BOOTSTRAP_CAPACITY_BPS_DEFAULT = 12_500_000   # 100 mbit/s in B/s
SCHEDULER_BURST_CAP_BYTES = 32 * 1024
SCHEDULER_DEBUG_LOG = False

# CapacityEstimator selection for the AdaptiveSendingScheduler. "EMA" tracks observed throughput
# (default; faithful to wire reality but can erode toward source-bp-cycled throughput when the
# source can't sustain cap). "FIXED" uses scheduler_fixed_capacity_bps verbatim every tick,
# regardless of observed delivery — suitable for experiments where we know the tc cap exactly
# and want to avoid EMA drift.
SCHEDULER_CAPACITY_MODE = "FIXED"   # "EMA" | "FIXED"
# Fixed capacity (bytes/sec) when MODE == "FIXED". None → driver fills with the trial's tc-cap-
# in-bps (so each cap in EXPERIMENT_CAPS gets the matching fixed value). Set explicitly to lock
# the scheduler to a specific capacity regardless of the tc throttle (rare).
SCHEDULER_FIXED_CAPACITY_BPS = None

# GeneratorSource pacing: each fillTupleBuffer call takes exactly flush_interval_ms (the source
# generates up to one buffer's worth of tuples then sleeps the rest). The max achievable source
# rate is therefore (operator_buffer_size / tuple_size) / flush_interval_s — with the 8 KB
# operator buffer and 8 B tuples, 10 ms ⇒ 102 400 tup/s, 50 ms ⇒ 20 480 tup/s.
# Kept at NES's default 10 ms so the source can reach the configured rate (e.g. 50 000 tup/s for
# HIGH) under cap=none. With the new HIGH_IDLE_GRACE in ADAPTIVE_DIFFERENT_PRIO, source-side
# backpressure cycles no longer leak LOW windows, so the previous source-pacing workaround is
# unnecessary — the receiver-side network_sending chart remains smooth and the strategy gates
# LOW correctly during HIGH's continuous-send phases.
GENERATOR_FLUSH_INTERVAL_MS = 10

# Whether to enable the worker latency listener by default. The latency subplot in the notebook is
# only populated when this is on.
LATENCY_LISTENER_ENABLED = True


## Query templates #############################################################

QUERY_TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query-templates")


## Build #######################################################################

def get_cmake_flags():
    """Cmake flags applied when the benchmark is asked to build NebulaStream from source.

    Defined as a function (not a module-level constant) so importing this module does not call
    ``get_vcpkg_dir()`` — the latter raises on hosts that aren't in its hostname allowlist, which
    would prevent ``--skip-build`` runs on unknown hosts.
    """
    from scripts.benchmarking.utils import get_vcpkg_dir
    return (
        "-G Ninja "
        "-DCMAKE_BUILD_TYPE=Release "
        f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
        "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
        "-DENABLE_LARGE_TESTS=0 "
        "-DNES_BUILD_NATIVE:BOOL=ON "
        "-DNES_LOG_LEVEL:STRING=INFO"
    )


## Result file (one row per query per trial) ##################################

# Bytes per tuple emitted by the GeneratorSource. Matches the FLOAT64 VAL field declared in the
# query-templates' logical schema. Used to convert the tc rate cap to tuples/s.
TUPLE_SIZE_BYTES = 8

# Sweep-dimension columns shared across all CSVs. cap pairs with max_network_rate_tuples_per_s
# (cap converted to tup/s using TUPLE_SIZE_BYTES). high_label / low_label identify the cell within
# the per-cap grid; the absolute high/low rates are recorded for downstream analysis. For
# HIGH_ALONE rows, low_label="none" and low_emit_rate is empty so the notebook can join HIGH_ALONE
# rows by (cap, high_label) alone.
_SWEEP_DIM_FIELDS = [
    "cap",
    "max_network_rate_tuples_per_s",
    "high_label",
    "low_label",
    "high_step_high_rate",
    "high_step_low_rate",
    "high_step_period_high",
    "high_step_period_low",
    "low_emit_rate",
]

RESULT_FIELDNAMES = [
    "run_idx",
    "strategy",
    *_SWEEP_DIM_FIELDS,
    "low_instance_id",     # 0 for HIGH, 0..N-1 for the i-th spawned LOW; empty for failed spawns
    "query_id",            # distributed name returned by nes-cli
    "priority",            # HIGH or LOW
    "spawn_offset_s",      # seconds since the trial started, when this query was submitted
    "delivered_tuples_per_s_avg",   # mean of BufferSent windows for this query
    "duration_s",
    "issue",
]


## Time-series CSVs (one row per priority per window) ##########################

# Width of the binning window applied to listener stdout events when computing per-priority metrics.
NETWORK_EVENTS_WINDOW_MS = 100

# Trim the head and tail of each per-trial event stream before binning. The head exclusion drops
# warmup / first-buffer transients; the tail exclusion drops teardown spikes (kernel TCP buffer
# flushes, last-second source dumps) that would otherwise dominate y-axis scaling and bias
# averages. Applied uniformly to BufferIngest, throughput, and latency binning. Time origin
# (runtime_s = 0) is the first event after the head trim.
TRIM_HEAD_SEC = 5.0
TRIM_TAIL_SEC = 5.0

# Network sending: post-throttle delivered tuples/s, derived from worker-1's throughput listener
# (the receive-side query engine's tuples-processed rate). Bounded by the tc cap. The high_only_*
# column repeats the HIGH value on every row so the notebook can plot HIGH-only without
# re-aggregating.
NETWORK_SENDING_TS_FIELDNAMES = [
    "run_idx",
    "strategy",
    *_SWEEP_DIM_FIELDS,
    "runtime_s",
    "priority",                     # HIGH or LOW (LOW aggregates across all spawned LOW queries)
    "delivered_tuples_per_s",
    "high_only_tuples_per_s",       # HIGH value at this runtime_s (repeated across both priorities)
]

# Throughput listener: per-priority sum of throughput windows reported by the worker.
THROUGHPUT_TS_FIELDNAMES = [
    "run_idx",
    "strategy",
    *_SWEEP_DIM_FIELDS,
    "runtime_s",
    "priority",
    "throughput_tuples_per_s",
    "high_only_tuples_per_s",
]

# Latency listener: per-priority average latency across queries in a window.
LATENCY_TS_FIELDNAMES = [
    "run_idx",
    "strategy",
    *_SWEEP_DIM_FIELDS,
    "runtime_s",
    "priority",
    "latency_seconds",
    "high_only_latency_seconds",
]

# BufferSojourn listener: per-priority mean engine-side sojourn time across buffers in a window.
# Sojourn is the elapsed time from when a buffer first entered NetworkSink::execute to when its
# send_buffer returned Ok — captures backpressure + scheduler-gate retries + Full-induced retries.
# Same shape as LATENCY_TS_FIELDNAMES; the notebook loads it with `plot_headline` unchanged.
SOJOURN_TS_FIELDNAMES = [
    "run_idx",
    "strategy",
    *_SWEEP_DIM_FIELDS,
    "runtime_s",
    "priority",
    "sojourn_seconds",
    "high_only_sojourn_seconds",
]

# BackpressureBlocked listener: per-priority fraction of each window the source thread spent
# blocked inside BackpressureListener::wait(). Complementary to sojourn — sojourn captures sink-
# side per-buffer wait (load-bearing for WEIGHTED), blocked captures source-side block time
# (load-bearing for ALWAYS_SEND, where most queueing manifests as the source being paced). Values
# are in [0, 1]: 0 = source never blocked in this window, 1 = source blocked the entire window.
BLOCKED_TS_FIELDNAMES = [
    "run_idx",
    "strategy",
    *_SWEEP_DIM_FIELDS,
    "runtime_s",
    "priority",
    "blocked_fraction",
    "high_only_blocked_fraction",
]

# SchedulerGated listener: per-priority fraction of each window the NetworkSink spent in a
# scheduler-deny / retry loop (isScheduledToSend returned false). Complementary to BLOCKED — that
# metric captures source-side wait on a CLOSED channel; this one captures sink-side gating by the
# AdaptiveSendingScheduler. Under WEIGHTED_STRICT permanently-gated LOW emits this event but zero
# BackpressureBlocked events. Values are in [0, 1] (overlap-split across windows).
GATED_TS_FIELDNAMES = [
    "run_idx",
    "strategy",
    *_SWEEP_DIM_FIELDS,
    "runtime_s",
    "priority",
    "gated_fraction",
    "high_only_gated_fraction",
]


## LOW spawn schedule (one row per LOW spawn) ##################################

LOW_SPAWN_SCHEDULE_FIELDNAMES = [
    "run_idx",
    "strategy",
    "cap",
    "high_label",
    "low_label",
    "low_instance_id",
    "spawn_offset_ms",   # ms since the trial started
    "query_id",
]
