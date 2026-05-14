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

"""Network-sink congestion experiment driver.

The experiment is a 2-D grid (HIGH staircase × LOW emit rate) replayed at every cap in
EXPERIMENT_CAPS. For each (cap, HIGH config, LOW config) cell the driver runs one trial per
strategy from EXPERIMENT_STRATEGIES; HIGH_ALONE is run once per (cap, HIGH config) tuple
(independent of the LOW dimension) and used as a no-harm overlay reference in every cell of its
row in the notebook. The notebook produces one twin figure per cap (ADAPTIVE vs. ALWAYS_SEND).

Per trial:
  1. Spawn worker-1 and worker-2 with the worker-side strategy
     (HIGH_ALONE resolves to ALWAYS_SEND).
  2. Apply a tc htb throttle (the cell's cap) to the data ports (WORKER_*_DATA), leaving
     the gRPC ports unthrottled.
  3. Submit one HIGH-priority query at t=0 driven by a STEP GeneratorRate (square-wave between
     high_frac and low_frac of cap_tps, periods configured per HIGH_STEP_CONFIGS entry).
  4. For non-HIGH-alone trials, submit one LOW-priority query (FIXED rate at low_cfg.frac_of_cap
     × cap_tps) immediately after HIGH. HIGH_ALONE skips this; it just sleeps for the same
     wall-clock duration so the time axes align across the twin grids.
  5. Tear down workers, parse the worker-2 stdout log, and append four time-series CSVs:
       * network_sending_timeseries.csv  (BufferSent events binned per priority + HIGH-only)
       * throughput_timeseries.csv       (throughput listener, per priority + HIGH-only)
       * latency_timeseries.csv          (latency listener, per priority + HIGH-only)
     plus the per-query results_network_sink.csv and low_spawn_schedule.csv. Each row carries
     high_label / low_label / explicit rate columns so the notebook can filter per cell.

The notebook in plots/ renders one twin figure per policy (ADAPTIVE_DIFFERENT_PRIO and
ALWAYS_SEND), each a (HIGH_STEP_CONFIGS × LOW_EMIT_RATES) grid of continuous time-series plots,
with HIGH_ALONE faintly dashed in every cell of its row.
"""

import argparse
import csv
import getpass
import os
import re
import shutil
import subprocess
import time
from collections import defaultdict
from datetime import datetime, timedelta
from string import Template

from scripts.benchmarking.common._console import banner, step
from scripts.benchmarking.common.config import (
    BUILD_DIR,
    NEBULI_EXECUTABLE,
    SINGLE_NODE_EXECUTABLE,
    THROUGHPUT_LISTENER_INTERVAL,
    WAIT_BETWEEN_COMMANDS_LONG,
)
from scripts.benchmarking.common.worker_lifecycle import (
    terminate_process_if_exists,
)
from scripts.benchmarking.network_sink.config import (
    EXPERIMENT_CAPS,
    EXPERIMENT_STRATEGIES,
    GENERATOR_FLUSH_INTERVAL_MS,
    HIGH_ONLY_TRIAL_DURATION_SEC,
    HIGH_STEP_CONFIGS,
    LATENCY_LISTENER_ENABLED,
    LATENCY_TS_FIELDNAMES,
    LOW_EMIT_RATES,
    LOW_SPAWN_INTERVAL_SEC,
    LOW_SPAWN_SCHEDULE_FIELDNAMES,
    NETWORK_EVENTS_WINDOW_MS,
    NETWORK_SENDING_TS_FIELDNAMES,
    NUM_LOW_QUERIES,
    NUM_RUNS_PER_EXPERIMENT,
    NUM_WORKER_THREADS,
    OPERATOR_BUFFER_SIZE,
    POST_SPAWN_TRIAL_DURATION_SEC,
    RECEIVER_IO_THREADS,
    SCHEDULER_BOOTSTRAP_CAPACITY_BPS_DEFAULT,
    SCHEDULER_BURST_CAP_BYTES,
    SCHEDULER_CAPACITY_MODE,
    SCHEDULER_DEBUG_LOG,
    SCHEDULER_FIXED_CAPACITY_BPS,
    SCHEDULER_HIGH_WEIGHT,
    SCHEDULER_LOW_WEIGHT,
    SCHEDULER_TICK_MS,
    SENDER_IO_THREADS,
    SOJOURN_TS_FIELDNAMES,
    BLOCKED_TS_FIELDNAMES,
    GATED_TS_FIELDNAMES,
    QUERY_TEMPLATES_DIR,
    RESULT_FIELDNAMES,
    THROUGHPUT_TS_FIELDNAMES,
    TRIM_HEAD_SEC,
    TRIM_TAIL_SEC,
    TUPLE_SIZE_BYTES,
    WORKER_1_DATA,
    WORKER_1_GRPC,
    WORKER_2_DATA,
    WORKER_2_GRPC,
    get_cmake_flags,
)
from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    convert_unit_prefix,
    create_folder_and_remove_if_exists,
    printError,
    printInfo,
    printSuccess,
)


## ---------------------------------------------------------------------------
## Worker process management.
## ---------------------------------------------------------------------------

def _spawn_worker(grpc_addr, data_addr, sink_strategy, log_path, latency_enabled, num_worker_threads,
                  scheduler_bootstrap_capacity_bps, scheduler_high_weight, scheduler_low_weight,
                  scheduler_fixed_capacity_bps):
    """Start a single-node-worker. Returns (Popen, log_file).

    *sink_strategy* is the worker-side enum value ("ALWAYS_SEND" or "WEIGHTED_PRIO"). Bench-level
    strategies (HIGH_ALONE, WEIGHTED_STRICT) are resolved by the caller before this call.
    *scheduler_bootstrap_capacity_bps* is the cap-in-bytes forwarded to the
    AdaptiveSendingScheduler — used as the EMA bootstrap value (mode=EMA) or as the fallback
    when scheduler_fixed_capacity_bps is 0 (mode=FIXED). *scheduler_high_weight* /
    *scheduler_low_weight* override the defaults — set to (1.0, 0.0) for the WEIGHTED_STRICT
    bench strategy. *scheduler_fixed_capacity_bps* is the constant used when mode=FIXED; 0 means
    fall back to bootstrap (which the bench fills with the trial's tc-cap-in-bps).
    """
    cmd = [
        SINGLE_NODE_EXECUTABLE,
        f"--grpc={grpc_addr}",
        f"--data_address={data_addr}",
        f"--network_sink_sending_strategy={sink_strategy}",
        f"--worker.throughput_listener_interval_in_ms={THROUGHPUT_LISTENER_INTERVAL}",
        f"--worker.latency_listener={str(latency_enabled).lower()}",
        f"--worker.query_engine.number_of_worker_threads={num_worker_threads}",
        f"--worker.network.sender_io_threads={SENDER_IO_THREADS}",
        f"--worker.network.receiver_io_threads={RECEIVER_IO_THREADS}",
        f"--worker.network.scheduler_tick_ms={SCHEDULER_TICK_MS}",
        f"--worker.network.scheduler_high_weight={scheduler_high_weight}",
        f"--worker.network.scheduler_low_weight={scheduler_low_weight}",
        f"--worker.network.scheduler_bootstrap_capacity_bps={scheduler_bootstrap_capacity_bps}",
        f"--worker.network.scheduler_capacity_mode={SCHEDULER_CAPACITY_MODE}",
        f"--worker.network.scheduler_fixed_capacity_bps={scheduler_fixed_capacity_bps}",
        f"--worker.network.scheduler_burst_cap_bytes={SCHEDULER_BURST_CAP_BYTES}",
        f"--worker.network.scheduler_debug_log={str(SCHEDULER_DEBUG_LOG).lower()}",
        f"--worker.default_query_execution.operator_buffer_size={OPERATOR_BUFFER_SIZE}",
    ]
    log_file = open(log_path, "w")
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
    return proc, log_file


## ---------------------------------------------------------------------------
## tc throttle (htb root + filter on the NetworkSink data ports).
## ---------------------------------------------------------------------------

def _print_tc_sudo_hint():
    """Print a copy-pasteable hint for configuring passwordless sudo for `tc`."""
    user = getpass.getuser()
    tc_path = shutil.which("tc") or "/usr/sbin/tc"
    printInfo(
        "\nTo enable the network-rate sweep, allow passwordless sudo for `tc` only.\n"
        "  Run (once):\n"
        f"    echo '{user} ALL=(root) NOPASSWD: {tc_path}' "
        "| sudo tee /etc/sudoers.d/nebulastream-tc\n"
        "    sudo chmod 0440 /etc/sudoers.d/nebulastream-tc\n"
        "  Or pass `--caps none` to skip the throttle phase entirely.\n"
    )


def _abort_throttle(msg, rate, *, sudo_hint=False):
    """Print *msg*, optionally the sudo hint, then abort the whole run."""
    printError(msg)
    if sudo_hint:
        _print_tc_sudo_hint()
    printError(
        f"Aborting: cannot apply throttle (rate={rate}). The congestion experiment is meaningless "
        "without throttling. Configure passwordless sudo as shown above, or pass `--caps none` "
        "to run unthrottled."
    )
    raise SystemExit(2)


def _preflight_tc_sudo(any_throttled):
    """Verify passwordless sudo for `tc` works before any worker is spawned. No-op when no caps."""
    if not any_throttled:
        return
    cmd = ["sudo", "-n", "tc", "qdisc", "show", "dev", "lo"]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=5)
    except FileNotFoundError:
        _abort_throttle("tc/sudo not found on PATH", "<preflight>", sudo_hint=True)
    except subprocess.TimeoutExpired:
        _abort_throttle("tc/sudo preflight timed out", "<preflight>")
    if result.returncode != 0:
        stderr_text = result.stderr.decode().strip()
        _abort_throttle(
            f"sudo -n tc preflight failed (rc={result.returncode}): {stderr_text}",
            "<preflight>",
            sudo_hint=stderr_text.lower().startswith("sudo:"),
        )


def _apply_tc_throttle(rate, ctx):
    """Apply htb throttle on loopback to the NetworkSink data ports (WORKER_*_DATA = 19090 / 19091)
    only — the gRPC control ports stay unthrottled so very low rates don't starve query submission.
    """
    if rate == "none":
        return

    data_port_1 = WORKER_1_DATA.rsplit(":", 1)[1]
    data_port_2 = WORKER_2_DATA.rsplit(":", 1)[1]
    setup_cmds = [
        ["sudo", "-n", "tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:", "htb", "default", "1"],
        ["sudo", "-n", "tc", "class", "add", "dev", "lo", "parent", "1:", "classid", "1:1",
         "htb", "rate", "10gbit"],
        ["sudo", "-n", "tc", "class", "add", "dev", "lo", "parent", "1:", "classid", "1:2",
         "htb", "rate", rate, "burst", "128kbit"],
        ["sudo", "-n", "tc", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "1:",
         "prio", "1", "u32", "match", "ip", "dport", data_port_1, "0xffff", "flowid", "1:2"],
        ["sudo", "-n", "tc", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "1:",
         "prio", "1", "u32", "match", "ip", "dport", data_port_2, "0xffff", "flowid", "1:2"],
    ]
    for cmd in setup_cmds:
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=5)
        except FileNotFoundError:
            _abort_throttle("tc/sudo not found on PATH", rate, sudo_hint=True)
        except subprocess.TimeoutExpired:
            _abort_throttle(f"tc command timed out: {' '.join(cmd[3:])}", rate)
        if result.returncode != 0:
            stderr_text = result.stderr.decode().strip()
            subprocess.run(
                ["sudo", "-n", "tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True, timeout=5)
            _abort_throttle(
                f"tc command failed (rc={result.returncode}): {' '.join(cmd[3:])}\n  stderr: {stderr_text}",
                rate,
                sudo_hint=stderr_text.lower().startswith("sudo:"),
            )
    ctx["tc_active"] = True


def _remove_tc_throttle(ctx):
    if not ctx.get("tc_active"):
        return
    subprocess.run(
        ["sudo", "-n", "tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True, timeout=5)
    ctx["tc_active"] = False


## ---------------------------------------------------------------------------
## YAML rendering and nes-cli submission.
## ---------------------------------------------------------------------------

def _render_template(template_path, dest_path, substitutions):
    with open(template_path) as fh:
        rendered = Template(fh.read()).safe_substitute(substitutions)
    with open(dest_path, "w") as fh:
        fh.write(rendered)


def _submit_query(yaml_path):
    """Submit a query via nes-cli and return its query id (the distributed name on stdout's last
    non-empty line). None on failure.
    """
    cmd = list(NEBULI_EXECUTABLE) + ["-t", yaml_path, "start"]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
    except subprocess.TimeoutExpired:
        printError(f"nes-cli start timed out for {yaml_path}")
        return None
    stdout_text = result.stdout.decode(errors="replace")
    stderr_text = result.stderr.decode(errors="replace")
    if result.returncode != 0:
        run_dir = os.path.dirname(yaml_path)
        base = os.path.splitext(os.path.basename(yaml_path))[0]
        stdout_log = os.path.join(run_dir, f"{base}.cli_stdout.log")
        stderr_log = os.path.join(run_dir, f"{base}.cli_stderr.log")
        with open(stdout_log, "w") as fh:
            fh.write(stdout_text)
        with open(stderr_log, "w") as fh:
            fh.write(stderr_text)
        printError(
            f"nes-cli start failed (rc={result.returncode}) for {yaml_path}\n"
            f"  full output in {stdout_log} / {stderr_log}\n"
            f"  stdout head: {stdout_text[:1000].rstrip()}\n"
            f"  stderr head: {stderr_text[:1000].rstrip()}")
        return None
    for line in reversed(stdout_text.strip().splitlines()):
        line = line.strip()
        if line:
            return line
    return None


## ---------------------------------------------------------------------------
## Worker stdout parsing.
## ---------------------------------------------------------------------------

# BufferSent: emitted by BackpressureStatisticStdoutEmitter::onEvent on every successful sink send
# (worker-2 only — that's where the NetworkSink lives). Carries the buffer's tuple count.
_BUFFER_SENT_LINE_RE = re.compile(
    r"BufferSent for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"priority (?P<prio>HIGH|LOW) tuples=(?P<tuples>\d+) at (?P<ns>\d+) ns"
)

# BufferIngest: emitted by SourceThread for every buffer the source ingests. Fires on both workers
# (GeneratorSource on w2, NetworkSource on w1) — we read the worker-1 stream, where every ingest
# event corresponds to one buffer that survived the tc throttle. The tuple count is on the line so
# we sum it directly into the per-window receive rate; no avg-buffer-size approximation needed.
_BUFFER_INGEST_LINE_RE = re.compile(
    r"BufferIngest for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"priority (?P<prio>HIGH|LOW) tuples=(?P<tuples>\d+) at (?P<ns>\d+) ns"
)

# Throughput listener: one line per (query, window) flushed every THROUGHPUT_LISTENER_INTERVAL ms.
# Window timestamps are relative monotonic-clock millis; we normalize to trial-start in post.
# Only fires on workers whose pipeline has a `firstPipeline=true` stage — that's worker-2 for our
# topology (worker-1's NetworkSource→VoidSink is Source→Sink directly, so no firstPipeline marker).
_THROUGHPUT_LINE_RE = re.compile(
    r"Throughput for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"in window (?P<start>\d+)-(?P<end>\d+) is (?P<value>\d+\.\d+) (?P<unit>\w*)Tup/s"
)

# Latency listener: one line per (query, window). Same window scheme as throughput.
_LATENCY_LINE_RE = re.compile(
    r"Latency for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"and (?P<tasks>\d+) tasks over duration (?P<start>\d+)-(?P<end>\d+) is "
    r"(?P<value>\d+\.\d+) (?P<unit>\w*)s"
)
_LATENCY_UNIT = {"": 1.0, "m": 1e-3, "u": 1e-6, "n": 1e-9}

# Buffer sojourn (engine-side queueing delay) listener: one line per successful send. Emitted by
# BackpressureStatisticStdoutEmitter alongside BufferSent. sojourn_ns is the elapsed time from
# `BackpressureController::recordBufferArrival` (top of NetworkSink::execute) to
# `recordBufferSojourn` (after send_buffer returned Ok), in nanoseconds.
_BUFFER_SOJOURN_LINE_RE = re.compile(
    r"BufferSojourn for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"priority (?P<prio>HIGH|LOW) sojourn_ns=(?P<sojourn_ns>\d+) at (?P<ns>\d+) ns"
)

# Backpressure blocked (source-side wait inside BackpressureListener::wait()) listener: one line
# per wait() that actually blocked (channel was CLOSED on entry). blocked_ns is the wall-clock
# duration the source thread spent blocked, in nanoseconds. The event's timestamp (`at … ns`) is
# the wait-end, so the binner reconstructs the wait window via [end - blocked_ns, end].
_BACKPRESSURE_BLOCKED_LINE_RE = re.compile(
    r"BackpressureBlocked for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"priority (?P<prio>HIGH|LOW) blocked_ns=(?P<blocked_ns>\d+) at (?P<ns>\d+) ns"
)

# Scheduler-gated (sink-side AdaptiveSendingScheduler contingent deny+retry loop) listener: one
# line per gating episode at the moment the next buffer finally passes isScheduledToSend. gated_ns
# is the wall-clock duration the sink spent in the deny/retry loop, in nanoseconds. The event's
# timestamp (`at … ns`) is the pass-through, so the binner reconstructs the gated window via
# [end - gated_ns, end]. Complementary to BackpressureBlocked: scheduler gating happens at the
# sink before the source ever calls wait(), so under WEIGHTED_STRICT permanently-gated LOW
# produces this event but emits zero blocked events.
_SCHEDULER_GATED_LINE_RE = re.compile(
    r"SchedulerGated for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"priority (?P<prio>HIGH|LOW) gated_ns=(?P<gated_ns>\d+) at (?P<ns>\d+) ns"
)

# StaircasePhaseStart listener: fired once per (queryId, priority) by SourceThread at the first
# successful fillTupleBuffer of a staircase-pattern source (GeneratorSource using StepGeneratorRate).
# The bench uses HIGH's phase-start ns as the trial's t=0 reference so HIGH-alone and contended HIGH
# trials align on the staircase phase regardless of warmup delay. phase_idx is always 0 today.
_STAIRCASE_PHASE_START_LINE_RE = re.compile(
    r"StaircasePhaseStart for queryId QueryId\(local=(?P<local>[0-9a-f-]+), distributed=(?P<dist>[a-z_]+)\) "
    r"priority (?P<prio>HIGH|LOW) phase_idx=(?P<phase>\d+) at (?P<ns>\d+) ns"
)


def _parse_phase_start_ns(log_path, query_priority):
    """Scan the worker log for StaircasePhaseStart events and return the HIGH-priority phase-start
    timestamp (ns). Returns None when the trial didn't run a staircase source (e.g. LOW-only
    benchmarks). Each (queryId, priority) emits exactly one event; we pick the earliest HIGH ns
    in case multiple HIGH queries are present (HIGH_ALONE plus contended in the same log would
    be the rare case; the bench separates them per-trial today).
    """
    if not os.path.exists(log_path):
        return None
    high_ns_candidates = []
    with open(log_path) as fh:
        for line in fh:
            match = _STAIRCASE_PHASE_START_LINE_RE.search(line)
            if not match:
                continue
            distributed = match.group("dist")
            if query_priority.get(distributed) != "HIGH":
                continue
            high_ns_candidates.append(int(match.group("ns")))
    return min(high_ns_candidates) if high_ns_candidates else None


def _align_to_cycle_boundary(phase_start_ns, period_high_s, period_low_s):
    """Snap *phase_start_ns* back to the most recent staircase cycle boundary in wall-clock time.

    StepGeneratorRate is anchored to system_clock epoch, so the staircase phase at any wall-clock
    time T is `(T mod cycle_length) < period_high`. Different trials' first-emit moments land at
    different positions within the cycle, so using first-emit as runtime_s=0 leaves a residual
    within-cycle phase offset. Subtracting the cycle remainder rebases each trial to the most
    recent cycle boundary instead — runtime_s=0 then maps to the same staircase position across
    every trial, making HIGH-alone and contended HIGH curves visually coincide.

    Returns the adjusted ns. No-op when *phase_start_ns* is None or the cycle length is invalid.
    """
    if phase_start_ns is None:
        return None
    cycle_ns = int((float(period_high_s) + float(period_low_s)) * 1_000_000_000)
    if cycle_ns <= 0:
        return phase_start_ns
    return phase_start_ns - (phase_start_ns % cycle_ns)


def _bin_buffer_ingest_events(log_path, query_priority, reference_ns=None):
    """Parse BufferIngest events from worker-1 and bin per (window, priority) → tuples/s.

    Each ingest event carries the buffer's actual tuple count (added to the log line so the bench
    no longer has to multiply ingest counts by an across-trial average buffer fill, which over-
    reported bursty traffic). Returns ({(widx, priority): tuples_per_s}, {distributed: total_tuples}).
    """
    if not os.path.exists(log_path):
        return {}, {}

    events = []  # (ns, distributed, priority, tuples)
    with open(log_path) as fh:
        for line in fh:
            match = _BUFFER_INGEST_LINE_RE.search(line)
            if not match:
                continue
            distributed = match.group("dist")
            if distributed not in query_priority:
                continue
            events.append((int(match.group("ns")), distributed, match.group("prio"), int(match.group("tuples"))))

    if not events:
        return {}, {}

    # Trim head/tail: drop events in the first TRIM_HEAD_SEC and the last TRIM_TAIL_SEC of the
    # observed event range. Removes warmup transients and teardown spikes (kernel TCP buffer
    # flushes etc.) so they don't bias the time-series rates or stretch the y-axis.
    t_first_raw_ns = min(ev[0] for ev in events)
    t_last_raw_ns = max(ev[0] for ev in events)
    trim_start_ns = t_first_raw_ns + int(TRIM_HEAD_SEC * 1_000_000_000)
    trim_end_ns = t_last_raw_ns - int(TRIM_TAIL_SEC * 1_000_000_000)
    events = [ev for ev in events if trim_start_ns <= ev[0] <= trim_end_ns]
    if not events:
        return {}, {}

    # Reference for `runtime_s = 0`: phase-start ns when supplied (cross-trial alignment via
    # StaircasePhaseStartEvent), else earliest observed event (current behavior).
    rebase_ns = reference_ns if reference_ns is not None else min(ev[0] for ev in events)
    window_ns = NETWORK_EVENTS_WINDOW_MS * 1_000_000
    window_seconds = NETWORK_EVENTS_WINDOW_MS / 1000.0

    # Per (window, priority) tuple totals; per-query running tuple totals feed results CSV's
    # delivered_tuples_per_s_avg.
    tuples_per_window = defaultdict(int)
    per_query_tuples = defaultdict(int)
    for ts_ns, distributed, priority, tuples in events:
        widx = (ts_ns - rebase_ns) // window_ns
        tuples_per_window[(widx, priority)] += tuples
        per_query_tuples[distributed] += tuples

    windows = {key: total / window_seconds for key, total in tuples_per_window.items()}
    return windows, dict(per_query_tuples)


def _aggregate_per_query_throughput(log_path, query_priority):
    """Average throughput per (distributed) query across the trial, from a worker's throughput-
    listener log lines. Returns {distributed_name: (priority, avg_tuples_per_s)}; queries with no
    samples are simply omitted.
    """
    if not os.path.exists(log_path):
        return {}

    sums = defaultdict(lambda: [0.0, 0])
    with open(log_path) as fh:
        for line in fh:
            match = _THROUGHPUT_LINE_RE.search(line)
            if not match:
                continue
            distributed = match.group("dist")
            priority = query_priority.get(distributed)
            if priority is None:
                continue
            tps = convert_unit_prefix(float(match.group("value")), match.group("unit"))
            slot = sums[distributed]
            slot[0] += tps
            slot[1] += 1
    return {
        distributed: (query_priority[distributed], total / count)
        for distributed, (total, count) in sums.items() if count > 0
    }


def _bin_throughput_events(log_path, query_priority, reference_ns=None):
    """Parse throughput-listener lines and bin per priority by window-start timestamp.

    Each line maps a (query, window) → tuples/s. We sum per priority within a NETWORK_EVENTS_WINDOW_MS
    bucket so the LOW row aggregates across all spawned LOW queries.

    Returns: {(window_idx, priority): tuples_per_s} keyed off the first throughput timestamp.
    """
    if not os.path.exists(log_path):
        return {}

    events = []
    with open(log_path) as fh:
        for line in fh:
            match = _THROUGHPUT_LINE_RE.search(line)
            if not match:
                continue
            distributed = match.group("dist")
            priority = query_priority.get(distributed)
            if priority is None:
                continue
            tps = convert_unit_prefix(float(match.group("value")), match.group("unit"))
            events.append((int(match.group("start")), priority, tps))

    if not events:
        return {}

    # Trim head/tail (see _bin_buffer_ingest_events for rationale).
    t_first_raw_ms = min(ev[0] for ev in events)
    t_last_raw_ms = max(ev[0] for ev in events)
    trim_start_ms = t_first_raw_ms + int(TRIM_HEAD_SEC * 1000)
    trim_end_ms = t_last_raw_ms - int(TRIM_TAIL_SEC * 1000)
    events = [ev for ev in events if trim_start_ms <= ev[0] <= trim_end_ms]
    if not events:
        return {}

    # ns→ms conversion for the optional cross-trial reference. The throughput log line emits
    # window-start times in ms, so we rebase in ms too.
    rebase_ms = (reference_ns // 1_000_000) if reference_ns is not None else min(ev[0] for ev in events)
    out = defaultdict(float)
    for ts_ms, priority, tps in events:
        widx = (ts_ms - rebase_ms) // NETWORK_EVENTS_WINDOW_MS
        out[(widx, priority)] += tps
    return dict(out)


def _bin_latency_events(log_path, query_priority, reference_ns=None):
    """Parse latency-listener lines and bin per priority. Latencies are averaged within a window
    (not summed): the per-priority value is the mean across all matching queries' window samples.

    Returns: {(window_idx, priority): mean_latency_seconds}
    """
    if not os.path.exists(log_path):
        return {}

    events = []
    with open(log_path) as fh:
        for line in fh:
            match = _LATENCY_LINE_RE.search(line)
            if not match:
                continue
            distributed = match.group("dist")
            priority = query_priority.get(distributed)
            if priority is None:
                continue
            multiplier = _LATENCY_UNIT.get(match.group("unit"), 1.0)
            seconds = float(match.group("value")) * multiplier
            events.append((int(match.group("start")), priority, seconds))

    if not events:
        return {}

    # Trim head/tail (see _bin_buffer_ingest_events for rationale).
    t_first_raw_ms = min(ev[0] for ev in events)
    t_last_raw_ms = max(ev[0] for ev in events)
    trim_start_ms = t_first_raw_ms + int(TRIM_HEAD_SEC * 1000)
    trim_end_ms = t_last_raw_ms - int(TRIM_TAIL_SEC * 1000)
    events = [ev for ev in events if trim_start_ms <= ev[0] <= trim_end_ms]
    if not events:
        return {}

    rebase_ms = (reference_ns // 1_000_000) if reference_ns is not None else min(ev[0] for ev in events)
    sums = defaultdict(lambda: [0.0, 0])
    for ts_ms, priority, seconds in events:
        widx = (ts_ms - rebase_ms) // NETWORK_EVENTS_WINDOW_MS
        slot = sums[(widx, priority)]
        slot[0] += seconds
        slot[1] += 1
    return {key: total / count for key, (total, count) in sums.items()}


def _bin_sojourn_events(log_path, query_priority, reference_ns=None):
    """Parse BufferSojourn events (one per successful send) and bin per (window, priority) →
    mean sojourn in seconds. Sojourn = engine-side queueing delay at NetworkSink, NOT wire time.

    Returns: {(window_idx, priority): mean_sojourn_seconds}
    """
    if not os.path.exists(log_path):
        return {}

    events = []  # (event_ns, priority, sojourn_seconds)
    with open(log_path) as fh:
        for line in fh:
            match = _BUFFER_SOJOURN_LINE_RE.search(line)
            if not match:
                continue
            distributed = match.group("dist")
            priority = query_priority.get(distributed)
            if priority is None:
                continue
            sojourn_seconds = int(match.group("sojourn_ns")) / 1_000_000_000.0
            events.append((int(match.group("ns")), priority, sojourn_seconds))

    if not events:
        return {}

    # Same head/tail trim as the throughput/latency binners — drop startup transient and teardown
    # spike (the post-Closed-branch buffers can have very high sojourn that's unrepresentative).
    t_first_raw_ns = min(ev[0] for ev in events)
    t_last_raw_ns = max(ev[0] for ev in events)
    trim_start_ns = t_first_raw_ns + int(TRIM_HEAD_SEC * 1_000_000_000)
    trim_end_ns = t_last_raw_ns - int(TRIM_TAIL_SEC * 1_000_000_000)
    events = [ev for ev in events if trim_start_ns <= ev[0] <= trim_end_ns]
    if not events:
        return {}

    rebase_ns = reference_ns if reference_ns is not None else min(ev[0] for ev in events)
    window_ns = NETWORK_EVENTS_WINDOW_MS * 1_000_000
    sums = defaultdict(lambda: [0.0, 0])  # (widx, priority) → [total_seconds, count]
    for ts_ns, priority, seconds in events:
        widx = (ts_ns - rebase_ns) // window_ns
        slot = sums[(widx, priority)]
        slot[0] += seconds
        slot[1] += 1
    return {key: total / count for key, (total, count) in sums.items()}


def _bin_span_events(log_path, query_priority, line_re, duration_group, reference_ns=None):
    """Shared binner for span-style events (one log line per [start, end] interval). Used for
    BackpressureBlocked (duration_group='blocked_ns') and SchedulerGated (duration_group='gated_ns').
    Each event represents a span; spans are split across the windows they overlap so a single
    multi-window interval contributes the right share to each window.

    Returns: {(window_idx, priority): fraction in [0, 1]}
    """
    if not os.path.exists(log_path):
        return {}

    raw = []  # (end_ns, priority, duration_ns)
    with open(log_path) as fh:
        for line in fh:
            match = line_re.search(line)
            if not match:
                continue
            distributed = match.group("dist")
            priority = query_priority.get(distributed)
            if priority is None:
                continue
            raw.append((int(match.group("ns")), priority, int(match.group(duration_group))))

    if not raw:
        return {}

    # Same head/tail trim as the other binners. We use the event END timestamp (wait-return) as
    # the reference for trim, matching how the sojourn/throughput binners pick their first event.
    t_first_raw_ns = min(ev[0] for ev in raw)
    t_last_raw_ns = max(ev[0] for ev in raw)
    trim_start_ns = t_first_raw_ns + int(TRIM_HEAD_SEC * 1_000_000_000)
    trim_end_ns = t_last_raw_ns - int(TRIM_TAIL_SEC * 1_000_000_000)

    spans = []  # (start_ns, end_ns, priority) — start clamped into the trim window
    for end_ns, priority, duration_ns in raw:
        start_ns = end_ns - duration_ns
        if end_ns < trim_start_ns or start_ns > trim_end_ns:
            continue
        clamped_start = max(start_ns, trim_start_ns)
        clamped_end = min(end_ns, trim_end_ns)
        if clamped_end > clamped_start:
            spans.append((clamped_start, clamped_end, priority))

    if not spans:
        return {}

    window_ns = NETWORK_EVENTS_WINDOW_MS * 1_000_000
    rebase_ns = reference_ns if reference_ns is not None else min(s[0] for s in spans)
    accum = defaultdict(float)  # (widx, priority) → total span ns in this window
    for start_ns, end_ns, priority in spans:
        widx_first = (start_ns - rebase_ns) // window_ns
        widx_last = (end_ns - rebase_ns) // window_ns
        for widx in range(widx_first, widx_last + 1):
            win_start = rebase_ns + widx * window_ns
            win_end = win_start + window_ns
            overlap = min(end_ns, win_end) - max(start_ns, win_start)
            if overlap > 0:
                accum[(widx, priority)] += overlap
    return {key: ns / window_ns for key, ns in accum.items()}


def _bin_blocked_events(log_path, query_priority, reference_ns=None):
    """Source-side BackpressureBlocked spans. See `_bin_span_events`."""
    return _bin_span_events(log_path, query_priority, _BACKPRESSURE_BLOCKED_LINE_RE, "blocked_ns", reference_ns)


def _bin_gated_events(log_path, query_priority, reference_ns=None):
    """Sink-side SchedulerGated spans (AdaptiveSendingScheduler deny+retry loops).
    See `_bin_span_events`."""
    return _bin_span_events(log_path, query_priority, _SCHEDULER_GATED_LINE_RE, "gated_ns", reference_ns)


## ---------------------------------------------------------------------------
## CSV writers.
## ---------------------------------------------------------------------------

def _ts_rows_with_high_only(windows, value_extractor):
    """Yield (window_idx, priority, value, high_only_value) tuples, where high_only_value repeats the
    HIGH value at the same window across both priorities (or '' if that window has no HIGH sample).

    *value_extractor* maps a window dict value to the scalar metric we want to write.
    """
    high_at = {widx: value_extractor(val) for (widx, prio), val in windows.items() if prio == "HIGH"}
    for (widx, priority), val in sorted(windows.items()):
        scalar = value_extractor(val)
        high_only = high_at.get(widx, "")
        yield widx, priority, scalar, high_only


def _sweep_dim_values(cap, cap_tps, high_cfg, low_cfg):
    """Return the sweep-dimension column values matching config._SWEEP_DIM_FIELDS order.

    *cap* is the tc rate string (e.g. "1mbit"); *cap_tps* is its tuples/s equivalent (or None
    for "none"). *high_cfg* is a dict from HIGH_STEP_CONFIGS; *low_cfg* is a dict from
    LOW_EMIT_RATES or None (for HIGH_ALONE rows: low_label='none', low_emit_rate='').
    """
    cap_tps_str = "" if cap_tps is None else f"{cap_tps:.6f}"
    if cap_tps is None:
        # cap == "none": no throttle. Resolve fractional rates against a sentinel value so the
        # absolute rate columns still round-trip; the notebook treats cap=="none" specially.
        # Practical default: 100mbit-equivalent so a frac=1.0 emits at ≈1.5M tup/s, well above
        # what the source can actually push.
        cap_tps_for_rates = 100_000_000 / (TUPLE_SIZE_BYTES * 8)
    else:
        cap_tps_for_rates = cap_tps
    high_high = high_cfg["high_frac"] * cap_tps_for_rates
    high_low = high_cfg["low_frac"] * cap_tps_for_rates
    if low_cfg is None:
        low_label = "none"
        low_rate_str = ""
    else:
        low_label = low_cfg["label"]
        low_rate_str = f"{low_cfg['frac_of_cap'] * cap_tps_for_rates:.6f}"
    return [
        cap,
        cap_tps_str,
        high_cfg["label"],
        low_label,
        f"{high_high:.6f}",
        f"{high_low:.6f}",
        f"{high_cfg['period_high']:.3f}",
        f"{high_cfg['period_low']:.3f}",
        low_rate_str,
    ]


def _append_network_sending_rows(*, dest_path, windows, run_idx, strategy, sweep_dims):
    """Write the post-throttle delivered tuples/s per priority, sourced from worker-1's throughput
    listener. *windows* has the same shape as throughput windows: {(widx, priority): tuples_per_s}.
    """
    if not windows:
        return
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for widx, priority, value, high_only in _ts_rows_with_high_only(windows, lambda v: v):
            runtime_s = (widx * NETWORK_EVENTS_WINDOW_MS) / 1000.0
            writer.writerow([
                run_idx, strategy, *sweep_dims,
                f"{runtime_s:.3f}", priority,
                f"{value:.6f}",
                "" if high_only == "" else f"{high_only:.6f}",
            ])


def _append_throughput_rows(*, dest_path, windows, run_idx, strategy, sweep_dims):
    if not windows:
        return
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for widx, priority, value, high_only in _ts_rows_with_high_only(windows, lambda v: v):
            runtime_s = (widx * NETWORK_EVENTS_WINDOW_MS) / 1000.0
            writer.writerow([
                run_idx, strategy, *sweep_dims,
                f"{runtime_s:.3f}", priority,
                f"{value:.6f}",
                "" if high_only == "" else f"{high_only:.6f}",
            ])


def _append_latency_rows(*, dest_path, windows, run_idx, strategy, sweep_dims):
    if not windows:
        return
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for widx, priority, value, high_only in _ts_rows_with_high_only(windows, lambda v: v):
            runtime_s = (widx * NETWORK_EVENTS_WINDOW_MS) / 1000.0
            writer.writerow([
                run_idx, strategy, *sweep_dims,
                f"{runtime_s:.3f}", priority,
                f"{value:.9f}",
                "" if high_only == "" else f"{high_only:.9f}",
            ])


def _append_sojourn_rows(*, dest_path, windows, run_idx, strategy, sweep_dims):
    """Per-window mean engine-side sojourn (seconds) per priority. Same shape as the latency
    writer — the notebook loads this CSV with `plot_headline` unchanged.
    """
    if not windows:
        return
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for widx, priority, value, high_only in _ts_rows_with_high_only(windows, lambda v: v):
            runtime_s = (widx * NETWORK_EVENTS_WINDOW_MS) / 1000.0
            writer.writerow([
                run_idx, strategy, *sweep_dims,
                f"{runtime_s:.3f}", priority,
                f"{value:.9f}",
                "" if high_only == "" else f"{high_only:.9f}",
            ])


def _append_blocked_rows(*, dest_path, windows, run_idx, strategy, sweep_dims):
    """Per-window fraction of source-blocked time per priority. *windows* is the dict returned by
    `_bin_blocked_events` — values are already in [0, 1] (overlap-split across windows). Same shape
    as the sojourn writer; the notebook loads it with `plot_headline` unchanged.
    """
    if not windows:
        return
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for widx, priority, value, high_only in _ts_rows_with_high_only(windows, lambda v: v):
            runtime_s = (widx * NETWORK_EVENTS_WINDOW_MS) / 1000.0
            writer.writerow([
                run_idx, strategy, *sweep_dims,
                f"{runtime_s:.3f}", priority,
                f"{value:.9f}",
                "" if high_only == "" else f"{high_only:.9f}",
            ])


def _append_gated_rows(*, dest_path, windows, run_idx, strategy, sweep_dims):
    """Per-window fraction of sink-side scheduler-gated time per priority. *windows* is the dict
    returned by `_bin_gated_events` — values are already in [0, 1] (overlap-split across windows).
    Same shape as the blocked writer.
    """
    if not windows:
        return
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for widx, priority, value, high_only in _ts_rows_with_high_only(windows, lambda v: v):
            runtime_s = (widx * NETWORK_EVENTS_WINDOW_MS) / 1000.0
            writer.writerow([
                run_idx, strategy, *sweep_dims,
                f"{runtime_s:.3f}", priority,
                f"{value:.9f}",
                "" if high_only == "" else f"{high_only:.9f}",
            ])


def _append_results_rows(*, dest_path, run_idx, strategy, sweep_dims,
                         submitted_queries, per_query_avg, trial_duration_s):
    """Append one row per submitted query to results_network_sink.csv. submitted_queries is a list of
    (low_instance_id, distributed_name, priority, spawn_offset_s, issue) tuples; HIGH gets
    low_instance_id=0 by convention. *per_query_avg* maps distributed_name → (priority, avg_tup/s)
    from the receiver-side throughput listener.
    """
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for low_instance_id, distributed, priority, spawn_offset_s, issue in submitted_queries:
            avg_per_s = ""
            if distributed in per_query_avg:
                _, avg_per_s_value = per_query_avg[distributed]
                avg_per_s = f"{avg_per_s_value:.6f}"
            writer.writerow([
                run_idx, strategy, *sweep_dims,
                "" if low_instance_id is None else low_instance_id,
                distributed, priority,
                "" if spawn_offset_s is None else f"{spawn_offset_s:.3f}",
                avg_per_s, f"{trial_duration_s:.3f}", issue,
            ])


def _append_spawn_schedule_rows(*, dest_path, run_idx, strategy, cap, high_label, low_label, low_spawns):
    """Append one row per LOW spawn to low_spawn_schedule.csv. low_spawns is a list of
    (low_instance_id, spawn_offset_ms, distributed_name) tuples.
    """
    with open(dest_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        for low_instance_id, spawn_offset_ms, distributed in low_spawns:
            writer.writerow([run_idx, strategy, cap, high_label, low_label,
                             low_instance_id, spawn_offset_ms, distributed])


## ---------------------------------------------------------------------------
## Trial driver.
## ---------------------------------------------------------------------------

_TC_RATE_RE = re.compile(r"^(?P<num>\d+(?:\.\d+)?)(?P<si>[kmg]?)(?P<base>bit|bps)$", re.IGNORECASE)


def _tc_rate_to_bits_per_s(rate):
    """Convert a tc rate string (e.g. '100kbit') to bits/s. None for 'none' or unparseable input."""
    if rate == "none":
        return None
    match = _TC_RATE_RE.fullmatch(rate.strip())
    if not match:
        return None
    multiplier = {"": 1, "k": 1e3, "m": 1e6, "g": 1e9}[match.group("si").lower()]
    bits = float(match.group("num")) * multiplier
    if match.group("base").lower() == "bps":
        bits *= 8
    return bits


def _tc_rate_to_tuples_per_s(rate, tuple_size_bytes=TUPLE_SIZE_BYTES):
    """Approximate tup/s cap derived from the tc rate and the per-tuple schema size."""
    bits = _tc_rate_to_bits_per_s(rate)
    if bits is None:
        return None
    return bits / (tuple_size_bytes * 8)


def run_congestion_trial(*, run_idx, strategy, output_dir, cap, cap_tuples_per_s, high_cfg, low_cfg,
                         num_worker_threads, generator_flush_interval_ms,
                         low_spawn_interval_sec, num_low_queries, high_only_trial_duration_sec,
                         post_spawn_trial_duration_sec,
                         latency_enabled, results_csv_path, network_sending_csv_path,
                         throughput_csv_path, latency_csv_path, sojourn_csv_path,
                         blocked_csv_path, gated_csv_path, spawn_schedule_csv_path):
    """Run one congestion trial for the given (cap, high_cfg, low_cfg, strategy) cell.

    *high_cfg* is a dict from HIGH_STEP_CONFIGS (label, high_frac, low_frac, period_high,
    period_low). *low_cfg* is a dict from LOW_EMIT_RATES (label, frac_of_cap) or None for
    HIGH_ALONE trials. Fractional rates are resolved against *cap_tuples_per_s* (or a 100mbit
    sentinel when cap == 'none'). Appends to all consolidated CSVs.
    """
    is_high_alone = (strategy == "HIGH_ALONE")
    is_weighted_strict = (strategy == "WEIGHTED_STRICT")
    # Bench-level → worker-level strategy. HIGH_ALONE and WEIGHTED_STRICT are bench labels that
    # both map to a worker-side strategy plus a weight override:
    #   HIGH_ALONE       → ALWAYS_SEND on the worker (no LOW spawned anyway).
    #   WEIGHTED_STRICT  → WEIGHTED_PRIO on the worker with weights (1.0, 0.0) — the strict
    #                      limit of HTB; LOW class share = 0 → LOW silenced; HIGH gets the full
    #                      wire. Lets us isolate the source-bp-cycling cost of the old Rust gate
    #                      from the policy itself.
    if is_high_alone:
        sink_strategy = "ALWAYS_SEND"
    elif is_weighted_strict:
        sink_strategy = "WEIGHTED_PRIO"
    else:
        sink_strategy = strategy
    scheduler_high_weight = 1.0 if is_weighted_strict else SCHEDULER_HIGH_WEIGHT
    scheduler_low_weight = 0.0 if is_weighted_strict else SCHEDULER_LOW_WEIGHT
    sweep_dims = _sweep_dim_values(cap, cap_tuples_per_s, high_cfg, low_cfg)

    # Resolve fractional rates in HIGH_STEP_CONFIGS / LOW_EMIT_RATES to absolute tup/s for the
    # GeneratorRate config strings. Match _sweep_dim_values's resolution rules.
    cap_tps_for_rates = (cap_tuples_per_s if cap_tuples_per_s is not None
                         else 100_000_000 / (TUPLE_SIZE_BYTES * 8))
    high_step_high_rate = high_cfg["high_frac"] * cap_tps_for_rates
    high_step_low_rate = high_cfg["low_frac"] * cap_tps_for_rates
    high_step_period_high = high_cfg["period_high"]
    high_step_period_low = high_cfg["period_low"]
    low_emit_rate = (low_cfg["frac_of_cap"] * cap_tps_for_rates) if low_cfg is not None else 0.0
    low_label = "none" if low_cfg is None else low_cfg["label"]

    run_label = f"run{run_idx}_{cap}_{high_cfg['label']}_{low_label}_{strategy}"
    run_dir = os.path.join(output_dir, run_label)
    os.makedirs(run_dir, exist_ok=True)

    high_yaml = os.path.join(run_dir, "high.yaml")
    low_yaml_paths = [os.path.join(run_dir, f"low_{i}.yaml") for i in range(num_low_queries)]

    # HIGH uses STEP — square-wave workload alternating between high_step_high_rate and
    # high_step_low_rate. LOW uses FIXED at low_emit_rate (resolved from the cell's frac_of_cap).
    _render_template(
        os.path.join(QUERY_TEMPLATES_DIR, "HighPriority_Generator.yaml.template"),
        high_yaml,
        {
            "GENERATOR_RATE_TYPE": "STEP",
            "GENERATOR_RATE_CONFIG": (
                f"high_rate {high_step_high_rate:.0f}, low_rate {high_step_low_rate:.0f}, "
                f"period_high {high_step_period_high}, period_low {high_step_period_low}"
            ),
            "FLUSH_INTERVAL_MS": str(generator_flush_interval_ms),
        },
    )
    if not is_high_alone:
        for i, path in enumerate(low_yaml_paths):
            _render_template(
                os.path.join(QUERY_TEMPLATES_DIR, "LowPriority_Generator.yaml.template"),
                path,
                {
                    "GENERATOR_RATE_TYPE": "FIXED",
                    "GENERATOR_RATE_CONFIG": f"emit_rate {low_emit_rate:.0f}",
                    "FLUSH_INTERVAL_MS": str(generator_flush_interval_ms),
                    "LOW_INSTANCE_ID": str(i),
                },
            )

    worker1_log = os.path.join(run_dir, "worker-1.log")
    worker2_log = os.path.join(run_dir, "worker-2.log")
    workers = []
    tc_ctx = {}
    submitted_queries = []   # (low_instance_id, distributed, priority, spawn_offset_s, issue)
    low_spawns = []          # (low_instance_id, spawn_offset_ms, distributed)
    query_priority = {}      # distributed_name → "HIGH" | "LOW"
    trial_start = None
    trial_end = None

    # Resolve scheduler bootstrap capacity: convert this trial's tc cap in tup/s to bytes/sec.
    # WEIGHTED_PRIO needs an operative capacity estimate from tick 0; matching it to the actual
    # tc cap means the scheduler doesn't burn early ticks converging from a stale default.
    scheduler_bootstrap_bps = (
        int(cap_tuples_per_s * TUPLE_SIZE_BYTES) if cap_tuples_per_s is not None
        else SCHEDULER_BOOTSTRAP_CAPACITY_BPS_DEFAULT
    )
    # Resolve scheduler_fixed_capacity_bps. When the config leaves it None, the bench fills with
    # the trial's tc-cap-in-bps so each cap in EXPERIMENT_CAPS gets the matching fixed value.
    # The worker treats 0 as "fall back to bootstrap" (which equals the same value here), so the
    # final cap-bytes is whatever the trial's tc cap dictates regardless of EMA/FIXED mode.
    scheduler_fixed_bps = (
        int(SCHEDULER_FIXED_CAPACITY_BPS) if SCHEDULER_FIXED_CAPACITY_BPS is not None
        else scheduler_bootstrap_bps
    )

    try:
        with step(f"start workers ({sink_strategy} weights={scheduler_high_weight}/{scheduler_low_weight}, "
                  f"cap_mode={SCHEDULER_CAPACITY_MODE} cap_bps={scheduler_fixed_bps}, "
                  f"threads={num_worker_threads})") as info:
            workers.append(_spawn_worker(WORKER_1_GRPC, WORKER_1_DATA, sink_strategy, worker1_log,
                                         latency_enabled, num_worker_threads,
                                         scheduler_bootstrap_bps, scheduler_high_weight,
                                         scheduler_low_weight, scheduler_fixed_bps))
            workers.append(_spawn_worker(WORKER_2_GRPC, WORKER_2_DATA, sink_strategy, worker2_log,
                                         latency_enabled, num_worker_threads,
                                         scheduler_bootstrap_bps, scheduler_high_weight,
                                         scheduler_low_weight, scheduler_fixed_bps))
            time.sleep(WAIT_BETWEEN_COMMANDS_LONG)
            info(f"workers up: 1@{WORKER_1_GRPC} 2@{WORKER_2_GRPC}")

        with step(f"apply tc rate={cap}"):
            _apply_tc_throttle(cap, tc_ctx)

        trial_start = time.time()
        # Single-query mode (NUM_LOW_QUERIES==0) ⇒ HIGH alone for HIGH_ONLY_TRIAL_DURATION_SEC.
        # Otherwise: LOW-0 spawns immediately after HIGH (at t≈0), subsequent LOWs every
        # LOW_SPAWN_INTERVAL_SEC, then a POST_SPAWN_TRIAL_DURATION_SEC settle. (N-1) inter-spawn
        # sleeps + 1 settle = (num_low_queries - 1) * interval + post_spawn.
        if num_low_queries == 0:
            total_trial_seconds = high_only_trial_duration_sec
        else:
            total_trial_seconds = ((num_low_queries - 1) * low_spawn_interval_sec
                                   + post_spawn_trial_duration_sec)

        with step("submit HIGH at t=0") as info:
            high_qid = _submit_query(high_yaml)
            spawn_offset_s = time.time() - trial_start
            if high_qid is None:
                submitted_queries.append((0, "high-submit-failed", "HIGH", spawn_offset_s, "submit-failed"))
                info("HIGH submit failed; aborting trial")
                raise SystemExit(3)
            submitted_queries.append((0, high_qid, "HIGH", spawn_offset_s, ""))
            query_priority[high_qid] = "HIGH"
            info(f"HIGH={high_qid}")

        if is_high_alone or num_low_queries == 0:
            label = "single-query" if num_low_queries == 0 else "HIGH-alone strategy"
            with step(f"{label}: hold HIGH for {total_trial_seconds:.1f}s without spawning LOWs"):
                time.sleep(total_trial_seconds)
        else:
            with step(
                f"spawn {num_low_queries} LOW queries every {low_spawn_interval_sec:.1f}s "
                f"(LOW-0 fires immediately after HIGH; each gets at least "
                f"{low_spawn_interval_sec:.0f}s before the next is added)"
            ) as info:
                for i in range(num_low_queries):
                    low_qid = _submit_query(low_yaml_paths[i])
                    spawn_offset_s = time.time() - trial_start
                    if low_qid is None:
                        submitted_queries.append((i, f"low-{i}-submit-failed", "LOW", spawn_offset_s,
                                                  "submit-failed"))
                        info(f"  LOW{i} submit failed (continuing)")
                    else:
                        submitted_queries.append((i, low_qid, "LOW", spawn_offset_s, ""))
                        query_priority[low_qid] = "LOW"
                        low_spawns.append((i, int(spawn_offset_s * 1000), low_qid))
                        info(f"  LOW{i}={low_qid} at t={spawn_offset_s:.1f}s")
                    if i < num_low_queries - 1:
                        time.sleep(low_spawn_interval_sec)
            with step(f"settle: all queries running, hold for {post_spawn_trial_duration_sec:.1f}s"):
                time.sleep(post_spawn_trial_duration_sec)

        trial_end = time.time()

    finally:
        with step("teardown"):
            _remove_tc_throttle(tc_ctx)
            for proc, log_file in workers:
                terminate_process_if_exists(proc)
                log_file.close()

    if trial_end is None:
        trial_end = time.time()
    trial_duration_s = trial_end - (trial_start if trial_start is not None else trial_end)

    # Cross-trial t=0 reference: pick HIGH's StaircasePhaseStart timestamp, then snap back to the
    # most recent staircase cycle boundary in wall-clock time so trials whose first-emit landed at
    # different within-cycle offsets still align on the staircase phase. Falls back to None for
    # non-staircase trials → binners use the legacy first-event rebase.
    phase_start_ns = _parse_phase_start_ns(worker2_log, query_priority)
    phase_start_ns = _align_to_cycle_boundary(phase_start_ns, high_cfg["period_high"], high_cfg["period_low"])

    # Parse listeners. Throughput + latency from worker-2 (sender side; only firstPipeline=true
    # tasks emit, which on our topology is worker-2 only). Network-sending derived from worker-1's
    # BufferIngest events (post-throttle buffer arrivals) — each event carries the buffer's actual
    # tuple count, so the per-window sum is wire-truth.
    throughput_windows = _bin_throughput_events(worker2_log, query_priority, reference_ns=phase_start_ns)
    latency_windows = _bin_latency_events(worker2_log, query_priority, reference_ns=phase_start_ns) if latency_enabled else {}
    sending_windows, per_query_total_tuples = _bin_buffer_ingest_events(worker1_log, query_priority, reference_ns=phase_start_ns)
    # Engine-side sojourn (per-buffer queueing delay) is emitted by worker-2's NetworkSink.
    sojourn_windows = _bin_sojourn_events(worker2_log, query_priority, reference_ns=phase_start_ns)
    # Source-side blocked time (BackpressureListener::wait duration) — also emitted by worker-2,
    # since BackpressureListener lives on the source side and worker-2 hosts the GeneratorSource.
    blocked_windows = _bin_blocked_events(worker2_log, query_priority, reference_ns=phase_start_ns)
    # Sink-side scheduler-gated time (AdaptiveSendingScheduler deny/retry loops). Emitted by
    # worker-2's NetworkSink at gate pass-through after a deny streak.
    gated_windows = _bin_gated_events(worker2_log, query_priority, reference_ns=phase_start_ns)

    # Per-query average delivered tuples/s for results CSV: total received tuples / trial duration.
    per_query_avg = {}
    if trial_duration_s > 0:
        for distributed, total_tuples in per_query_total_tuples.items():
            per_query_avg[distributed] = (
                query_priority[distributed],
                total_tuples / trial_duration_s,
            )

    _append_network_sending_rows(
        dest_path=network_sending_csv_path,
        windows=sending_windows,
        run_idx=run_idx,
        strategy=strategy,
        sweep_dims=sweep_dims,
    )
    _append_throughput_rows(
        dest_path=throughput_csv_path,
        windows=throughput_windows,
        run_idx=run_idx,
        strategy=strategy,
        sweep_dims=sweep_dims,
    )
    if latency_enabled:
        _append_latency_rows(
            dest_path=latency_csv_path,
            windows=latency_windows,
            run_idx=run_idx,
            strategy=strategy,
            sweep_dims=sweep_dims,
        )
    _append_sojourn_rows(
        dest_path=sojourn_csv_path,
        windows=sojourn_windows,
        run_idx=run_idx,
        strategy=strategy,
        sweep_dims=sweep_dims,
    )
    _append_blocked_rows(
        dest_path=blocked_csv_path,
        windows=blocked_windows,
        run_idx=run_idx,
        strategy=strategy,
        sweep_dims=sweep_dims,
    )
    _append_gated_rows(
        dest_path=gated_csv_path,
        windows=gated_windows,
        run_idx=run_idx,
        strategy=strategy,
        sweep_dims=sweep_dims,
    )
    _append_results_rows(
        dest_path=results_csv_path,
        run_idx=run_idx,
        strategy=strategy,
        sweep_dims=sweep_dims,
        submitted_queries=submitted_queries,
        per_query_avg=per_query_avg,
        trial_duration_s=trial_duration_s,
    )
    _append_spawn_schedule_rows(
        dest_path=spawn_schedule_csv_path,
        run_idx=run_idx,
        strategy=strategy,
        cap=cap,
        high_label=high_cfg["label"],
        low_label=low_label,
        low_spawns=low_spawns,
    )
    return trial_duration_s


## ---------------------------------------------------------------------------
## CLI entry point.
## ---------------------------------------------------------------------------

def _parse_bool_flag(value):
    """Map argparse string values to bool. Accepts true/false/1/0 (case-insensitive)."""
    lowered = str(value).lower()
    if lowered in ("true", "1", "yes", "on"):
        return True
    if lowered in ("false", "0", "no", "off"):
        return False
    raise argparse.ArgumentTypeError(f"expected boolean (true|false), got {value!r}")


def _parse_caps_flag(value):
    """Comma-separated tc rate list (e.g. '500kbit,1mbit,2mbit'). Single value also accepted."""
    return [token.strip() for token in str(value).split(",") if token.strip()]


def _estimate_eta(start_time, now, completed_runs, total_runs):
    elapsed = now - start_time
    avg = elapsed / completed_runs
    eta_seconds = avg * (total_runs - completed_runs)
    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
    eta_h, rem = divmod(eta_seconds, 3600)
    eta_m, eta_s = divmod(rem, 60)
    return int(eta_h), int(eta_m), eta_s, eta_time


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", default=".", help="Where to write the consolidated CSVs and per-run logs")
    parser.add_argument("--num-runs", type=int, default=NUM_RUNS_PER_EXPERIMENT,
                        help="Repetitions of the full (cap × HIGH × LOW × strategy) sweep")
    parser.add_argument("--caps", type=_parse_caps_flag, default=EXPERIMENT_CAPS,
                        help="Comma-separated tc rates to sweep (htb on data ports). "
                             f"Default: {','.join(EXPERIMENT_CAPS)} from config.py:EXPERIMENT_CAPS. "
                             "Pass 'none' to skip throttling.")
    parser.add_argument("--strategies", nargs="+", default=EXPERIMENT_STRATEGIES,
                        help=f"Strategies to compare (default {' '.join(EXPERIMENT_STRATEGIES)}). "
                             "HIGH_ALONE skips LOW spawning while running ALWAYS_SEND on the worker; "
                             "one HIGH_ALONE trial is run per (cap, HIGH staircase config), "
                             "independent of the LOW dimension.")
    parser.add_argument("--num-low-queries", type=int, default=NUM_LOW_QUERIES,
                        help="Exact number of LOW queries spawned per trial. Trial wall-clock is "
                             "(N-1)*low_spawn_interval_sec + post_spawn_trial_duration_sec. The grid "
                             "varies LOW *rate* via LOW_EMIT_RATES; the *count* is fixed at 1 by "
                             "default. Set to 0 for single-query mode.")
    parser.add_argument("--low-spawn-interval-sec", type=float, default=LOW_SPAWN_INTERVAL_SEC,
                        help="Seconds between LOW spawns; also the minimum runtime guaranteed for the "
                             "last LOW before trial teardown.")
    parser.add_argument("--high-only-trial-duration-sec", type=float,
                        default=HIGH_ONLY_TRIAL_DURATION_SEC,
                        help="Trial wall-clock seconds when --num-low-queries=0 (single-query mode).")
    parser.add_argument("--post-spawn-trial-duration-sec", type=float,
                        default=POST_SPAWN_TRIAL_DURATION_SEC,
                        help="Wall-clock seconds the trial keeps running after the last LOW has been "
                             "submitted (steady-state-at-peak-load window before teardown).")
    parser.add_argument("--generator-flush-interval-ms", type=int, default=GENERATOR_FLUSH_INTERVAL_MS,
                        help="Per-call duration of the GeneratorSource's fillTupleBuffer (ms). "
                             "Higher = source paces itself slower (fewer buffers/s, less overproduction "
                             "vs the wire), making the throughput chart smoother.")
    parser.add_argument("--num-worker-threads", type=int, default=NUM_WORKER_THREADS,
                        help=f"--worker.query_engine.number_of_worker_threads forwarded to every "
                             f"spawned single-node-worker. Default %(default)s.")
    parser.add_argument("--latency-listener", type=_parse_bool_flag, default=LATENCY_LISTENER_ENABLED,
                        help="Enable the latency listener on each worker. Default %(default)s.")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip cmake configure + ninja build. Use when binaries are already up to date.")
    parser.add_argument("--clean", action="store_true",
                        help="Remove the build directory before configuring (forces a clean rebuild).")
    args = parser.parse_args()
    # HIGH_STEP_CONFIGS and LOW_EMIT_RATES are not exposed as CLI overrides — edit config.py to
    # change the grid. Per-cell rates are resolved from cap × frac at runtime so the same config
    # file produces the same grid shape at every cap.

    check_repository_root()

    if args.clean:
        create_folder_and_remove_if_exists(BUILD_DIR)

    if not args.skip_build:
        compile_nebulastream(get_cmake_flags(), BUILD_DIR)

    for path in (SINGLE_NODE_EXECUTABLE, NEBULI_EXECUTABLE[0]):
        if not os.path.isfile(path):
            printError(f"Required executable not found: {path}")
            printError("Build NebulaStream first (drop --skip-build), or set NES_BUILD_DIR appropriately.")
            raise SystemExit(1)

    any_throttled = any(cap != "none" for cap in args.caps)
    _preflight_tc_sudo(any_throttled)

    # Validate caps up-front so a typo doesn't surface mid-sweep.
    cap_tuples = {}
    for cap in args.caps:
        cap_tps = _tc_rate_to_tuples_per_s(cap)
        if cap_tps is None and cap != "none":
            printError(f"Unparseable cap value: {cap!r}")
            raise SystemExit(2)
        cap_tuples[cap] = cap_tps

    create_folder_and_remove_if_exists(args.output_dir)
    results_csv_path = os.path.join(args.output_dir, "results_network_sink.csv")
    network_sending_csv_path = os.path.join(args.output_dir, "network_sending_timeseries.csv")
    throughput_csv_path = os.path.join(args.output_dir, "throughput_timeseries.csv")
    latency_csv_path = os.path.join(args.output_dir, "latency_timeseries.csv")
    sojourn_csv_path = os.path.join(args.output_dir, "sojourn_timeseries.csv")
    blocked_csv_path = os.path.join(args.output_dir, "blocked_timeseries.csv")
    gated_csv_path = os.path.join(args.output_dir, "gated_timeseries.csv")
    spawn_schedule_csv_path = os.path.join(args.output_dir, "low_spawn_schedule.csv")
    with open(results_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(RESULT_FIELDNAMES)
    with open(network_sending_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(NETWORK_SENDING_TS_FIELDNAMES)
    with open(throughput_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(THROUGHPUT_TS_FIELDNAMES)
    with open(latency_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(LATENCY_TS_FIELDNAMES)
    with open(sojourn_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(SOJOURN_TS_FIELDNAMES)
    with open(blocked_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(BLOCKED_TS_FIELDNAMES)
    with open(gated_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(GATED_TS_FIELDNAMES)
    with open(spawn_schedule_csv_path, "w", newline="") as fh:
        csv.writer(fh).writerow(LOW_SPAWN_SCHEDULE_FIELDNAMES)

    # Trial count: per (run, cap, HIGH config), one HIGH_ALONE trial (independent of LOW) plus
    # one trial per (LOW config × non-HIGH_ALONE strategy).
    non_alone_strategies = [s for s in args.strategies if s != "HIGH_ALONE"]
    has_high_alone = "HIGH_ALONE" in args.strategies
    trials_per_high_cfg = (1 if has_high_alone else 0) + len(LOW_EMIT_RATES) * len(non_alone_strategies)
    total_runs = args.num_runs * len(args.caps) * len(HIGH_STEP_CONFIGS) * trials_per_high_cfg

    if args.num_low_queries == 0:
        trial_seconds = args.high_only_trial_duration_sec
    else:
        trial_seconds = ((args.num_low_queries - 1) * args.low_spawn_interval_sec
                         + args.post_spawn_trial_duration_sec)
    banner(
        f"network-sink congestion experiment: {total_runs} trials  "
        f"caps={','.join(args.caps)}  strategies={','.join(args.strategies)}  "
        f"high_configs=[{','.join(h['label'] for h in HIGH_STEP_CONFIGS)}]  "
        f"low_rates=[{','.join(l['label'] for l in LOW_EMIT_RATES)}]  "
        f"{trial_seconds:g}s/trial"
    )

    start_time = time.time()
    completed_runs = 0
    for run_idx in range(args.num_runs):
        for cap in args.caps:
            cap_tps = cap_tuples[cap]
            cap_tps_str = "uncapped" if cap_tps is None else f"{cap_tps:.0f} tup/s"
            for high_cfg in HIGH_STEP_CONFIGS:
                # HIGH_ALONE runs once per (cap, HIGH config); independent of the LOW dimension.
                # The notebook joins HIGH_ALONE rows by (cap, high_label) and overlays the trace
                # in every cell of that row in the twin-grid plot.
                if has_high_alone:
                    printInfo(
                        f"--- run {run_idx + 1}/{args.num_runs}  cap={cap} ({cap_tps_str})  "
                        f"high={high_cfg['label']} ({high_cfg['high_frac']:.2f}↔{high_cfg['low_frac']:.2f}×cap)  "
                        f"low=none  strategy=HIGH_ALONE ---"
                    )
                    run_congestion_trial(
                        run_idx=run_idx,
                        strategy="HIGH_ALONE",
                        output_dir=args.output_dir,
                        cap=cap,
                        cap_tuples_per_s=cap_tps,
                        high_cfg=high_cfg,
                        low_cfg=None,
                        num_worker_threads=args.num_worker_threads,
                        generator_flush_interval_ms=args.generator_flush_interval_ms,
                        low_spawn_interval_sec=args.low_spawn_interval_sec,
                        num_low_queries=args.num_low_queries,
                        high_only_trial_duration_sec=args.high_only_trial_duration_sec,
                        post_spawn_trial_duration_sec=args.post_spawn_trial_duration_sec,
                        latency_enabled=args.latency_listener,
                        results_csv_path=results_csv_path,
                        network_sending_csv_path=network_sending_csv_path,
                        throughput_csv_path=throughput_csv_path,
                        latency_csv_path=latency_csv_path,
                        sojourn_csv_path=sojourn_csv_path,
                        blocked_csv_path=blocked_csv_path,
                        gated_csv_path=gated_csv_path,
                        spawn_schedule_csv_path=spawn_schedule_csv_path,
                    )
                    completed_runs += 1
                    with step("done") as info:
                        info(f"{completed_runs}/{total_runs}")
                        if completed_runs < total_runs:
                            eta_h, eta_m, _eta_s, eta_time = _estimate_eta(
                                start_time, time.time(), completed_runs, total_runs)
                            info(f"ETA {eta_h}h {eta_m:02d}m (~{eta_time.strftime('%H:%M:%S')})")
                for low_cfg in LOW_EMIT_RATES:
                    for strategy in non_alone_strategies:
                        printInfo(
                            f"--- run {run_idx + 1}/{args.num_runs}  cap={cap} ({cap_tps_str})  "
                            f"high={high_cfg['label']}  low={low_cfg['label']} "
                            f"({low_cfg['frac_of_cap']:.2f}×cap)  strategy={strategy} ---"
                        )
                        run_congestion_trial(
                            run_idx=run_idx,
                            strategy=strategy,
                            output_dir=args.output_dir,
                            cap=cap,
                            cap_tuples_per_s=cap_tps,
                            high_cfg=high_cfg,
                            low_cfg=low_cfg,
                            num_worker_threads=args.num_worker_threads,
                            generator_flush_interval_ms=args.generator_flush_interval_ms,
                            low_spawn_interval_sec=args.low_spawn_interval_sec,
                            num_low_queries=args.num_low_queries,
                            high_only_trial_duration_sec=args.high_only_trial_duration_sec,
                            post_spawn_trial_duration_sec=args.post_spawn_trial_duration_sec,
                            latency_enabled=args.latency_listener,
                            results_csv_path=results_csv_path,
                            network_sending_csv_path=network_sending_csv_path,
                            throughput_csv_path=throughput_csv_path,
                            latency_csv_path=latency_csv_path,
                            sojourn_csv_path=sojourn_csv_path,
                            blocked_csv_path=blocked_csv_path,
                            gated_csv_path=gated_csv_path,
                            spawn_schedule_csv_path=spawn_schedule_csv_path,
                        )
                        completed_runs += 1
                        with step("done") as info:
                            info(f"{completed_runs}/{total_runs}")
                            if completed_runs < total_runs:
                                eta_h, eta_m, _eta_s, eta_time = _estimate_eta(
                                    start_time, time.time(), completed_runs, total_runs)
                                info(f"ETA {eta_h}h {eta_m:02d}m (~{eta_time.strftime('%H:%M:%S')})")

    elapsed = time.time() - start_time
    h, rem = divmod(elapsed, 3600)
    m, s = divmod(rem, 60)
    printInfo(f"\nDone in {int(h)}h {int(m)}m {s:.1f}s")
    printSuccess(f"Wrote {os.path.abspath(results_csv_path)}")
    printSuccess(f"Wrote {os.path.abspath(network_sending_csv_path)}")
    printSuccess(f"Wrote {os.path.abspath(throughput_csv_path)}")
    printSuccess(f"Wrote {os.path.abspath(latency_csv_path)}")
    printSuccess(f"Wrote {os.path.abspath(sojourn_csv_path)}")
    printSuccess(f"Wrote {os.path.abspath(blocked_csv_path)}")
    printSuccess(f"Wrote {os.path.abspath(gated_csv_path)}")
    printSuccess(f"Wrote {os.path.abspath(spawn_schedule_csv_path)}")


if __name__ == "__main__":
    main()
