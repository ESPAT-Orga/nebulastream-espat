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

"""Throughput benchmark for histogram delta compression.

Compares the throughput of a plain EquiWidthHistogram build against the
delta-compressed GEN/RESOLVER split (``EQUIWIDTHHISTOGRAMDELTA``), sweeping
**worker threads** and the **keyframe interval N**. Single-node: this measures
the CPU/throughput cost of the delta machinery and its multi-thread scaling,
NOT the network byte savings (those come from the distributed/ sub-suite).

Two throughput metrics are recorded per run, because they measure different things:

``tuples_per_second`` (primary) is the engine's own throughput listener, the same
source the other suites here use. It is emitted on **stdout** by
``SingleNodeWorker.cpp`` -- not through the NES logger -- so unlike the wall-clock
metric below it survives any ``NES_LOG_LEVEL``. It reports the emit rate of the
first pipeline after the source (``ThroughputListener.cpp``: ``taskEmit.firstPipeline``),
i.e. INGEST rate. Downstream cost -- notably the delta RESOLVER, which lives in a
later pipeline -- reaches it only as backpressure on the source.

``tuples_per_second_walltime`` is input rows divided by the source-to-sink duration
(see parse_query_durations_ms): end-to-end, covering every pipeline through to EOS,
but dependent on two log lines a build can compile out. Compile time is excluded from
both, since the source only starts once the pipeline is compiled.

Keep an eye on the two disagreeing: that is the signature of the RESOLVER costing
something the ingest-side listener cannot see on its own.

Run from the repo root (inside the benchmark venv):
    myenv/bin/python3 -m scripts.benchmarking.histogram_delta.run_histogram_delta_throughput \
        --output-dir <dir> [--smoke]
"""

import argparse
import os
import re
import statistics
import subprocess
import time

from scripts.benchmarking.common.config import BUILD_DIR, SINGLE_NODE_EXECUTABLE, WORKING_DIR
from scripts.benchmarking.common.worker_lifecycle import (
    parse_average_throughput_from_throughput_listener,
    start_single_node_worker,
    submit_query,
    wait_for_query_to_finish,
    terminate_process_if_exists,
)

QUERY_DIR = os.path.join(os.path.dirname(__file__), "queries")

# Fixed worker knobs. Buffer pool kept modest (~800 MB = 8192 * 100000) rather than the
# statistic_build_probe big-machine default (100KiB * 200k = ~20 GB) so a stray worker can't OOM the box;
# streaming recycles buffers, so this does not bottleneck throughput.
EXECUTION_MODE = "COMPILER"
JOIN_STRATEGY = "HASH_JOIN"
BUFFER_SIZE_BYTES = 8192
BUFFERS_GBM = 100 * 1000
PAGE_SIZE = 8192

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_TS_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.(\d+)\]")


def _ts_ms(match):
    h, m, s, frac = match.groups()
    return (int(h) * 3600 + int(m) * 60 + int(s)) * 1000 + int(frac[:3].ljust(3, "0"))


def parse_query_durations_ms(log_path):
    """Per-query processing durations (ms) parsed from a worker log, in order.

    Duration = "Starting source with originId" -> "Void Sink completed" (source-to-sink), which measures
    the actual streaming time and EXCLUDES query compile time (the source only starts after the pipeline
    is compiled). Queries run sequentially on the worker, so the i-th source-start pairs with the i-th
    sink-complete.

    Recorded as a cross-check alongside the throughput listener, not instead of it: this covers the whole
    chain through to EOS (the listener only sees the first pipeline after the source), but it depends on
    two log lines that a build with NES_LOG_LEVEL above DEBUG compiles out -- see diagnose_empty_parse.
    """
    starts, ends = [], []
    try:
        with open(log_path, "r") as f:
            for raw in f:
                line = _ANSI_RE.sub("", raw)
                if "Starting source with originId" in line:
                    m = _TS_RE.search(line)
                    if m:
                        starts.append(_ts_ms(m))
                elif "Void Sink completed" in line:
                    m = _TS_RE.search(line)
                    if m:
                        ends.append(_ts_ms(m))
    except FileNotFoundError:
        return []
    return [e - s for s, e in zip(starts, ends)]


def diagnose_empty_parse(log_path):
    """Explain a zero-duration parse, which otherwise looks identical to a failed benchmark.

    The usual cause is the BUILD, not the run: both markers are compiled out unless the build's
    NES_LOG_LEVEL admits them ("Starting source with originId" is NES_DEBUG, "Void Sink completed" is
    NES_INFO). NES_LOG_LEVEL is a COMPILE-time flag -- SingleNodeWorkerStarter hardcodes the runtime
    level to LOG_DEBUG, so there is nothing to pass at launch. A Release (defaults to ERROR) or
    Benchmark (LEVEL_NONE) build parses as zero here even though every query ran perfectly.
    """
    try:
        with open(log_path, "r") as f:
            log = _ANSI_RE.sub("", f.read())
    except FileNotFoundError:
        return f"no worker log at {log_path} -- the worker never started."
    if not log.strip():
        return f"worker log {log_path} is empty -- the worker never started."
    if "[D]" not in log:
        return (f"worker log {log_path} contains no DEBUG lines, so the build at BUILD_DIR was compiled "
                f"with NES_LOG_LEVEL above DEBUG and both marker lines are absent from the binary. "
                f"Reconfigure that build with -DNES_LOG_LEVEL:STRING=DEBUG, or point NES_BUILD_DIR at a "
                f"build that already has it.")
    if "Starting source with originId" not in log:
        return f"worker log {log_path} has DEBUG lines but no source start -- the query never began streaming."
    return f"worker log {log_path} has source starts but no 'Void Sink completed' -- queries did not reach EOS."


def hms(seconds):
    """Compact duration: 2h05m / 7m12s / 43s."""
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"


def render_query(variant, run_dir, statistic_id, data_path, memory_budget, window_size, min_value, max_value):
    template = open(os.path.join(QUERY_DIR, f"{variant}.yaml.template")).read()
    yaml_str = template.format(
        statistic_id=statistic_id, memory_budget=memory_budget, min_value=min_value,
        max_value=max_value, window_size=window_size, data_path=data_path)
    yaml_path = os.path.join(run_dir, f"{variant}_{statistic_id}.yaml")
    with open(yaml_path, "w") as f:
        f.write(yaml_str)
    return yaml_path


def kill_stray_workers():
    # The harness pkill fallback uses `-x` which misses the >15-char process name; match the full path.
    subprocess.run(["pkill", "-9", "-f", "nes-single-node-worker/nes-single-node-worker"],
                   capture_output=True)
    time.sleep(2)


def run_config(variant, threads, keyframe_interval, *, run_dir, log_dir, cli_log, data_path, rows,
               memory_budget, window_size, min_value, max_value, runs, warmup, stat_id_counter):
    """Boot one worker for this (variant, threads, N), run warmup+runs queries, return list of throughputs (tuples/s)."""
    extra = ""
    if variant == "delta":
        extra = f"--worker.default_query_execution.histogram_delta_keyframe_interval={keyframe_interval}"

    tag = f"{variant}_t{threads}_N{keyframe_interval}"
    worker_log = os.path.join(log_dir, f"worker_{tag}.log")
    total = warmup + runs
    worker_process = None
    run_qids = []  # per-run query ids, used to attribute listener samples to the right run
    try:
        with open(worker_log, "w") as wlog:
            worker_process = start_single_node_worker(
                wlog, threads, EXECUTION_MODE, JOIN_STRATEGY, PAGE_SIZE,
                BUFFER_SIZE_BYTES, BUFFERS_GBM, cli_log_file=cli_log,
                extra_worker_args=extra)
            time.sleep(5)
            for r in range(total):
                stat_id_counter[0] += 1
                sid = stat_id_counter[0]
                qfile = render_query(variant, run_dir, sid, data_path, memory_budget,
                                     window_size, min_value, max_value)
                qids = submit_query(qfile, cli_log)
                run_qids.append(qids)
                ok, reason = wait_for_query_to_finish(qids, qfile, max_wait=300,
                                                      worker_process=worker_process)
                if not ok:
                    print(f"    [{tag}] run {r}: query did not finish ({reason})")
                time.sleep(0.3)  # let the sink-complete line flush
    finally:
        # Cleanup has to survive a Ctrl-C landing INSIDE it. terminate_process_if_exists() blocks in
        # process.wait(), so an interrupt there would otherwise skip kill_stray_workers() and leave a
        # worker holding 8080/9090 -- which makes the NEXT run fail at startup with a misleading
        # "Worker process exited immediately with code 1".
        try:
            if worker_process is not None:
                terminate_process_if_exists(worker_process)
        finally:
            kill_stray_workers()

    # Primary metric: the engine's throughput listener, attributed per run by query id (queries run
    # sequentially, but matching on the id avoids relying on that). -1 means "nothing parsed".
    listener = [parse_average_throughput_from_throughput_listener(worker_log, qids) for qids in run_qids]
    # Cross-check: end-to-end wall clock. The i-th query maps to the i-th (source-start -> sink-complete)
    # pair, since queries run sequentially on this worker.
    durations = parse_query_durations_ms(worker_log)

    results = []
    for r in range(total):
        lis = listener[r] if r < len(listener) and listener[r] > 0 else None
        wall = rows / (durations[r] / 1000.0) if r < len(durations) else None
        if lis is None and wall is None:
            continue
        is_warmup = r < warmup
        shown = " ".join(filter(None, [
            f"listener {lis/1e6:.3f} M/s" if lis else None,
            f"walltime {wall/1e6:.3f} M/s ({durations[r]} ms)" if wall else None]))
        print(f"    [{tag}] run {r}{' (warmup)' if is_warmup else ''}: {shown}")
        if not is_warmup:
            results.append((lis if lis else wall, wall))

    if len(durations) < total:
        # Not fatal any more: the listener is on stdout and survives any NES_LOG_LEVEL, so the run is
        # still measured. Only the end-to-end cross-check column is lost.
        print(f"    [{tag}] NOTE: only {len(durations)}/{total} wall-clock durations parsed")
        if not durations:
            print(f"    [{tag}] cause: {diagnose_empty_parse(worker_log)}")
    if not any(t > 0 for t in listener):
        print(f"    [{tag}] WARNING: no throughput-listener samples for any run -- falling back to wall clock")
    return results


def _report_progress(done, total, started):
    """One line per finished config: how far in, how long so far, roughly how long left."""
    elapsed = time.monotonic() - started
    if done >= total:
        print(f"   [{done}/{total}] done, {hms(elapsed)} elapsed")
        return
    eta = elapsed / done * (total - done)
    print(f"   [{done}/{total}] {hms(elapsed)} elapsed, ~{hms(eta)} left "
          f"({hms(elapsed / done)}/config)")


def main():
    ap = argparse.ArgumentParser(description="Histogram delta-compression throughput benchmark.")
    ap.add_argument("--output-dir", required=True, help="run directory for logs + results CSV")
    ap.add_argument("--data", default=os.path.join(WORKING_DIR, "cluster_monitoring.csv"),
                    help="input CSV (value,timestamp); prepared from the real ClusterMonitoring trace if missing")
    ap.add_argument("--synthetic", action="store_true",
                    help="use the synthetic uniform-random generator instead of the real ClusterMonitoring trace")
    ap.add_argument("--rows", type=int, default=5_000_000,
                    help="rows to generate if --synthetic and --data missing (default 5M)")
    ap.add_argument("--threads", type=int, nargs="+", default=[1, 2, 4, 8, 16])
    ap.add_argument("--keyframe-intervals", type=int, nargs="+", default=[1, 2, 5, 10, 50])
    ap.add_argument("--runs", type=int, default=3, help="measured runs per config")
    ap.add_argument("--warmup", type=int, default=1, help="warmup runs per config (discarded)")
    # 60 s: the real ClusterMonitoring trace is sparse at 1 s (~29 k empty windows) but holds ~3 k tuples
    # per 60 s window. The synthetic generator is dense at any size, so --synthetic can use 1.
    ap.add_argument("--window-size", type=int, default=60, help="tumbling window size in seconds")
    ap.add_argument("--memory-budget", type=int, default=2408, help="histogram byte budget => (budget-8)/24 bins")
    ap.add_argument("--min-value", type=int, default=0)
    # Default matches the real taskId range [0, 20009]; override for other fields/datasets.
    ap.add_argument("--max-value", type=int, default=20009)
    ap.add_argument("--smoke", action="store_true",
                    help="shape-check sweep: threads=[1,4,16], N=[2,10,50], 1 run, no warmup. Every "
                         "figure gets a real multi-point shape from 12 queries instead of 120 -- for "
                         "checking the pipeline works and the plots look right, NOT for results")
    args = ap.parse_args()

    if args.smoke:
        # Three thread counts and three N values on purpose: with a single thread count the
        # throughput-vs-threads and overhead-vs-threads figures degenerate to one point, which tells
        # you nothing about whether the plots are right. 12 queries instead of the full sweep's 120.
        args.threads = [1, 4, 16]
        args.keyframe_intervals = [2, 10, 50]
        args.runs = 1
        args.warmup = 0

    # Preflight the binary. Without this every config fails identically with "Worker process exited
    # immediately with code 1", which reads like an engine bug rather than a missing build -- and the
    # default BUILD_DIR (./build_dir) sits inside the rsync-synced source tree, where an IDE sync can
    # wipe it between runs. Cheap check, saves a full sweep of identical failures.
    if not os.path.isfile(SINGLE_NODE_EXECUTABLE):
        raise SystemExit(
            f"No worker binary at {os.path.abspath(SINGLE_NODE_EXECUTABLE)}.\n"
            f"Build it, or point NES_BUILD_DIR at a build that has one -- note it must be configured "
            f"with -DNES_LOG_LEVEL:STRING=DEBUG only if you also want the wall-clock cross-check column; "
            f"the throughput listener works at any log level.")

    os.makedirs(args.output_dir, exist_ok=True)
    log_dir = os.path.join(args.output_dir, "worker_logs")
    os.makedirs(log_dir, exist_ok=True)

    # Ensure input data exists.
    if not os.path.exists(args.data):
        os.makedirs(os.path.dirname(args.data), exist_ok=True)
        if args.synthetic:
            print(f"Generating {args.rows} synthetic rows -> {args.data}")
            from scripts.benchmarking.histogram_delta.gen_data import generate
            generate(args.data, args.rows, rows_per_window=1000, value_range=args.max_value)
        else:
            print(f"Preparing real ClusterMonitoring trace -> {args.data}")
            from scripts.benchmarking.histogram_delta.prepare_cluster_monitoring import prepare
            prepare(args.data)

    # Exact input tuple count (throughput = rows / source-to-sink time).
    with open(args.data, "rb") as f:
        rows = sum(buf.count(b"\n") for buf in iter(lambda: f.read(1 << 20), b""))
    print(f"Input: {args.data} ({rows} rows)")

    csv_path = os.path.join(args.output_dir, "results_histogram_delta_throughput.csv")
    stat_id_counter = [0]
    kill_stray_workers()

    with open(os.path.join(args.output_dir, "cli_commands.log"), "w") as cli_log, \
         open(csv_path, "w") as csv_f:
        csv_f.write("variant,threads,keyframe_interval,run,tuples_per_second,tuples_per_second_walltime\n")
        csv_f.flush()
        # plain: keyframe interval is irrelevant -> a single sentinel value.
        plan = [("plain", t, 0) for t in args.threads] + \
               [("delta", t, n) for t in args.threads for n in args.keyframe_intervals]
        # Progress + ETA. The sweep is long (each config boots a worker, JIT-compiles, then runs
        # warmup+runs queries over the whole trace), so print where we are and roughly how much is
        # left. The estimate is a running mean over completed configs: it is rough early on, because
        # a 1-thread config takes several times longer than a 16-thread one and the plan interleaves
        # them, but it converges quickly and is far better than no signal at all.
        started = time.monotonic()
        total_configs = len(plan)
        total_queries = total_configs * (args.warmup + args.runs)
        print(f"\nSweep: {total_configs} configs x {args.warmup + args.runs} queries "
              f"({args.warmup} warmup + {args.runs} measured) = {total_queries} queries over "
              f"{rows} rows each.\n")

        for idx, (variant, threads, N) in enumerate(plan, start=1):
            print(f"== [{idx}/{total_configs}] {variant} threads={threads} N={N} ==")
            try:
                tps_list = run_config(
                    variant, threads, N, run_dir=args.output_dir, log_dir=log_dir, cli_log=cli_log,
                    data_path=args.data, rows=rows, memory_budget=args.memory_budget, window_size=args.window_size,
                    min_value=args.min_value, max_value=args.max_value, runs=args.runs, warmup=args.warmup,
                    stat_id_counter=stat_id_counter)
            except Exception as exc:  # one bad config (e.g. a slow compile) must not abort the whole sweep
                print(f"   !! {variant} t{threads} N{N} FAILED: {exc}")
                kill_stray_workers()
                _report_progress(idx, total_configs, started)
                continue
            for i, (tps, wall) in enumerate(tps_list):
                wall_col = f"{wall:.1f}" if wall else ""
                csv_f.write(f"{variant},{threads},{N},{i},{tps:.1f},{wall_col}\n")
            csv_f.flush()
            if tps_list:
                # tps_list holds (primary, walltime) pairs; summarise the primary metric.
                median = statistics.median(t for t, _ in tps_list)
                print(f"   -> median {median/1e6:.3f} MTup/s over {len(tps_list)} runs")
            _report_progress(idx, total_configs, started)

    print(f"\nResults written to {csv_path} (total {hms(time.monotonic() - started)})")


if __name__ == "__main__":
    main()
