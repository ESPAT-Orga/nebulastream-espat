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

"""
Adaptive optimization benchmark.

Starts a nes-single-node-worker and nes-repl (distributed mode) as separate processes,
deploys a query, lets it run for a configurable duration, then tears everything down.

Usage (run from repository root):
    python -m scripts.benchmarking.adaptive-optimization.run_adaptive_optimization_benchmark
    python -m scripts.benchmarking.adaptive-optimization.run_adaptive_optimization_benchmark --duration 120
    python -m scripts.benchmarking.adaptive-optimization.run_adaptive_optimization_benchmark --clean
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    convert_unit_prefix,
    create_folder_and_remove_if_exists,
    get_vcpkg_dir,
    printError,
    printInfo,
    printSuccess,
)

# generate_bid_data.py sits in this same directory; the dotted hyphenated path
# `scripts.benchmarking.adaptive-optimization.generate_bid_data` cannot be imported, so we
# add the local directory to sys.path and import by short name.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_bid_data import DEFAULT_OUTPUT_A, DEFAULT_OUTPUT_B, ensure_dataset_a, ensure_dataset_b

# Number of regime changes (A↔B file switches) we want per run. The per-file wall-clock duration
# is derived from this and the run duration: N changes split the run into N+1 equal segments, so
# MILLIS_PER_FILE = duration / (N+1). Keeping this small (4) keeps the time-series plots uncluttered
# while still exercising several adaptive swaps. Wall-clock based (not replay-count based) so every
# regime lasts the same duration regardless of throughput — with replay counting a faster regime
# cycles sooner, so the periodic throughput slumps would drift apart in the plots.
REGIME_CHANGES_PER_RUN = 4


def millis_per_file_for(duration_s: int) -> int:
    """Per-file wall-clock budget (ms) that yields REGIME_CHANGES_PER_RUN switches over the run.

    N changes carve the run into N+1 equal segments (start in A, switch at duration/(N+1),
    2*duration/(N+1), …), so each file plays for duration / (N+1)."""
    return max(1, int(duration_s * 1000 / (REGIME_CHANGES_PER_RUN + 1)))

#### Build Configuration
build_dir = os.path.join(".", "build_dir")

cmake_flags = (
    "-G Ninja "
    "-DCMAKE_BUILD_TYPE=RelWithDebInfo "
    f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
    "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
    "-DNES_BUILD_NATIVE:BOOL=ON"
)

#### Binary Paths
worker_binary = os.path.join(build_dir, "nes-single-node-worker", "nes-single-node-worker")
repl_binary = os.path.join(build_dir, "nes-frontend", "apps", "nes-repl")

#### Worker / REPL Addresses
WORKER_GRPC = "localhost:8080"
WORKER_DATA = "localhost:9090"

#### Prometheus-baseline addresses
# The PrometheusSink's exposer binds this address on the worker; the external Prometheus scrapes
# it and (later) the coordinator polls Prometheus. For the single-node bench the worker is local,
# so we scrape it directly at localhost for validation.
PROM_SINK_BIND = "0.0.0.0:9464"
PROM_SCRAPE_URL = "http://localhost:9464/metrics"
PROM_SINK_TARGET = "localhost:9464"
# Prometheus server's own web/API listen address. NOT 9090 — that collides with the worker's
# data_address. The coordinator poll loop (and our validation) query this for PromQL results.
PROM_WEB_BIND = "0.0.0.0:9595"
PROM_QUERY_BASE = "http://localhost:9595"

#### Query to deploy (nexmark bid-like schema, LoopingMemory source with LOOP)
#
# Two filters are applied; fields are independent. Selectivity is set by construction in
# generate_bid_data.py (each field is two tight clusters straddling its threshold, with an
# EXACT fraction in the low/pass cluster) — not via a distribution tail — so the realized
# pass rates hit the targets precisely with a sharp boundary:
#   bidValue < 10.45  →  selectivity 0.01 (selective)
#   price    < 888.49 →  selectivity 0.99 (non-selective)
#   Combined selectivity (AND): 0.01 * 0.99 = 0.0099
#
def _expensive_filter_clause(sqrts: int) -> str:
    """Per-tuple SQRT chain summed > 0 (always passes). Empty for sqrts <= 0.

    The terms are summed as a BALANCED tree (parenthesized pairwise) rather than a flat
    left-associative chain, so the parsed expression has depth ~log2(sqrts) instead of
    ~sqrts. A deep chain makes parsing and every subsequent recursive AST traversal during
    query submission blow up; the balanced form is shallow while keeping the exact same
    per-tuple cost (same SQRT/+ op count). The JIT must still compile `sqrts` operations,
    so submission time grows with the count regardless — keep `sqrts` moderate.
    """
    printInfo(f"Sqrts: {sqrts}")
    if sqrts <= 0:
        return ""
    terms = [
        f"SQRT({'bidValue' if i % 2 == 0 else 'price'} + FLOAT64({1000 + i // 2}))"
        for i in range(sqrts)
    ]
    while len(terms) > 1:
        terms = [
            f"({terms[i]} + {terms[i + 1]})" if i + 1 < len(terms) else terms[i]
            for i in range(0, len(terms), 2)
        ]
    return f"{terms[0]} > FLOAT64(0.0)"


def make_data_select_sql(variant: str, sqrts: int) -> str:
    """SELECT block for the data query in the requested filter order.

    variant="bid_first":  bidValue first, then price (the original adaptive default).
    variant="price_first": price first, then bidValue (the swap target).

    The expensive SQRT clause, when present, is sandwiched between the two filters in both
    variants so per-tuple cost is comparable. The variant determines only which filter is
    more selective on the upstream side and therefore "wins" on that regime's distribution.
    """
    expensive = _expensive_filter_clause(sqrts)
    if variant == "price_first":
        first_filter = "price < FLOAT64(888.49)"
        second_filter = "bidValue < FLOAT64(10.45)"
    else:
        first_filter = "bidValue < FLOAT64(10.45)"
        second_filter = "price < FLOAT64(888.49)"
    if expensive:
        return f"""\
SELECT timestamp, auctionId, bidValue, price
FROM (
  SELECT timestamp, auctionId, bidValue, price
  FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE {first_filter})
  WHERE {expensive}
)
WHERE {second_filter}
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""
    return f"""\
SELECT timestamp, auctionId, bidValue, price
FROM (SELECT timestamp, auctionId, bidValue, price FROM bid WHERE {first_filter})
WHERE {second_filter}
INTO someSink
SET (FALSE as `QUERY`.FUSE);"""


def make_setup_sql(
    data_path_a: str,
    data_path_b: str,
    sqrts: int,
    constant_workload: bool,
    variant: str = "bid_first",
    millis_per_file: int = millis_per_file_for(120),
) -> str:
    select_block = make_data_select_sql(variant, sqrts)
    # With --constant-workload, only regime A is loaded and looped indefinitely. With both,
    # the source alternates between A and B every MILLIS_PER_FILE wall-clock milliseconds to
    # simulate a workload-distribution shift on a fixed-duration schedule.
    if constant_workload:
        source_set_clause = f"""\
    'NATIVE' as PARSER.`TYPE`,
    '{data_path_a}' AS `SOURCE`.FILE_PATH,
    'timestamp' AS `SOURCE`.MONOTONIC_TIMESTAMP_FIELD,
    'true' AS `SOURCE`.LOOP,
    '{WORKER_GRPC}' AS `SOURCE`.HOST"""
    else:
        source_set_clause = f"""\
    'NATIVE' as PARSER.`TYPE`,
    '{data_path_a}' AS `SOURCE`.FILE_PATH,
    '{data_path_b}' AS `SOURCE`.FILE_PATH_2,
    '{millis_per_file}' AS `SOURCE`.MILLIS_PER_FILE,
    'timestamp' AS `SOURCE`.MONOTONIC_TIMESTAMP_FIELD,
    'true' AS `SOURCE`.LOOP,
    '{WORKER_GRPC}' AS `SOURCE`.HOST"""
    return f"""\
CREATE WORKER "{WORKER_GRPC}" SET ('{WORKER_DATA}' AS DATA);
CREATE LOGICAL SOURCE bid(timestamp UINT64 NOT NULL, auctionId INT32 NOT NULL, bidValue FLOAT64 NOT NULL, price FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR bid
TYPE LoopingMemory
SET(
{source_set_clause}
);
CREATE SINK someSink(BID.TIMESTAMP UINT64 NOT NULL, BID.AUCTIONID INT32 NOT NULL, BID.BIDVALUE FLOAT64 NOT NULL, BID.PRICE FLOAT64 NOT NULL)
TYPE File
SET(
    'out.csv' as `SINK`.FILE_PATH,
    'CSV' as `SINK`.OUTPUT_FORMAT,
    '{WORKER_GRPC}' AS `SINK`.HOST
);
{select_block}
"""


def make_reversed_query_sql(sqrts: int) -> str:
    """Filter-reversed alternate (price first); used by adaptive mode as --companion-switch-to-sql.
    Delegates to make_data_select_sql so the bid-first / price-first / adaptive paths stay in
    sync as the query shape evolves."""
    return make_data_select_sql("price_first", sqrts)

# Matches: Throughput for queryId QueryId(local=<UUID>, distributed=<horse-name>) in window <ts>-<ts> is <val> <prefix>Tup/s
_THROUGHPUT_RE = re.compile(
    r"Throughput for queryId QueryId\(local=[^,)]+, distributed=([^)]+)\)"
    r" in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s"
)

# Matches: [{"query_id": "<horse-name>"}]  (initial SELECT deployed via REPL stdin → JSON output path)
_JSON_QUERY_ID_RE = re.compile(r'^\[{"query_id":\s*"([^"]+)"}]')

# Matches: [AdaptiveOpt] Deployed query (id=<horse-name>).  (swap callback → plain-text output path)
_ADAPTIVE_QUERY_ID_RE = re.compile(r'\[AdaptiveOpt\] Deployed query \(id=([^)]+)\)')


def stream_output(proc, label, lines_out):
    """Read lines from a process stdout/stderr and print them with a label prefix."""
    for line in iter(proc.stdout.readline, b""):
        decoded = line.decode(errors="replace").rstrip()
        print(f"[{label}] {decoded}", flush=True)
        lines_out.append(decoded)


def wait_for_port(host, port, timeout=10.0, interval=0.2):
    """Poll until a TCP port accepts connections or timeout is reached."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=interval):
                return True
        except OSError:
            time.sleep(interval)
    return False


def scrape_and_validate_prometheus_sink(metrics_url, retries=15, interval=2.0):
    """Scrape the PrometheusSink's /metrics endpoint and confirm it built a populated histogram.

    Returns (ok, metrics_text, total_observations). `total_observations` is the max over all
    histogram families of the +Inf bucket count (= total values Observe()'d). We retry because the
    spliced build branch needs a moment after deploy to start emitting into the sink.
    """
    last_text = ""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(metrics_url, timeout=5) as resp:
                last_text = resp.read().decode("utf-8", errors="replace")
        except Exception as e:  # connection refused until the exposer binds at sink start()
            printInfo(f"  scrape {attempt + 1}/{retries}: {metrics_url} not ready ({e})")
            time.sleep(interval)
            continue
        totals = [int(m) for m in re.findall(r'le="\+Inf"\}\s+(\d+)', last_text)]
        if totals and max(totals) > 0:
            return True, last_text, max(totals)
        printInfo(f"  scrape {attempt + 1}/{retries}: exposer up, no observations yet (totals={totals})")
        time.sleep(interval)
    return False, last_text, 0


def resolve_prometheus_binary():
    """Resolve (download + cache on first use) the prometheus binary via scripts/install-prometheus.sh.

    Returns the absolute path, or None on failure. The script echoes the path on stdout.
    """
    try:
        result = subprocess.run(
            ["bash", os.path.join("scripts", "install-prometheus.sh")],
            capture_output=True, text=True, timeout=600,
        )
    except Exception as e:
        printError(f"Failed to run install-prometheus.sh: {e}")
        return None
    if result.returncode != 0:
        printError(f"install-prometheus.sh failed: {result.stderr.strip() or result.stdout.strip()}")
        return None
    path = (result.stdout.strip().splitlines() or [""])[-1].strip()
    if not path or not os.path.isfile(path):
        printError(f"install-prometheus.sh did not return a valid binary path (got: {path!r})")
        return None
    return path


def write_prometheus_scrape_config(path, target, scrape_interval="1s"):
    """Write a minimal Prometheus config scraping the PrometheusSink exposer at `target`."""
    cfg = (
        "global:\n"
        f"  scrape_interval: {scrape_interval}\n"
        f"  evaluation_interval: {scrape_interval}\n"
        "\n"
        "scrape_configs:\n"
        "  - job_name: nes-prometheus-sink\n"
        "    static_configs:\n"
        f'      - targets: ["{target}"]\n'
    )
    with open(path, "w") as f:
        f.write(cfg)
    return path


def prometheus_scalar(promql, base=PROM_QUERY_BASE, timeout=5):
    """Run an instant PromQL query and return the first result's value as float, or None."""
    url = f"{base}/api/v1/query?query=" + urllib.parse.quote(promql)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = json.load(resp)
    except Exception as e:
        printInfo(f"  PromQL query failed ({promql}): {e}")
        return None
    if data.get("status") != "success":
        printInfo(f"  PromQL non-success for ({promql}): {data.get('status')}")
        return None
    result = data.get("data", {}).get("result", [])
    if not result:
        return None
    value = result[0].get("value", [None, None])[1]
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def launch_and_validate_prometheus(worker_lines_label="PROM"):
    """Launch Prometheus scraping the sink, wait for scrapes, and run a PromQL histogram_quantile
    to confirm the metrics pipeline works end-to-end. Returns the Prometheus process (to tear down)
    or None if it could not be started.
    """
    prom_bin = resolve_prometheus_binary()
    if not prom_bin:
        printError("Could not resolve the prometheus binary; skipping Prometheus-server validation.")
        return None

    here = os.path.dirname(os.path.abspath(__file__))
    cfg_path = write_prometheus_scrape_config(os.path.join(here, "prometheus_baseline.yml"), PROM_SINK_TARGET)
    tsdb_dir = os.path.join("/tmp", "nes_prom_tsdb")
    create_folder_and_remove_if_exists(tsdb_dir)  # fresh TSDB so PromQL rates aren't polluted by prior runs

    printInfo(f"Launching Prometheus ({prom_bin}) scraping {PROM_SINK_TARGET}, web={PROM_WEB_BIND} ...")
    prom_proc = subprocess.Popen(
        [
            prom_bin,
            f"--config.file={cfg_path}",
            f"--storage.tsdb.path={tsdb_dir}",
            f"--web.listen-address={PROM_WEB_BIND}",
            "--storage.tsdb.retention.time=1h",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    threading.Thread(target=stream_output, args=(prom_proc, worker_lines_label, []), daemon=True).start()

    web_port = int(PROM_WEB_BIND.split(":")[1])
    if not wait_for_port("localhost", web_port, timeout=20):
        printError("Prometheus web/API port did not open within 20s.")
        return prom_proc

    # rate() needs >= 2 samples in its window; with a 1s scrape, ~12s gives a comfortable margin.
    printInfo("Waiting ~12s for Prometheus to scrape the sink several times ...")
    time.sleep(12)

    count = prometheus_scalar("PRICE_count")
    obs_rate = prometheus_scalar("rate(PRICE_count[4s])")
    median = prometheus_scalar("histogram_quantile(0.5, rate(PRICE_bucket[4s]))")
    printInfo(f"PromQL PRICE_count                                 = {count}")
    printInfo(f"PromQL rate(PRICE_count[4s])                       = {obs_rate} obs/s")
    printInfo(f"PromQL histogram_quantile(0.5, rate(PRICE_bucket[4s])) = {median}")
    if median is not None and 300.0 < median < 800.0:
        printSuccess(
            f"Prometheus + PromQL validated: median price ≈ {median:.1f} "
            f"(regime A is price~N(500,167), so ~500 is expected). The coordinator poll loop can "
            f"query this same expression to detect the regime.")
    elif count and count > 0:
        printError(
            f"Prometheus is scraping (PRICE_count={count}) but histogram_quantile returned {median} "
            f"(outside the expected ~500 band) — check bucket range / metric name.")
    else:
        printError("Prometheus returned no data for the sink metrics — scrape target or metric name is wrong.")
    return prom_proc


def terminate_process(proc, name, timeout=5):
    """Gracefully terminate a process, escalating to SIGKILL if needed."""
    if proc.poll() is not None:
        printInfo(f"{name} already exited (code {proc.returncode})")
        return
    printInfo(f"Terminating {name} (pid={proc.pid})...")
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
        printSuccess(f"{name} terminated cleanly")
    except subprocess.TimeoutExpired:
        printError(f"{name} did not stop in {timeout}s — sending SIGKILL")
        proc.kill()
        proc.wait()


def find_data_query_id(repl_lines, timeout=15.0):
    """Poll repl_lines until the SELECT response appears and return its distributed query ID.

    The REPL emits [{"query_id": "<horse-name>"}] (exactly one key) for a deployed SELECT.
    All other setup statement responses have additional keys (worker, source_name, sink_name, …).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for line in repl_lines:
            try:
                parsed = json.loads(line)
                if (
                    isinstance(parsed, list)
                    and len(parsed) == 1
                    and set(parsed[0].keys()) == {"query_id"}
                ):
                    return parsed[0]["query_id"]
            except (json.JSONDecodeError, KeyError):
                pass
        time.sleep(0.1)
    return None


def collect_all_data_query_ids(repl_lines):
    """Scan all REPL output and return the set of every data query's distributed ID.

    Two output paths emit query IDs:
      - Initial SELECT (via stdin → JSON path): [{"query_id": "<horse-name>"}]
      - Adaptive swap (via callback → plain-text): [AdaptiveOpt] Deployed query (id=<horse-name>).
    Statistic collection queries are deployed internally via StatisticCoordinator and never
    print a distributed query ID to the REPL stdout, so they are excluded naturally.
    """
    ids = set()
    for line in repl_lines:
        if m := _JSON_QUERY_ID_RE.search(line):
            ids.add(m.group(1))
        elif m := _ADAPTIVE_QUERY_ID_RE.search(line):
            ids.add(m.group(1))
    return ids


def parse_throughput(worker_lines, data_query_ids):
    """Return list of (window_start_ms, query_id, query_type, throughput_tup_per_s).

    `query_type` is "data" if the queryId was observed in REPL output (initial SELECT or
    adaptive swap), otherwise "stat" — the statistic-collection queries are deployed
    internally by StatisticCoordinator and never print a distributed query ID to REPL stdout,
    so they fall through to the "stat" bucket.
    """
    measurements = []
    for line in worker_lines:
        m = _THROUGHPUT_RE.search(line)
        if m:
            qid = m.group(1)
            window_start = int(m.group(2))
            throughput = convert_unit_prefix(float(m.group(4)), m.group(5))
            qtype = "data" if qid in data_query_ids else "stat"
            measurements.append((window_start, qid, qtype, throughput))
    return measurements


def write_throughput_csv(measurements, output_path, duration_ms=None):
    """Write all per-window throughput samples to a CSV for time-series plotting.

    Timestamps are normalized to the first sample so all variants share the same
    source-start anchor, keeping regime slumps at the same x positions across runs.
    If duration_ms is given, measurements beyond that window are dropped — this trims
    the extra setup-phase measurements that variants like Prometheus collect before the
    benchmark timer starts, without shifting the alignment origin.
    """
    if not measurements:
        printError("No throughput data collected.")
        return
    origin = measurements[0][0]
    if duration_ms is not None:
        measurements = [m for m in measurements if m[0] - origin <= duration_ms]
    if not measurements:
        printError("No throughput data within duration window.")
        return
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "query_id", "query_type", "throughput_tup_per_s"])
        for ts, qid, qtype, tput in measurements:
            writer.writerow([ts - origin, qid, qtype, tput])
    printSuccess(f"Throughput data ({len(measurements)} samples) written to {os.path.abspath(output_path)}")


def run_benchmark(
    duration: int,
    skip_build: bool,
    clean: bool,
    output: str,
    sqrts: int = 0,
    constant_workload: bool = False,
    fixed_variant: str = "",
    baseline_prometheus: bool = False,
    baseline_switch_threshold: float = 888.49,
    baseline_poll_interval_ms: int = 1000,
    millis_per_file: int = 0,
):
    check_repository_root()

    prom_proc = None  # Prometheus server process (baseline mode only); torn down at the end.

    if clean:
        create_folder_and_remove_if_exists(build_dir)

    if not skip_build:
        printInfo("Building NebulaStream...")
        compile_nebulastream(cmake_flags, build_dir)
        printSuccess("Build complete.")
    else:
        printInfo("Skipping build (--skip-build)")

    for binary, label in [(worker_binary, "nes-single-node-worker"), (repl_binary, "nes-repl")]:
        if not os.path.isfile(binary):
            printError(f"Binary not found: {binary}")
            printError("Run without --skip-build to compile first.")
            sys.exit(1)

    data_path_a = ensure_dataset_a(path=DEFAULT_OUTPUT_A)
    if constant_workload:
        # Single regime — regime B isn't loaded. The probe predicate is expected to fire only
        # in the very first window (initial convergence to the optimal ordering); after that
        # the histogram is stable and the swap callback should be a no-op.
        data_path_b = ""
    else:
        data_path_b = ensure_dataset_b(path=DEFAULT_OUTPUT_B)
    # When --fixed-variant is set we want a baseline: the chosen filter order runs without ANY
    # adaptive machinery (no build branch splice, no probe queries, no swap callback). This is
    # the comparison point for "what would performance be without the adaptive system?" — the
    # answer to whether adaptation actually pays for its own overhead.
    variant_for_query = fixed_variant if fixed_variant else "bid_first"
    # millis_per_file <= 0 means "auto": size each regime so the run sees REGIME_CHANGES_PER_RUN
    # switches. An explicit --millis-per-file overrides this.
    if millis_per_file <= 0:
        millis_per_file = millis_per_file_for(duration)
    printInfo(f"Regime switch every {millis_per_file} ms (~{REGIME_CHANGES_PER_RUN} changes over {duration}s).")
    setup_sql = make_setup_sql(data_path_a, data_path_b, sqrts, constant_workload, variant_for_query, millis_per_file)
    if fixed_variant:
        printInfo(
            f"Using {sqrts} SQRT operators between filters; "
            f"workload={'CONSTANT (regime A only)' if constant_workload else 'ALTERNATING (A/B)'}; "
            f"fixed-variant={fixed_variant} (companion-statistic DISABLED for baseline).")
    else:
        printInfo(
            f"Using {sqrts} SQRT operators between filters; "
            f"workload={'CONSTANT (regime A only)' if constant_workload else 'ALTERNATING (A/B)'}; "
            f"variant=ADAPTIVE (companion-statistic enabled).")

    # --- Start worker ---
    printInfo(f"Starting nes-single-node-worker (grpc={WORKER_GRPC}, data={WORKER_DATA})...")
    worker_proc = subprocess.Popen(
        [
            worker_binary,
            "--grpc=0.0.0.0:8080",
            "--data_address=0.0.0.0:9090",
            "--worker.throughput_listener_interval_in_ms=100",
            "--worker.default_query_execution.operator_buffer_size=4194304",
            "--worker.number_of_buffers_in_global_buffer_manager=1024",
            "--worker.query_engine.number_of_worker_threads=4",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    worker_lines = []
    worker_thread = threading.Thread(
        target=stream_output, args=(worker_proc, "WORKER", worker_lines), daemon=True
    )
    worker_thread.start()

    grpc_port = int(WORKER_GRPC.split(":")[1])
    printInfo(f"Waiting for worker gRPC port {grpc_port}...")
    if not wait_for_port("localhost", grpc_port, timeout=15):
        printError("Worker did not open gRPC port within 15 seconds")
        terminate_process(worker_proc, "worker")
        sys.exit(1)
    printSuccess("Worker is up.")

    # --- Build REPL command ---
    # Base command (always present, no companion): the fixed-variant baseline path.
    repl_cmd = [repl_binary, "-f", "JSON"]
    # Adaptive mode: add the full --companion-* configuration so the REPL spawns the build
    # branch + two gated probes + swap callback. With --fixed-variant set we skip all of this
    # so the bench measures pure data-query throughput for that filter order — a clean
    # baseline to compare adaptive against.
    if baseline_prometheus:
        # Prometheus SOTA baseline: splice a single build branch that routes `price` into a
        # PrometheusSink (which builds the histogram and exposes it for scraping) instead of the
        # in-engine StatisticBuild→Probe→GrpcSink chain. One field → one sink → one exposer port
        # (a second --companion-field would need a second port). No gated --companion-condition
        # (the predicate is unused in baseline mode). We DO pass --companion-switch-to-sql so the
        # data query deploys via deployWithSwitchableAlternate (the same path the native benchmark
        # uses, which registers the data source before the build branch splices in) AND so the
        # switchable filter pair is in place for the step-3 poll loop to flip. The gated SetSwitch
        # callback stays installed but is inert in baseline mode (the sink emits no gRPC reports).
        repl_cmd += [
            "--companion-statistic",
            "--companion-field", "price",
            "--companion-metric", "MinVal",
            "--companion-window-size-ms", "60000000",
            "--companion-event-time-field", "BID$TIMESTAMP",
            "--companion-host", WORKER_GRPC,
            "--companion-switch-to-sql", make_reversed_query_sql(sqrts),
            "--companion-histogram-min", "0",
            "--companion-histogram-max", "2000",
            "--baseline-prometheus",
            "--prometheus-server-url", PROM_SINK_BIND,
            # Coordinator poll loop: query the Prometheus server we launch below, threshold the
            # median price to pick the filter order, and flip the gate via the shared switch client.
            "--baseline-prometheus-query-url", f"localhost:{PROM_WEB_BIND.split(':')[1]}",
            "--baseline-switch-threshold", str(baseline_switch_threshold),
            "--baseline-poll-interval-ms", str(baseline_poll_interval_ms),
        ]
    elif not fixed_variant:
        repl_cmd += [
            "--companion-statistic",
            # Splice the build branch into the data query (one source thread feeds both
            # subtrees) and run gated probes whose swap callbacks flip the workload-switch.
            "--companion-field", "price",
            # MinVal maps to Equi_Width_Histogram, which is the only metric the gated probe
            # supports (StatisticStoreReader returns histogram bins as rows for the Selection
            # predicate to filter on). Cardinality goes to a CountMinSketch — different probe
            # operator, no in-pipeline bin filtering.
            "--companion-metric", "MinVal",
            # 60M event-time ms — at the steady-state ingest rate of ~200M tup/s the histogram
            # closes ~3× per wall-clock second, low enough to keep the statistic store bounded
            # while frequent enough that gated-probe trigger fires arrive within ~1s. Smaller
            # windows have OOM'd the worker (event time advances at the tuple-emit rate).
            "--companion-window-size-ms", "60000000",
            "--companion-event-time-field", "BID$TIMESTAMP",
            "--companion-host", WORKER_GRPC,
            "--companion-switch-to-sql", make_reversed_query_sql(sqrts),
            # Histogram bucket range widened to [0, 2000] so both regimes' price distributions
            # fit (regime A: price~N(500,167); regime B: price~N(1277,167); regime B would be
            # clipped at the default max=1000).
            "--companion-histogram-min", "0",
            "--companion-histogram-max", "2000",
            # Two gated probes covering the two regimes by NON-OVERLAPPING price-bin ranges.
            # Probe A (fires on regime A: price~N(500, 167)): BINSTART < 900 → set switch=0
            #   (bid-first variant). Regime A's bidValue~N(50, 17) makes `bidValue < 10.45`
            #   the more selective filter (~0.9% pass) so we want it first.
            # Probe B (fires on regime B: price~N(1277, 167)): BINSTART >= 900 → set switch=1
            #   (price-first variant). Regime B's bidValue~N(-30, 17) makes `bidValue < 10.45`
            #   match ~99%, while `price < 888.49` is the selective one (~1%), so price-first wins.
            # Density threshold UINT64(500000): regime A's right-tail mass at BINSTART=900 is
            # ~160K tuples (~0.27% of 60M); regime B's left-tail there is ~2.16M (~3.6%). 500K
            # filters out the leaky tails so each probe matches only when its regime is
            # genuinely dominant.
            # Phase 2: two build branches monitoring TWO different fields, each with its own
            # predicate-and-target. Each request gets its own statisticId; both build branches
            # splice into BID via SpliceToRunningSourceTrait, and the source defers emission until
            # both have wired in (expected_splice_count=2). The probe inside each build branch
            # routes survivors to its own callback (no shared probe, no Generator polling).
            #
            # Probe A monitors PRICE (set via --companion-field "price" above): in regime A
            # (price ~ N(500, 167)) most mass is in [0, 900), so BINSTART < 900 → fires → target
            # switch=0 (bid-first variant; bidValue<10.45 is very selective in regime A).
            "--companion-condition", "BINSTART < UINT64(900) AND BINCOUNTER > UINT64(500000)",
            "--companion-target-value", "0",
            # Probe B monitors BIDVALUE: in regime B (bidValue ~ N(-30, 17)) most mass is below
            # the filter threshold 10.45, so BINSTART < 11 → fires → target switch=1.
            # Threshold 1500000: regime A's bidValue left-tail mass in [0, 11) is ~564K
            # (P(bidValue<11 | N(50, 17)) ≈ 0.94% × 60M); regime B's positive-tail mass there
            # is ~1.87M (P(0<bidValue<11 | N(-30, 17)) ≈ 3.12% × 60M). 1.5M cleanly separates.
            # NOTE: histogram-min=0/max=2000 (set globally for the bench) clips regime B's
            # negative bidValues — only its right shoulder survives, which is enough to
            # produce the distinguishable density spike Probe B's predicate keys on.
            "--companion-field-2", "bidValue",
            "--companion-condition-2", "BINSTART < UINT64(11) AND BINCOUNTER > UINT64(1500000)",
            "--companion-target-value-2", "1",
        ]

    # --- Start REPL ---
    printInfo("Starting nes-repl (distributed mode)...")
    repl_proc = subprocess.Popen(
        repl_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    repl_lines = []
    repl_thread = threading.Thread(
        target=stream_output, args=(repl_proc, "REPL", repl_lines), daemon=True
    )
    repl_thread.start()

    # --- Send SQL setup commands ---
    printInfo("Sending SQL setup commands to REPL...")
    try:
        repl_proc.stdin.write(setup_sql.encode())
        repl_proc.stdin.flush()
    except BrokenPipeError:
        printError("REPL stdin closed unexpectedly — did the REPL crash?")
        terminate_process(repl_proc, "nes-repl")
        terminate_process(worker_proc, "nes-single-node-worker")
        sys.exit(1)

    # --- Record the distributed query ID assigned to the data query ---
    printInfo("Waiting for data query deployment confirmation...")
    # LoopingMemory source parses the whole CSV at setup() before reporting deployed.
    # A 2.4 GB file takes ~1–2 min on this box, so give the REPL plenty of slack.
    data_query_id = find_data_query_id(repl_lines, timeout=300.0)
    if data_query_id is None:
        printError("Timed out waiting for the SELECT query response — REPL may have crashed.")
        terminate_process(repl_proc, "nes-repl")
        terminate_process(worker_proc, "nes-single-node-worker")
        sys.exit(1)
    printSuccess(f"Data query deployed with id: {data_query_id}")

    # --- Prometheus-baseline validation: confirm the spliced PrometheusSink built + exposed a histogram ---
    if baseline_prometheus:
        printInfo(f"Validating PrometheusSink exposer at {PROM_SCRAPE_URL} ...")
        ok, metrics_text, total = scrape_and_validate_prometheus_sink(PROM_SCRAPE_URL)
        sample = "\n".join(
            line for line in metrics_text.splitlines()
            if ("_bucket{" in line or line.endswith("_count") or "_count " in line or "_sum " in line)
        )[:1500]
        if ok:
            printSuccess(
                f"PrometheusSink is exposing a populated histogram ({total} observations). Sample:\n{sample}")
        else:
            printError(f"PrometheusSink validation FAILED — no observations scraped from {PROM_SCRAPE_URL}.")
            printError("Metrics text (first 800 chars):\n" + (metrics_text[:800] or "<empty>"))

        # Stand up a real Prometheus server scraping the sink and confirm PromQL works end-to-end.
        # This is the data path the coordinator poll loop will query (histogram_quantile over the
        # scraped buckets) to detect the workload regime.
        if ok:
            prom_proc = launch_and_validate_prometheus()

    printSuccess(f"Query deployed. Running for {duration} seconds...")

    # --- Run for the configured duration ---
    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        printInfo("Interrupted by user — tearing down early.")

    # --- Tear down ---
    printInfo("Tearing down...")
    if prom_proc is not None:
        terminate_process(prom_proc, "prometheus")
    terminate_process(repl_proc, "nes-repl")
    terminate_process(worker_proc, "nes-single-node-worker")

    # Wait for streaming threads to drain the remaining pipe output
    repl_thread.join(timeout=5)
    worker_thread.join(timeout=5)

    # --- Parse and write throughput CSV ---
    data_query_ids = collect_all_data_query_ids(repl_lines)
    printInfo(f"Data query IDs observed across all deployments: {data_query_ids}")
    measurements = parse_throughput(worker_lines, data_query_ids)
    write_throughput_csv(measurements, output, duration_ms=duration * 1000)

    printSuccess("Benchmark complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Adaptive optimization benchmark: single worker + distributed REPL."
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="How long (seconds) to let the query run before tearing down (default: 120).",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip the cmake configure + build step (binaries must already exist).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove and recreate the build directory before building.",
    )
    parser.add_argument(
        "--output",
        default="data_throughput_adaptive.csv",
        help="Path for the throughput CSV output (default: data_throughput_adaptive.csv).",
    )
    parser.add_argument(
        "--sqrts",
        type=int,
        default=0,
        help="Number of SQRT operators to insert between the two filters (default: 0). The SQRT chain "
        "is an always-true WHERE adding per-tuple CPU cost in the middle pipeline — useful for "
        "exposing throughput differences between filter orderings.",
    )
    parser.add_argument(
        "--constant-workload",
        action="store_true",
        help="Run with only regime A loaded (no FILE_PATH_2, no MILLIS_PER_FILE alternation). "
        "Expected behavior: histogram converges to a single distribution within the first window "
        "and the swap callback should fire at most once (the initial reconfiguration). Useful for "
        "verifying the adaptive mechanism settles instead of toggling continuously.",
    )
    parser.add_argument(
        "--fixed-variant",
        choices=["bid_first", "price_first"],
        default="",
        help="If set, skip the entire companion-statistic deployment (no build branch splice, no "
        "gated probes, no swap callback) and run ONLY the chosen filter ordering. Use this to "
        "produce a baseline curve showing what raw throughput looks like without adaptation — "
        "the comparison point that justifies the overhead of the adaptive machinery. "
        "Combine with --constant-workload to also restrict to a single data regime; otherwise "
        "the alternating workload exposes the dip when the fixed order mismatches the regime. "
        "Mirrors run_bid_value_first_benchmark.py / run_price_first_benchmark.py but keeps "
        "everything in one script so parameter drift between baselines and adaptive can't happen.",
    )
    parser.add_argument(
        "--baseline-prometheus",
        action="store_true",
        help="Run the Prometheus SOTA baseline instead of the native adaptive path: the spliced "
        "build branch routes the monitored field into a PrometheusSink (histogram built in the "
        "sink, exposed for scraping), a real Prometheus server scrapes it, and the coordinator "
        "poll loop queries Prometheus (PromQL) to drive the filter-order switch.",
    )
    parser.add_argument(
        "--baseline-switch-threshold",
        type=float,
        default=888.49,
        help="Poll-loop decision threshold on the PromQL median price: >= picks price-first (switch=1), "
        "below picks bid-first (switch=0). Default 888.49 (between regime A ~500 and regime B ~1277).",
    )
    parser.add_argument(
        "--baseline-poll-interval-ms",
        type=int,
        default=1000,
        help="How often (ms) the coordinator poll loop queries Prometheus (default: 1000).",
    )
    parser.add_argument(
        "--millis-per-file",
        type=int,
        default=0,
        help=f"Wall-clock milliseconds spent on one dataset before the alternating workload flips "
        f"regimes. Default 0 means auto: size each regime so the run sees ~{REGIME_CHANGES_PER_RUN} "
        f"regime changes regardless of --duration (= duration / {REGIME_CHANGES_PER_RUN + 1}). "
        f"Because this is wall-clock based, the regime duration is throughput-independent: the same "
        f"value gives the same regime length for native and for the ~50-70x slower Prometheus "
        f"baseline, with no per-variant retuning, and the throughput slumps stay aligned in the plots.",
    )
    args = parser.parse_args()

    run_benchmark(
        duration=args.duration,
        skip_build=args.skip_build,
        clean=args.clean,
        output=args.output,
        constant_workload=args.constant_workload,
        sqrts=args.sqrts,
        fixed_variant=args.fixed_variant,
        baseline_prometheus=args.baseline_prometheus,
        baseline_switch_threshold=args.baseline_switch_threshold,
        baseline_poll_interval_ms=args.baseline_poll_interval_ms,
        millis_per_file=args.millis_per_file,
    )
