#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Over-time benchmark with two runs per condition:

  run "statistic_build"  — every N seconds, submit a statistic-build query.
                           Prometheus is NOT started; the worker gets the full
                           container CPU+memory budget.
  run "prometheus_sink"  — every N seconds, submit a query that exposes rows
                           through the Prometheus sink. Prometheus is started
                           inside the same container so worker + Prometheus
                           share one cgroup.

Conditions are now three deployment tiers (sensor / edge / cloud); each tier's
(cpus, memory) budget comes from config.CONDITION_BUDGETS. The runner translates
the lookup into `--cpus` / `--memory` on `docker run` (cloud gets neither flag).
Network throttling (tc tbf on a bridge) has been removed — bandwidth was never
the bottleneck; CPU+memory contention between worker and Prometheus is what the
experiment measures.

For each run we compute:
  - total throughput across all queries (sum of per-query averages)
  - median latency across all queries

Run from the repository root:
  python3 -m scripts.benchmarking.multiple-statistic-queries-over-time-prometheus.run_prometheus_benchmark
"""

import csv
import importlib.util
import os
import random
import re
import shlex
import statistics
import subprocess
import sys
import threading
import time

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

### Load the sibling config.py by file path — the containing folder name has
### hyphens, so a dotted `from ... import config` would not resolve.
### Must happen BEFORE importing scripts.benchmarking.common.* because common.config
### resolves SINGLE_NODE_EXECUTABLE / NEBULI_EXECUTABLE against NES_BUILD_DIR at
### import time, and we propagate our BUILD_DIR into that env var below.
_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.py")
_spec = importlib.util.spec_from_file_location("prometheus_bench_config", _config_path)
config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(config)

### Bridge env-var naming: this runner's wrapper sets BUILD_DIR, common.config reads NES_BUILD_DIR.
os.environ.setdefault("NES_BUILD_DIR", config.BUILD_DIR)

from scripts.benchmarking.common.config import (
    NEBULI_EXECUTABLE,
    SINGLE_NODE_EXECUTABLE,
)
from scripts.benchmarking.common.worker_lifecycle import (
    stop_queries,
    submit_query,
)

### --- Constants ---------------------------------------------------------

SINGLE_NODE_BINARY = SINGLE_NODE_EXECUTABLE
NES_CLI = NEBULI_EXECUTABLE[0]

### Ports published by the combined container (loopback inside container <-> host).
GRPC_PORT = 8080
DATA_PORT = 9091
PROM_UI_PORT = 9090

### Release + native, with logging at ERROR (matches CMakeLists.txt's own Release default). LEVEL_NONE
### compiles every log macro out — including error paths — so a misconfigured query yaml fails with
### empty stdout/stderr and a 0-byte log, making diagnosis impossible. ERROR adds negligible overhead
### and keeps genuine failures visible. Built lazily — get_vcpkg_dir() raises on unregistered hostnames,
### and we want the module to import cleanly on the dev box even if cmake will only ever run on the
### benchmark host.
def _cmake_flags():
    return (
        "-G Ninja "
        "-DCMAKE_BUILD_TYPE=Release "
        f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
        "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
        "-DENABLE_LARGE_TESTS=0 "
        "-DNES_BUILD_NATIVE:BOOL=ON "
        "-DNES_LOG_LEVEL:STRING=ERROR"
    )


### --- Docker image / container plumbing ---------------------------------

DOCKERFILE_RELATIVE = "docker/single-node-worker/SingleNodeWorkerWithPrometheus.dockerfile"
DOCKER_BUILD_CONTEXT = "docker/single-node-worker"
RUNTIME_BASE_DOCKERFILE = "docker/runtime/RuntimeBase.dockerfile"
RUNTIME_BASE_BUILD_CONTEXT = "docker/runtime"
RUNTIME_BASE_IMAGE = "nes-runtime-base:test"
TCPGEN_BUILD_CONTEXT = "rust-tcp-generator"


def ensure_runtime_base_image():
    """Build nes-runtime-base:test if it's not already present locally. The combined
    SingleNodeWorkerWithPrometheus.dockerfile uses it as the FROM base, and it is NOT
    pushed to Docker Hub under that path, so a fresh host (or one where docker images were
    pruned) needs us to build it here."""
    inspect = subprocess.run(["docker", "image", "inspect", RUNTIME_BASE_IMAGE], capture_output=True)
    if inspect.returncode == 0:
        return
    printInfo(f"Building Docker image {RUNTIME_BASE_IMAGE} (one-time, layer-cached afterwards)")
    subprocess.run(
        ["docker", "build", "-t", RUNTIME_BASE_IMAGE, "-f", RUNTIME_BASE_DOCKERFILE, RUNTIME_BASE_BUILD_CONTEXT],
        check=True,
    )


def ensure_image():
    """Build nes-bench-prom-combined:local if it's not already present. docker build is
    layer-cached so calling this on every run is cheap; we still skip the subprocess
    spawn when the image is known-present."""
    image = config.WORKER_DOCKER_IMAGE
    inspect = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
    if inspect.returncode == 0:
        return
    printInfo(f"Building Docker image {image}")
    subprocess.run(
        ["docker", "build", "-t", image, "-f", DOCKERFILE_RELATIVE, DOCKER_BUILD_CONTEXT],
        check=True,
    )


def ensure_tcpgen_image():
    """Build nes-bench-tcp-gen:local if it's not already present. The Rust crate lives in
    rust-tcp-generator/ at the repo root and uses its own Dockerfile."""
    image = config.TCP_GENERATOR_IMAGE
    inspect = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
    if inspect.returncode == 0:
        return
    printInfo(f"Building Docker image {image}")
    subprocess.run(
        ["docker", "build", "-t", image, TCPGEN_BUILD_CONTEXT],
        check=True,
    )


def _worker_args(condition):
    """Command-line passed to nes-single-node-worker inside the container.

    Bind on 0.0.0.0 so the published Docker ports can reach the server. Otherwise
    mirrors the host-binary launch in scripts.benchmarking.common.worker_lifecycle.start_single_node_worker
    so the worker behaves identically. The worker-thread count is tied to the tier's CPU budget
    (config.worker_threads_for) so a capped tier isn't oversubscribed.
    """
    return [
        f"--grpc=0.0.0.0:{GRPC_PORT}",
        f"--data_address=0.0.0.0:{DATA_PORT}",
        f"--worker.query_engine.number_of_worker_threads={config.worker_threads_for(condition)}",
        f"--worker.default_query_execution.execution_mode={config.EXECUTION_MODE}",
        f"--worker.number_of_buffers_in_global_buffer_manager={config.NUMBER_OF_BUFFERS}",
        f"--worker.default_query_optimization.join_strategy={config.JOIN_STRATEGY}",
        "--worker.query_engine.admission_queue_size=1000000",
        f"--worker.default_query_execution.page_size={config.PAGE_SIZE}",
        f"--worker.default_query_execution.operator_buffer_size={config.BUFFER_SIZE_BYTES}",
        "--worker.latency_listener=true",
        f"--worker.throughput_listener_interval_in_ms=200",
    ]


def _container_name(condition, run_name):
    return f"nes-bench-{condition}-{run_name}"


def start_combined_container(condition, run_name, *, enable_prometheus, num_prom_targets, log_stream):
    """Launch the combined nes-single-node-worker + (optional) Prometheus container.

    Returns (container_name, docker_logs_proc) — the caller is expected to call
    stop_combined_container() with the container name when done.

    `log_stream` is a writable file handle; `docker logs -f` is piped into it so the
    existing throughput/latency log parsers keep working unchanged.
    """
    name = _container_name(condition, run_name)

    ### Best-effort: clear any straggler from a previous aborted run.
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)

    ### Intentionally no --rm here: docker inspect on State.OOMKilled / State.ExitCode requires
    ### the container record to still exist after exit. stop_combined_container() does an explicit
    ### `docker rm -f` once inspection is done.
    cmd = ["docker", "run", "-d", "--name", name]

    budget = config.CONDITION_BUDGETS.get(condition, {"cpus": None, "memory": None})
    if budget.get("cpus") is not None:
        cmd += [f"--cpus={budget['cpus']}"]
    if budget.get("memory") is not None:
        cmd += [f"--memory={budget['memory']}"]

    ### Bind-mount the host-built worker binary into the container so we don't have to rebuild
    ### the image when engine code changes.
    cmd += ["-v", f"{os.path.abspath(SINGLE_NODE_BINARY)}:/usr/bin/nes-single-node-worker:ro"]

    ### Publish only the ports the host actually talks to: gRPC for nes-cli (mandatory) and the
    ### Prometheus UI (so you can browse http://localhost:9090). The 8800..8800+N-1 sink ports
    ### stay loopback-only — Prometheus scrapes them from inside this same container, no host
    ### round-trip needed. DATA_PORT (9091) is forwarded for symmetry/diagnostics; nothing external
    ### connects to it in this single-worker setup.
    cmd += [
        "-p", f"{GRPC_PORT}:{GRPC_PORT}",
        "-p", f"{DATA_PORT}:{DATA_PORT}",
        "-p", f"{PROM_UI_PORT}:{PROM_UI_PORT}",
    ]

    if enable_prometheus and num_prom_targets > 0:
        ### Compact range form: the entrypoint expands NUM_TARGETS×PORT_BASE into the actual
        ### 127.0.0.1:<port> list inside the container, so `docker run` argv stays short even
        ### at N=100+ queries.
        cmd += [
            "-e", "NES_RUN_PROMETHEUS=1",
            "-e", f"NES_PROM_PORT_BASE={config.SINK_PORT_BASE}",
            "-e", f"NES_PROM_NUM_TARGETS={num_prom_targets}",
            "-e", f"NES_PROM_SCRAPE_INTERVAL={config.PROMETHEUS_SCRAPE_INTERVAL_SECONDS}s",
        ]
    else:
        cmd += ["-e", "NES_RUN_PROMETHEUS=0"]

    cmd += [config.WORKER_DOCKER_IMAGE]
    cmd += _worker_args(condition)

    printInfo(f"docker run: {' '.join(shlex.quote(c) for c in cmd)}")
    container_id = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()
    printInfo(f"started container {name} ({container_id[:12]})")

    ### Pipe container stdout/stderr into the run's worker.log; the existing parser regexes
    ### expect the worker's log format unchanged.
    logs_proc = subprocess.Popen(
        ["docker", "logs", "-f", name],
        stdout=log_stream,
        stderr=subprocess.STDOUT,
    )
    return name, logs_proc


def stop_combined_container(name, logs_proc):
    """Stop the container, wait for the docker-logs sidecar to drain, and return a dict
    {oom_killed: bool, exit_code: int} so the caller can flag OOM-kill in the summary."""
    ### Give the worker a moment to flush listener output before SIGTERM.
    time.sleep(1)
    subprocess.run(["docker", "stop", "--time", "5", name], capture_output=True)
    ### `docker logs -f` exits after the container stops; wait so the file handle isn't truncated.
    try:
        logs_proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        logs_proc.kill()
        logs_proc.wait()

    state = {"oom_killed": False, "exit_code": None}
    inspect = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.OOMKilled}} {{.State.ExitCode}}", name],
        capture_output=True,
        text=True,
    )
    if inspect.returncode == 0:
        parts = inspect.stdout.strip().split()
        if len(parts) == 2:
            state["oom_killed"] = parts[0].lower() == "true"
            try:
                state["exit_code"] = int(parts[1])
            except ValueError:
                pass

    ### `--rm` cleans the container on exit, but if docker stop hit a timeout the container can
    ### linger; best-effort prune.
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    return state


### --- Per-process RSS sampling -----------------------------------------
### Memory-budget story: on a capped edge tier the co-located Prometheus server occupies a fixed
### chunk of the cgroup's --memory budget that is then unavailable to the worker's buffer pool. We
### sample per-process RSS for the whole run and report the peak per process group, so the cost can
### be framed as "X% of the device budget" rather than (mis)attributed to a throughput slowdown.
###
### RSS is read via `docker top` (host-side ps against the container's PID namespace), so the
### container image needs no ps of its own. RSS double-counts shared pages, so treat these as a
### per-process footprint estimate, not an exact unique-set size.
RSS_SAMPLE_INTERVAL_SECONDS = float(os.environ.get("RSS_SAMPLE_INTERVAL_SECONDS", "1.0"))


def _sample_container_rss_kb(name):
    """Return {'prometheus', 'worker', 'total'} RSS in KiB for the container, or None if it can't be
    sampled (container exited / docker error). RSS is summed per process group via `ps -eo rss,args`."""
    proc = subprocess.run(["docker", "top", name, "-eo", "rss,args"], capture_output=True, text=True)
    if proc.returncode != 0:
        return None
    totals = {"prometheus": 0, "worker": 0, "total": 0}
    for line in proc.stdout.splitlines()[1:]:  ### skip the "RSS COMMAND" header row
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        try:
            rss = int(parts[0])
        except ValueError:
            continue
        cmd = parts[1]
        totals["total"] += rss
        if "prometheus" in cmd:
            totals["prometheus"] += rss
        elif "nes-single-node-worker" in cmd:
            totals["worker"] += rss
    return totals


class RssSampler:
    """Polls per-process RSS inside a running container on a background thread until stop()."""

    def __init__(self, name, interval=RSS_SAMPLE_INTERVAL_SECONDS):
        self.name = name
        self.interval = interval
        self.samples = []  ### list of (elapsed_s, prometheus_kb, worker_kb, total_kb)
        self.peak = {"prometheus": 0, "worker": 0, "total": 0}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        t0 = time.time()
        while not self._stop.is_set():
            sample = _sample_container_rss_kb(self.name)
            if sample is not None:
                self.samples.append((time.time() - t0, sample["prometheus"], sample["worker"], sample["total"]))
                for key in self.peak:
                    self.peak[key] = max(self.peak[key], sample[key])
            self._stop.wait(self.interval)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=self.interval + 5)
        return self.peak


def write_rss_timeseries(samples, csv_path):
    """Write the per-process RSS time-series (one row per sample) in MiB for downstream plotting."""
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["elapsed_s", "prometheus_rss_mb", "worker_rss_mb", "total_rss_mb"])
        for elapsed, prom_kb, worker_kb, total_kb in samples:
            w.writerow([f"{elapsed:.3f}", prom_kb / 1024.0, worker_kb / 1024.0, total_kb / 1024.0])


def _tcpgen_name(condition, run_name):
    return f"nes-bench-tcpgen-{condition}-{run_name}"


def start_tcp_generator(condition, run_name, num_ports, log_path):
    """Launch the Rust TCP producer joined to the worker container's network namespace.

    The producer streams CSV rows `id,value,timestamp\\n` on each of the
    [TCP_PORT_BASE, TCP_PORT_BASE+num_ports) ports. It prints a single READY line
    on stdout after all listeners are bound, which we block on so the first query's
    TCP source doesn't connect before the producer is ready.

    Returns (gen_container_name, docker_logs_proc, log_stream).
    """
    worker_name = _container_name(condition, run_name)
    gen_name = _tcpgen_name(condition, run_name)

    ### Clear any straggler from a previous run before bringing up a new one.
    subprocess.run(["docker", "rm", "-f", gen_name], capture_output=True)

    ### --network container:<worker_name> shares the worker's netns: the producer binds on
    ### 127.0.0.1 inside that netns, the worker connects to 127.0.0.1:<port> over loopback.
    ### Cannot combine with `-p` (Docker refuses), but we don't need to — no host needs to
    ### reach the producer directly.
    cmd = [
        "docker", "run", "-d",
        "--name", gen_name,
        "--network", f"container:{worker_name}",
        config.TCP_GENERATOR_IMAGE,
        "--port-base", str(config.TCP_PORT_BASE),
        "--num-ports", str(num_ports),
    ]
    printInfo(f"docker run (tcpgen): {' '.join(shlex.quote(c) for c in cmd)}")
    subprocess.run(cmd, check=True, capture_output=True, text=True)

    log_stream = open(log_path, "w")
    logs_proc = subprocess.Popen(
        ["docker", "logs", "-f", gen_name],
        stdout=log_stream,
        stderr=subprocess.STDOUT,
    )

    ### Poll the live tcpgen.log for the READY handshake. With 100 ports tokio binds
    ### them in well under a second; 30s is a generous safety margin for first run.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        time.sleep(0.2)
        try:
            with open(log_path, "r") as f:
                if "READY" in f.read():
                    printInfo(
                        f"[tcpgen/{condition}/{run_name}] producer ready on "
                        f"ports {config.TCP_PORT_BASE}..{config.TCP_PORT_BASE + num_ports - 1}"
                    )
                    return gen_name, logs_proc, log_stream
        except FileNotFoundError:
            pass

    ### Timeout: producer never reported READY. Tear it down and surface the error.
    logs_proc.kill()
    logs_proc.wait()
    log_stream.close()
    subprocess.run(["docker", "rm", "-f", gen_name], capture_output=True)
    raise RuntimeError(f"TCP generator did not report READY within 30s; see {log_path}")


def stop_tcp_generator(name, logs_proc, log_stream):
    """Stop the producer container and drain its docker-logs sidecar.

    Must run BEFORE stop_combined_container — when the worker netns is destroyed,
    the producer would otherwise be killed mid-syscall."""
    subprocess.run(["docker", "stop", "--time", "2", name], capture_output=True)
    try:
        logs_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logs_proc.kill()
        logs_proc.wait()
    log_stream.close()
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)


### --- Query template rendering -----------------------------------------

def render_statistic_query(template_path, source_name, query_index, out_path):
    with open(template_path) as f:
        template = f.read()
    args = {
        "source_name": source_name,
        "tcp_port": config.TCP_PORT_BASE + query_index,
        "flush_interval": config.FLUSH_INTERVAL_MS,
        "reservoir_size": config.RESERVOIR_SIZE,
        **config.EQUI_WIDTH_HISTOGRAM,
    }
    with open(out_path, "w") as f:
        f.write(template.format(**args))


def render_prometheus_query(source_name, sink_port, query_index, out_path):
    with open(config.PROMETHEUS_QUERY_TEMPLATE) as f:
        template = f.read()
    ### Inside the combined container Prometheus scrapes 127.0.0.1:<sink_port>, so the sink's
    ### exposer also binds to 127.0.0.1 — same loopback. No SINK_BIND_HOST override needed.
    args = {
        "source_name": source_name,
        "sink_host": "127.0.0.1",
        "sink_port": sink_port,
        "tcp_port": config.TCP_PORT_BASE + query_index,
        "flush_interval": config.FLUSH_INTERVAL_MS,
        ### Spread EQUI_WIDTH_HISTOGRAM to pull num_buckets / min_value / max_value into the sink's
        ### histogram config, so SOTA's bucket layout matches the in-engine EQUIWIDTHHISTOGRAM operator
        ### for apples-to-apples comparison. The extra `counter_type` key is ignored by str.format.
        **config.EQUI_WIDTH_HISTOGRAM,
    }
    with open(out_path, "w") as f:
        f.write(template.format(**args))


### --- Log parsing ------------------------------------------------------

### The worker formats queryId as `QueryId(local=<uuid>, distributed=<name>)`. We capture
### the distributed name, since that's what `submit_query` echoes back and what `query_ids`
### holds — keeping the old group-number contract (group 1 = id).
THROUGHPUT_RE = re.compile(r'Throughput for queryId QueryId\(local=[\w-]+, distributed=(\w+)\) in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s')
LATENCY_RE = re.compile(r'Latency for queryId QueryId\(local=[\w-]+, distributed=(\w+)\) and (\d+) tasks over duration (\d+)-(\d+) is (\d+\.\d+) (\w?)s')


def parse_throughput(log_path):
    """Return list of (query_id, window_start_ms, window_end_ms, throughput_tps) tuples."""
    samples = []
    with open(log_path) as f:
        for line in f:
            m = THROUGHPUT_RE.search(line)
            if not m:
                continue
            query_id = m.group(1)
            window_start = int(m.group(2))
            window_end = int(m.group(3))
            value = convert_unit_prefix(float(m.group(4)), m.group(5))
            samples.append((query_id, window_start, window_end, value))
    return samples


def parse_latency(log_path):
    """Return list of (query_id, num_tasks, duration_start_ms, duration_end_ms, latency_seconds) tuples."""
    samples = []
    with open(log_path) as f:
        for line in f:
            m = LATENCY_RE.search(line)
            if not m:
                continue
            query_id = m.group(1)
            num_tasks = int(m.group(2))
            duration_start = int(m.group(3))
            duration_end = int(m.group(4))
            value = convert_unit_prefix(float(m.group(5)), m.group(6))
            samples.append((query_id, num_tasks, duration_start, duration_end, value))
    return samples


def aggregate(samples, query_ids):
    """Average each query's samples (value is the last tuple element), return one value per query."""
    grouped = {}
    for sample in samples:
        q = sample[0]
        if q in query_ids:
            grouped.setdefault(q, []).append(sample[-1])
    return [statistics.mean(vs) for vs in grouped.values() if vs]


def write_throughput_timeseries(samples, query_ids, output_path):
    """Per-sample throughput CSV. Columns: query_id, window_start_ms, window_end_ms,
    window_start_ms_normalized, throughput_tps. The normalized timestamp is relative to the
    earliest sample in this run so plots have a zero-aligned x-axis."""
    filtered = [s for s in samples if s[0] in query_ids]
    if not filtered:
        return
    t0 = min(s[1] for s in filtered)
    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query_id", "window_start_ms", "window_end_ms", "window_start_ms_normalized", "throughput_tps"])
        for query_id, window_start, window_end, value in filtered:
            w.writerow([query_id, window_start, window_end, window_start - t0, value])


def write_latency_timeseries(samples, query_ids, output_path):
    """Per-sample latency CSV. Columns: query_id, num_tasks, duration_start_ms, duration_end_ms,
    duration_start_ms_normalized, latency_s. Normalized timestamp uses the earliest sample in this
    run."""
    filtered = [s for s in samples if s[0] in query_ids]
    if not filtered:
        return
    t0 = min(s[2] for s in filtered)

    ### The latency listener emits one sample per completed task, so a single query can produce tens of
    ### millions of rows (multi-GB CSV). Uniformly stride each query's samples down to at most
    ### config.LATENCY_MAX_SAMPLES_PER_QUERY rows (0 = keep all). Striding (vs head-truncation) keeps the
    ### distribution and the time spread, so percentile / over-time plots stay representative.
    cap = config.LATENCY_MAX_SAMPLES_PER_QUERY
    stride = {}
    if cap and cap > 0:
        counts = {}
        for s in filtered:
            counts[s[0]] = counts.get(s[0], 0) + 1
        stride = {q: (c + cap - 1) // cap for q, c in counts.items()}
        if any(st > 1 for st in stride.values()):
            kept = sum(min(c, cap) for c in counts.values())
            printInfo(
                f"latency CSV {os.path.basename(output_path)}: downsampling to <= {cap} samples/query "
                f"(~{len(filtered) - kept} of {len(filtered)} rows dropped)"
            )

    seen = {}
    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query_id", "num_tasks", "duration_start_ms", "duration_end_ms", "duration_start_ms_normalized", "latency_s"])
        for query_id, num_tasks, duration_start, duration_end, value in filtered:
            if stride:
                idx = seen.get(query_id, 0)
                seen[query_id] = idx + 1
                if idx % stride[query_id] != 0:
                    continue
            w.writerow([query_id, num_tasks, duration_start, duration_end, duration_start - t0, value])


### --- Run orchestration ------------------------------------------------

def run_one(name, working_dir, prepare_query_for, csv_output_dir, condition, *, enable_prometheus, num_prom_targets):
    """Submit TOTAL_QUERIES_PER_RUN queries every QUERY_LAUNCH_INTERVAL_SECONDS.

    `prepare_query_for(i, working_dir)` returns the path to a rendered query yaml;
    statistic_build and prometheus_sink share this timing loop.

    `csv_output_dir` is where the per-run time-series CSVs are written (flat, not nested
    under working_dir), so all CSVs from one benchmark run sit next to each other.

    `condition` ("cloud" | "edge" | "sensor") is part of the CSV filename so multiple runs into
    the same OUTPUT_DIR can be compared side-by-side without overwriting each other.

    `enable_prometheus` controls whether Prometheus runs inside the container alongside the
    worker. statistic_build keeps it off (full container budget for the worker);
    prometheus_sink turns it on (shared cgroup).
    """
    create_folder_and_remove_if_exists(working_dir)
    worker_log = os.path.join(working_dir, "worker.log")
    cli_log_path = os.path.join(working_dir, "nes-cli.log")
    tcpgen_log_path = os.path.join(working_dir, "tcpgen.log")
    worker_stdout = open(worker_log, "w")
    cli_log = open(cli_log_path, "w")
    container_name = None
    logs_proc = None
    tcpgen_name = None
    tcpgen_logs_proc = None
    tcpgen_log_stream = None
    rss_sampler = None
    query_ids = []
    container_state = {
        "oom_killed": False,
        "exit_code": None,
        "rss_peak_prometheus_kb": 0,
        "rss_peak_worker_kb": 0,
        "rss_peak_total_kb": 0,
    }

    try:
        container_name, logs_proc = start_combined_container(
            condition,
            name,
            enable_prometheus=enable_prometheus,
            num_prom_targets=num_prom_targets,
            log_stream=worker_stdout,
        )
        time.sleep(5)  ### give the worker a moment to bind its grpc port

        ### Poll per-process RSS for the whole run so we can report the peak Prometheus footprint
        ### (and the worker's, as a baseline) against the tier's --memory budget.
        rss_sampler = RssSampler(container_name).start()

        total = config.TOTAL_QUERIES_PER_RUN[name]

        ### Bring up the Rust producer in the worker's netns, then block on its READY line so
        ### the first query's TCPSource doesn't hit a still-binding port.
        tcpgen_name, tcpgen_logs_proc, tcpgen_log_stream = start_tcp_generator(
            condition, name, total, tcpgen_log_path,
        )
        query_yaml = None
        for i in range(total):
            query_yaml = prepare_query_for(i, working_dir)
            ### \r-overwrite the per-query progress line so the terminal doesn't get
            ### flooded with one line per submission for large TOTAL_QUERIES_PER_RUN.
            sys.stdout.write(f"\r[{condition}/{name}] submitting query {i + 1}/{total}")
            sys.stdout.flush()
            query_id = submit_query(query_yaml, cli_log)[0]
            query_ids.append(query_id)
            time.sleep(config.QUERY_LAUNCH_INTERVAL_SECONDS)
        sys.stdout.write("\n")
        sys.stdout.flush()

        printInfo(f"[{condition}/{name}] all queries submitted; collecting for {config.MEASUREMENT_WINDOW_SECONDS}s")
        time.sleep(config.MEASUREMENT_WINDOW_SECONDS)

        printInfo(f"[{condition}/{name}] stopping queries")
        ### When the container OOM-killed mid-run, submit_query/stop_queries will hang or fail.
        ### Best-effort: only attempt stop if container is still up.
        if query_ids:
            try:
                stops = stop_queries(query_ids, query_file=query_yaml, cli_log_file=cli_log)
                for p in stops:
                    p.wait(timeout=30)
            except Exception as e:
                printError(f"[{condition}/{name}] stop_queries failed: {e}")
    finally:
        time.sleep(2)  ### let the latency listener flush its last interval
        ### Stop the TCP generator BEFORE the worker — the producer is joined to the worker's
        ### netns, so tearing down the worker would otherwise kill the producer mid-syscall.
        if tcpgen_name is not None:
            try:
                stop_tcp_generator(tcpgen_name, tcpgen_logs_proc, tcpgen_log_stream)
            except Exception as e:
                printError(f"[tcpgen/{condition}/{name}] stop failed: {e}")
        ### Stop the RSS sampler before the container so the last `docker top` still has a live target.
        if rss_sampler is not None:
            rss_sampler.stop()
        if container_name is not None:
            container_state = stop_combined_container(container_name, logs_proc)
        if rss_sampler is not None:
            container_state["rss_peak_prometheus_kb"] = rss_sampler.peak["prometheus"]
            container_state["rss_peak_worker_kb"] = rss_sampler.peak["worker"]
            container_state["rss_peak_total_kb"] = rss_sampler.peak["total"]
        worker_stdout.close()
        cli_log.close()

    if container_state["oom_killed"]:
        printError(
            f"[{condition}/{name}] container OOM-killed (exit_code={container_state['exit_code']}). "
            f"Throughput/latency tail likely reflects collapse, not steady-state."
        )

    throughput_samples = parse_throughput(worker_log)
    latency_samples = parse_latency(worker_log)
    query_id_set = set(query_ids)
    per_query_throughput = aggregate(throughput_samples, query_id_set)
    per_query_latency = aggregate(latency_samples, query_id_set)

    ### Per-sample time-series CSVs for downstream plotting (runtime vs throughput / latency).
    ### Flat into csv_output_dir with `{condition}_{run}_{metric}.csv` so the notebook glob picks them up.
    os.makedirs(csv_output_dir, exist_ok=True)
    prefix = f"{condition}_"
    throughput_csv = os.path.join(csv_output_dir, f"{prefix}{name}_throughput.csv")
    latency_csv = os.path.join(csv_output_dir, f"{prefix}{name}_latency.csv")
    write_throughput_timeseries(throughput_samples, query_id_set, throughput_csv)
    write_latency_timeseries(latency_samples, query_id_set, latency_csv)
    if os.path.exists(throughput_csv):
        printSuccess(f"[{condition}/{name}] throughput time-series: {os.path.abspath(throughput_csv)}")
    if os.path.exists(latency_csv):
        printSuccess(f"[{condition}/{name}] latency time-series:    {os.path.abspath(latency_csv)}")

    ### Per-process RSS time-series + peak. The peak Prometheus RSS is the memory-budget cost we care
    ### about on capped tiers; the worker RSS is the baseline it competes with.
    if rss_sampler is not None and rss_sampler.samples:
        rss_csv = os.path.join(csv_output_dir, f"{prefix}{name}_rss.csv")
        write_rss_timeseries(rss_sampler.samples, rss_csv)
        printSuccess(f"[{condition}/{name}] RSS time-series:        {os.path.abspath(rss_csv)}")
        printInfo(
            f"[{condition}/{name}] peak RSS — prometheus {container_state['rss_peak_prometheus_kb'] / 1024.0:.1f} MiB, "
            f"worker {container_state['rss_peak_worker_kb'] / 1024.0:.1f} MiB, "
            f"total {container_state['rss_peak_total_kb'] / 1024.0:.1f} MiB"
        )

    total_throughput = sum(per_query_throughput) if per_query_throughput else 0.0
    median_latency = statistics.median(per_query_latency) if per_query_latency else float("nan")
    return total_throughput, median_latency, per_query_throughput, per_query_latency, query_ids, container_state


def make_statistic_prep():
    def prepare(i, working_dir):
        template = random.choice(config.STATISTIC_QUERY_TEMPLATES)
        source_name = f"stat_src_{i}"
        out = os.path.join(working_dir, f"query_{i:03d}.yaml")
        render_statistic_query(template, source_name, i, out)
        return out
    return prepare


def make_prometheus_prep():
    def prepare(i, working_dir):
        source_name = f"prom_src_{i}"
        sink_port = config.SINK_PORT_BASE + i
        out = os.path.join(working_dir, f"query_{i:03d}.yaml")
        render_prometheus_query(source_name, sink_port, i, out)
        return out
    return prepare


def write_summary(output_dir, results, condition):
    """Append (or create) a summary CSV at output_dir root."""
    csv_path = os.path.join(output_dir, "summary.csv")
    write_header = not os.path.exists(csv_path)
    mode = "a" if os.path.exists(csv_path) else "w"
    with open(csv_path, mode, newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow([
                "condition", "run", "total_throughput_tps", "median_latency_s", "num_queries",
                "oom_killed", "exit_code",
                "peak_prometheus_rss_mb", "peak_worker_rss_mb", "peak_total_rss_mb",
            ])
        for name, (tp, lat, per_tp, per_lat, qids, state) in results.items():
            w.writerow([
                condition, name, tp, lat, len(qids), state["oom_killed"], state["exit_code"],
                state.get("rss_peak_prometheus_kb", 0) / 1024.0,
                state.get("rss_peak_worker_kb", 0) / 1024.0,
                state.get("rss_peak_total_kb", 0) / 1024.0,
            ])
    printSuccess(f"Summary written to {os.path.abspath(csv_path)}")


### Four deployment tiers, all sharing the same image / ports / engine config — only the cgroup
### quota differs. Lookup in CONDITION_BUDGETS picks the (cpus, memory) pair per condition; cloud
### passes no caps. Override via CONDITIONS=cloud,edge_heavy,edge_light,sensor or CONDITION=sensor
### for a single tier.
DEFAULT_CONDITIONS = ["cloud", "edge_heavy", "edge_light", "sensor"]


def _run_condition(condition, output_root):
    """Execute every run_name in config.RUNS for a single *condition*."""
    cond_subdir = condition
    results = {}

    for run_name in config.RUNS:
        run_dir = os.path.join(output_root, cond_subdir, run_name)
        if run_name == "statistic_build":
            results[run_name] = run_one(
                run_name, run_dir, make_statistic_prep(), output_root, condition,
                enable_prometheus=False,
                num_prom_targets=0,
            )
        elif run_name == "prometheus_sink":
            num_targets = config.TOTAL_QUERIES_PER_RUN["prometheus_sink"]
            results[run_name] = run_one(
                run_name, run_dir, make_prometheus_prep(), output_root, condition,
                enable_prometheus=True,
                num_prom_targets=num_targets,
            )
        else:
            printError(f"unknown run name '{run_name}', skipping")

    write_summary(output_root, results, condition=condition)
    for name, (tp, lat, *_rest, state) in ((n, r) for n, r in results.items()):
        oom_note = "  [OOM-KILLED]" if state["oom_killed"] else ""
        printSuccess(f"[{condition}] [{name}] total_throughput={tp:.2f} Tup/s  median_latency={lat:.6f}s{oom_note}")
    return results


def main():
    check_repository_root()

    ### Build unless explicitly skipped. SKIP_BUILD=1 is useful when BUILD_DIR points at an
    ### already-built tree configured with different cmake flags than CMAKE_FLAGS above.
    if os.environ.get("SKIP_BUILD"):
        printInfo("Skipping build (SKIP_BUILD env var set)")
    else:
        printInfo(f"Building NebulaStream into {config.BUILD_DIR}...")
        compile_nebulastream(_cmake_flags(), config.BUILD_DIR)
        printSuccess("Build complete.")

    if not os.path.exists(SINGLE_NODE_BINARY):
        printError(f"Worker binary not found at {SINGLE_NODE_BINARY}; build the project first (unset SKIP_BUILD)")
        sys.exit(1)
    if not os.path.exists(NES_CLI):
        printError(f"nes-cli not found at {NES_CLI}; build the project first (unset SKIP_BUILD)")
        sys.exit(1)

    ### Build the combined image (idempotent). The runner-side build is convenient for first-time
    ### setup; the .sh wrapper also tries to build it ahead of time. The runtime base must come
    ### first since the combined image FROMs it.
    ensure_runtime_base_image()
    ensure_image()
    ensure_tcpgen_image()

    ### Honor an externally-supplied OUTPUT_DIR so the wrapper can place outputs in a consistently-
    ### named, timestamped directory. Otherwise fall back to creating our own timestamped dir.
    env_output_dir = os.environ.get("OUTPUT_DIR")
    if env_output_dir:
        output_root = env_output_dir
        os.makedirs(output_root, exist_ok=True)
    else:
        output_root = os.path.join(".", f"prometheus_bench_{int(time.time())}")
        create_folder_and_remove_if_exists(output_root)

    ### Which conditions to run. CONDITIONS (csv) > CONDITION (single) > DEFAULT_CONDITIONS.
    env_conditions = os.environ.get("CONDITIONS") or os.environ.get("CONDITION")
    if env_conditions and env_conditions.strip():
        conditions = [c.strip() for c in env_conditions.split(",") if c.strip()]
    else:
        conditions = DEFAULT_CONDITIONS

    printInfo(f"Conditions to run: {conditions}")
    for condition in conditions:
        printInfo(f"========== Starting condition: {condition} ==========")
        _run_condition(condition, output_root)


if __name__ == "__main__":
    main()
