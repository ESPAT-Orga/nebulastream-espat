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
Distributed statistic-collection benchmark.

Sweeps a set of tree topologies (config.TOPOLOGIES, e.g. "1/2", "1/2/4", "1/4", "1/4/8"). For each
topology it brings up one Docker container per worker on a shared bridge network, puts data sources on
the leaves, and measures the INCOMING network bytes to the root (root container eth0 RX) over time for
three variants:

  prometheus  source(leaf) -> Prometheus sink(root)         raw stream reaches root
  split       build(leaf)  -> StatisticStoreWriter(root)    full per-window synopsis reaches root
  local       build(leaf)  -> StatisticStoreWriter(leaf)    only 4 scalar fields per window reach root

Data generators run over loopback inside each leaf's netns, so they don't appear on the root's eth0.

Run from the repository root:
  python3 -m scripts.benchmarking.distributed_statistic_collection.run_benchmark
"""

import csv
import os
import shutil
import subprocess
import threading
import time

from scripts.benchmarking.utils import (
    create_folder_and_remove_if_exists,
    printError,
    printInfo,
    printSuccess,
)
from scripts.benchmarking.distributed_statistic_collection import config

### common.config resolves SINGLE_NODE_EXECUTABLE / NEBULI_EXECUTABLE against NES_BUILD_DIR at import
### time, so propagate our BUILD_DIR before importing it.
os.environ.setdefault("NES_BUILD_DIR", config.BUILD_DIR)

from scripts.benchmarking.common.config import SINGLE_NODE_EXECUTABLE  # noqa: E402
from scripts.benchmarking.common.worker_lifecycle import stop_queries, submit_query  # noqa: E402

SINGLE_NODE_BINARY = os.path.abspath(SINGLE_NODE_EXECUTABLE)


### --- Docker network -----------------------------------------------------

def ensure_network():
    exists = subprocess.run(["docker", "network", "inspect", config.DOCKER_NETWORK], capture_output=True)
    if exists.returncode != 0:
        subprocess.run(["docker", "network", "create", config.DOCKER_NETWORK], check=True, capture_output=True)
        printInfo(f"created docker network {config.DOCKER_NETWORK}")


### --- Worker containers --------------------------------------------------

def _worker_args(worker):
    """Args passed to nes-single-node-worker. Bind gRPC on 0.0.0.0 so the published host port reaches
    it; advertise/bind the data plane on the container's own DNS name so peers can dial it."""
    return [
        f"--grpc=0.0.0.0:{config.CONTAINER_GRPC_PORT}",
        f"--data_address={worker['data_address']}",
        f"--worker.query_engine.number_of_worker_threads={config.worker_threads(worker['role'])}",
        f"--worker.default_query_execution.execution_mode={config.EXECUTION_MODE}",
        f"--worker.number_of_buffers_in_global_buffer_manager={config.NUMBER_OF_BUFFERS}",
        f"--worker.default_query_optimization.join_strategy={config.JOIN_STRATEGY}",
        "--worker.query_engine.admission_queue_size=1000000",
        f"--worker.default_query_execution.page_size={config.PAGE_SIZE}",
        f"--worker.default_query_execution.operator_buffer_size={config.BUFFER_SIZE_BYTES}",
        "--worker.latency_listener=false",
        "--worker.throughput_listener_interval_in_ms=200",
    ]


def start_worker(worker, run_prometheus, num_prom_targets, log_stream):
    name = worker["name"]
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    cmd = [
        "docker", "run", "-d",
        "--name", name,
        "--network", config.DOCKER_NETWORK,
        f"--cpus={worker['cpus']}",
        "-v", f"{SINGLE_NODE_BINARY}:/usr/bin/nes-single-node-worker:ro",
        "-p", f"{worker['host_port']}:{config.CONTAINER_GRPC_PORT}",
    ]
    ### The root needs NET_ADMIN to install the ingress bandwidth cap (contention mode); harmless otherwise.
    if worker["role"] == "root":
        cmd += ["--cap-add=NET_ADMIN"]
    if worker["role"] == "root" and run_prometheus:
        cmd += ["-p", f"{config.PROM_UI_PORT}:{config.PROM_UI_PORT}"]
        cmd += [
            "-e", "NES_RUN_PROMETHEUS=1",
            "-e", f"NES_PROM_PORT_BASE={config.SINK_PORT_BASE}",
            "-e", f"NES_PROM_NUM_TARGETS={num_prom_targets}",
            "-e", f"NES_PROM_SCRAPE_INTERVAL={config.PROMETHEUS_SCRAPE_INTERVAL_SECONDS}s",
        ]
    else:
        cmd += ["-e", "NES_RUN_PROMETHEUS=0"]
    cmd += [config.WORKER_DOCKER_IMAGE]
    cmd += _worker_args(worker)
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    logs_proc = subprocess.Popen(["docker", "logs", "-f", name], stdout=log_stream, stderr=subprocess.STDOUT)
    printInfo(f"started worker {name} (cpus={worker['cpus']}, role={worker['role']}, level={worker['level']})")
    return name, logs_proc


def stop_worker(name, logs_proc):
    subprocess.run(["docker", "stop", "--time", "5", name], capture_output=True)
    try:
        logs_proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        logs_proc.kill()
        logs_proc.wait()
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def apply_ingress_cap(root_name, kbit):
    """Cap the root's aggregate incoming bandwidth to `kbit` kbit by *shaping* it: redirect eth0 ingress
    to an ifb device and put a tbf (token bucket) on it. Shaping queues+delays (rather than dropping like
    police), so the TCP senders (leaf NetworkSinks) are throttled via congestion control and backpressure
    propagates up to the leaf sources — which is what makes the leaf-side throughput reflect the shared
    cap. kbit<=0 leaves it uncapped. Requires --cap-add=NET_ADMIN + tc (iproute2); the kernel auto-loads
    ifb/act_mirred on first use."""
    if kbit <= 0:
        return
    ### Size the burst to the rate: tbf needs burst >= rate/HZ to actually reach `rate` (otherwise the
    ### effective rate collapses to burst*HZ, silently throttling the high Mbit caps), but a burst much
    ### larger than that lets a chunk through instantly and stops a low kbit cap from binding over the
    ### measurement window. Use a conservative HZ=100 so the rate is met on any kernel, with
    ### config.INGRESS_BURST as a ~2-packet floor for the smallest caps.
    burst_bytes = max(config.INGRESS_BURST, (kbit * 1000) // (8 * 100))
    script = (
        "ip link add ifb0 type ifb && ip link set ifb0 up && "
        f"tc qdisc add dev {config.NET_IFACE} handle ffff: ingress && "
        f"tc filter add dev {config.NET_IFACE} parent ffff: protocol ip u32 match u32 0 0 "
        "action mirred egress redirect dev ifb0 && "
        f"tc qdisc add dev ifb0 root tbf rate {kbit}kbit burst {burst_bytes} latency 400ms"
    )
    res = subprocess.run(["docker", "exec", root_name, "sh", "-c", script], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(
            f"failed to apply {kbit}kbit ingress shaping on {root_name} (need NET_ADMIN + tc + ifb): "
            f"{res.stderr.strip() or res.stdout.strip()}"
        )
    printInfo(f"applied {kbit}kbit ingress shaping on {root_name}:{config.NET_IFACE} (ifb0/tbf)")


### --- Per-query TCP generators (one per source) --------------------------

def start_generator(leaf_container, port, gen_name, log_path, rate=None):
    """One rust-tcp-generator joined to the leaf's netns, bound to a single loopback port. `rate` is
    tuples/sec (0 = full speed); defaults to config.GENERATOR_RATE."""
    if rate is None:
        rate = config.GENERATOR_RATE
    subprocess.run(["docker", "rm", "-f", gen_name], capture_output=True)
    cmd = [
        "docker", "run", "-d",
        "--name", gen_name,
        "--network", f"container:{leaf_container}",
        config.TCP_GENERATOR_IMAGE,
        "--port-base", str(port),
        "--num-ports", "1",
        "--rate", str(rate),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    log_stream = open(log_path, "w")
    logs_proc = subprocess.Popen(["docker", "logs", "-f", gen_name], stdout=log_stream, stderr=subprocess.STDOUT)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        time.sleep(0.2)
        try:
            with open(log_path) as f:
                if "READY" in f.read():
                    return gen_name, logs_proc, log_stream
        except FileNotFoundError:
            pass
    logs_proc.kill()
    logs_proc.wait()
    log_stream.close()
    subprocess.run(["docker", "rm", "-f", gen_name], capture_output=True)
    raise RuntimeError(f"TCP generator {gen_name} did not report READY within 30s; see {log_path}")


def stop_generator(gen_name, logs_proc, log_stream):
    subprocess.run(["docker", "stop", "--time", "2", gen_name], capture_output=True)
    try:
        logs_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logs_proc.kill()
        logs_proc.wait()
    log_stream.close()
    subprocess.run(["docker", "rm", "-f", gen_name], capture_output=True)


### --- Root eth0 RX sampler -----------------------------------------------

def _read_rx_bytes(container):
    """RX (received) bytes for the container's non-loopback iface from /proc/net/dev. The 16 numbers
    after the 'iface:' label are 8 RX fields then 8 TX fields; RX bytes is the first value (index 0)."""
    out = subprocess.run(["docker", "exec", container, "cat", "/proc/net/dev"], capture_output=True, text=True)
    if out.returncode != 0:
        return None
    for line in out.stdout.splitlines():
        if ":" not in line:
            continue
        iface, rest = line.split(":", 1)
        if iface.strip() != config.NET_IFACE:
            continue
        fields = rest.split()
        if len(fields) >= 1:
            return int(fields[0])
    return None


class RootRxSampler:
    """Samples the root container's cumulative eth0 RX bytes on an interval in a background thread."""

    def __init__(self, root_name):
        self._root = root_name
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._samples = []  ### list of (elapsed_s, rx_bytes)

    def start(self):
        self._t0 = time.monotonic()
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            elapsed = time.monotonic() - self._t0
            self._samples.append((elapsed, _read_rx_bytes(self._root)))
            self._stop.wait(config.TRAFFIC_SAMPLE_INTERVAL_SECONDS)

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=10)

    def write_csv(self, csv_path):
        """Difference consecutive cumulative counters into bytes/sec. Returns (total_bytes, peak_bps)."""
        rows, total_bytes = [], 0
        for (t_prev, prev), (t_cur, cur) in zip(self._samples, self._samples[1:]):
            dt = t_cur - t_prev
            if dt <= 0 or prev is None or cur is None:
                continue
            delta = max(0, cur - prev)
            total_bytes += delta
            rows.append((t_cur, delta / dt))
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["elapsed_s", "root_rx_bps"])
            for t_cur, bps in rows:
                writer.writerow([f"{t_cur:.3f}", f"{bps:.1f}"])
        peak_bps = max((bps for _, bps in rows), default=0.0)
        return total_bytes, peak_bps


### --- Query rendering ----------------------------------------------------

def _workers_block(workers):
    lines = ["workers:"]
    for w in workers:
        lines.append(f"  - host: {w['host']}")
        lines.append(f"    data_address: {w['data_address']}")
        lines.append(f"    max_operators: {config.MAX_OPERATORS}")
        if w["parent"] is not None:
            lines.append(f"    downstream: [{w['parent']}]")
    return "\n".join(lines)


def _source_block(source_name, leaf_host, tcp_port):
    return f"""logical:
  - name: {source_name}
    schema:
      - name: id
        type: UINT64
      - name: value
        type: UINT64
      - name: timestamp
        type: UINT64
physical:
  - logical: {source_name}
    host: {leaf_host}
    type: TCP
    parser_config:
      type: CSV
    source_config:
      socket_host: 127.0.0.1
      socket_port: {tcp_port}
      flush_interval_ms: {config.FLUSH_INTERVAL_MS}
      connect_timeout_seconds: 10"""


def render_prometheus_query(source_name, leaf_host, tcp_port, sink_port, root_host, workers):
    query = f"SELECT value FROM {source_name} INTO prom_sink;"
    sinks = f"""sinks:
  - name: prom_sink
    host: {root_host}
    schema:
      - name: {source_name}$value
        type: UINT64
    type: Prometheus
    config:
      output_format: NATIVE
      server_url: 127.0.0.1:{sink_port}
      histogram_num_buckets: {config.PROM_HISTOGRAM_NUM_BUCKETS}
      histogram_min_value: {config.HISTOGRAM_MIN}
      histogram_max_value: {config.HISTOGRAM_MAX}
    parser_config: {{}}"""
    return f"query: {query}\n{sinks}\n{_source_block(source_name, leaf_host, tcp_port)}\n{_workers_block(workers)}\n"


def render_statistic_query(variant, source_name, leaf_host, tcp_port, statistic_id, root_host, workers):
    """`split` pins the StatisticStoreWriter to the root via writer_host; `local` omits it so the writer
    stays on the leaf. Both use a void terminal sink pinned to the root (a plain worker cannot receive
    Grpc statistic reports), so the writer's output crosses the network to the root either way."""
    opts = [
        f"{statistic_id} AS statistic_id",
        f'"{root_host}" AS report_host',
        '"void" AS terminal_sink',
        f"{config.HISTOGRAM_MEMORY_BUDGET} AS memory_budget",
    ]
    if variant == "split":
        opts.append(f'"{root_host}" AS writer_host')
    set_clause = ", ".join(opts)
    ### Ingestion time (USE_EVENT_TIME=0) closes windows on wall-clock; event time windows on the
    ### generator's per-tuple counter (the default; the rate limiter makes the cadence predictable).
    event_time = " EVENTTIME timestamp" if config.USE_EVENT_TIME else ""
    query = (
        f"REQUEST STATISTIC DATA {config.STATISTIC_METRIC} ON {source_name}(value) "
        f"WINDOW TUMBLING(size {config.STATISTIC_WINDOW_SIZE_MS} ms){event_time} "
        f"SET ({set_clause});"
    )
    ### sinks must be present (the YAML decoder reads the key unconditionally); the statistic terminal
    ### sink is added by the engine, so the user-facing list is empty.
    return f"query: {query}\nsinks: []\n{_source_block(source_name, leaf_host, tcp_port)}\n{_workers_block(workers)}\n"


def gp_sink_file(source_name):
    """Path (inside the root container) of the File sink for one GP query."""
    return f"/tmp/gp_{source_name}.csv"


def render_gp_query(source_name, leaf_host, tcp_port, root_host, workers):
    """A general-purpose passthrough query: source(leaf) -> projection -> File sink(root). All tuples
    cross leaf->root; the File sink on the root writes one CSV row per *delivered* tuple, so counting
    rows over time measures the GP throughput that actually made it across the (capped) root link —
    unlike the leaf-side throughput listener, which counts tuples the leaf pushes into its buffers
    regardless of what the bottleneck delivers."""
    query = f"SELECT id, value, timestamp FROM {source_name} INTO gp_sink;"
    sinks = f"""sinks:
  - name: gp_sink
    host: {root_host}
    schema:
      - name: {source_name}$id
        type: UINT64
      - name: {source_name}$value
        type: UINT64
      - name: {source_name}$timestamp
        type: UINT64
    type: File
    config:
      file_path: {gp_sink_file(source_name)}
      append: "false"
      output_format: CSV
    parser_config: {{}}"""
    return f"query: {query}\n{sinks}\n{_source_block(source_name, leaf_host, tcp_port)}\n{_workers_block(workers)}\n"


def count_gp_rows(root_name):
    """Total rows written across all GP File sinks on the root = tuples delivered to the root so far."""
    res = subprocess.run(
        ["docker", "exec", root_name, "sh", "-c", "cat /tmp/gp_*.csv 2>/dev/null | wc -l"],
        capture_output=True, text=True,
    )
    try:
        return int(res.stdout.strip())
    except (ValueError, AttributeError):
        return 0




### --- One variant run ----------------------------------------------------

def _sources(leaves):
    """Yield (global_index, leaf, local_index, source_name) for every source across all leaves."""
    gidx = 0
    for leaf in leaves:
        for j in range(config.SOURCES_PER_LEAF):
            yield gidx, leaf, j, f"src_{leaf['name']}_{j}"
            gidx += 1


def run_variant(topology, variant, workers, out_dir):
    printInfo(f"=== topology {topology} | variant {variant} ===")
    root = next(w for w in workers if w["role"] == "root")
    leaves = [w for w in workers if w["role"] == "leaf"]
    work_dir = os.path.join(out_dir, variant)
    create_folder_and_remove_if_exists(work_dir)
    total_sources = config.SOURCES_PER_LEAF * len(leaves)

    worker_log = open(os.path.join(work_dir, "workers.log"), "w")
    cli_log = open(os.path.join(work_dir, "cli.log"), "w")

    generators, started_workers = [], []
    sampler = RootRxSampler(root["name"])
    query_ids, last_query_file = [], None
    try:
        ensure_network()
        run_prometheus = variant == "prometheus"
        for w in workers:
            started_workers.append((start_worker(w, run_prometheus, total_sources, worker_log), w))
        time.sleep(8)  ### let workers come up and register before submitting

        ### One generator per query, in the owning leaf's netns.
        for gidx, leaf, j, source_name in _sources(leaves):
            port = config.TCP_PORT_BASE + j
            gen_name = f"nes-dsc-gen-{variant}-{leaf['name']}-{j}"
            gen_log = os.path.join(work_dir, f"gen_{leaf['name']}_{j}.log")
            generators.append(start_generator(leaf["name"], port, gen_name, gen_log))
        printSuccess(f"started {len(generators)} generators")

        sampler.start()

        ### Submit one query per source, then hold.
        for gidx, leaf, j, source_name in _sources(leaves):
            port = config.TCP_PORT_BASE + j
            query_file = os.path.join(work_dir, f"query_{source_name}.yaml")
            if variant == "prometheus":
                yaml = render_prometheus_query(
                    source_name, leaf["host"], port, config.SINK_PORT_BASE + gidx, root["host"], workers
                )
            else:
                yaml = render_statistic_query(variant, source_name, leaf["host"], port, gidx + 1, root["host"], workers)
            with open(query_file, "w") as f:
                f.write(yaml)
            last_query_file = query_file
            query_ids += submit_query(query_file, cli_log)
        printSuccess(f"submitted {len(query_ids)} queries; holding {config.RUN_DURATION_SECONDS}s")

        time.sleep(config.RUN_DURATION_SECONDS)

    finally:
        sampler.stop()
        if query_ids and last_query_file is not None:
            for proc in stop_queries(query_ids, last_query_file, cli_log):
                proc.wait()
        ### Generators must stop before the leaf netns is torn down.
        for gen_name, logs_proc, log_stream in generators:
            stop_generator(gen_name, logs_proc, log_stream)
        for (name, logs_proc), _ in started_workers:
            stop_worker(name, logs_proc)
        worker_log.close()
        cli_log.close()

    traffic_csv = os.path.join(out_dir, f"{variant}_traffic.csv")
    total_bytes, peak_bps = sampler.write_csv(traffic_csv)
    printSuccess(f"[{topology}/{variant}] root RX bytes={total_bytes} peak_bps={peak_bps:.0f}  -> {traffic_csv}")
    return {
        "topology": topology,
        "variant": variant,
        "total_bytes": total_bytes,
        "peak_bps": peak_bps,
        "num_queries": len(query_ids),
        "num_leaves": len(leaves),
    }


def _gp_sources(leaves):
    """Yield (global_index, leaf, local_index, source_name) for every GP query across all leaves."""
    gidx = 0
    for leaf in leaves:
        for k in range(config.GP_QUERIES_PER_LEAF):
            yield gidx, leaf, k, f"gp_{leaf['name']}_{k}"
            gidx += 1


def run_contention(topology, variant, workers, bandwidth_kbit, out_dir):
    """Run the statistic workload (set by `variant`) AND general-purpose passthrough queries together,
    under a capped root-ingress bandwidth, and measure the GP queries' throughput at the root."""
    bw_label = bandwidth_kbit if bandwidth_kbit > 0 else "inf"
    printInfo(f"=== topology {topology} | variant {variant} | bandwidth {bandwidth_kbit or 'uncapped'} kbit ===")
    root = next(w for w in workers if w["role"] == "root")
    leaves = [w for w in workers if w["role"] == "leaf"]
    work_dir = os.path.join(out_dir, f"{variant}_bw{bw_label}")
    create_folder_and_remove_if_exists(work_dir)
    total_stat_sources = config.SOURCES_PER_LEAF * len(leaves)

    ### Keep the root's log separate from the leaves'/intermediates' for easier debugging.
    root_log = open(os.path.join(work_dir, "root.log"), "w")
    other_log = open(os.path.join(work_dir, "workers.log"), "w")
    cli_log = open(os.path.join(work_dir, "cli.log"), "w")

    generators, started_workers, stat_ids, gp_ids, last_query_file = [], [], [], [], None
    rows_start, rows_end, measured_s = 0, 0, float(config.RUN_DURATION_SECONDS)
    try:
        ensure_network()
        run_prometheus = variant == "prometheus"
        for w in workers:
            log = root_log if w["role"] == "root" else other_log
            started_workers.append((start_worker(w, run_prometheus, total_stat_sources, log), w))
        time.sleep(8)

        ### Generators: statistic sources (rate-limited) + GP sources (full speed, bandwidth-bound).
        for _, leaf, j, _ in _sources(leaves):
            gen = f"nes-dsc-gen-{topology.replace('/', '-')}-{variant}-bw{bw_label}-stat-{leaf['name']}-{j}"
            generators.append(
                start_generator(leaf["name"], config.TCP_PORT_BASE + j, gen,
                                os.path.join(work_dir, f"gen_stat_{leaf['name']}_{j}.log"))
            )
        for _, leaf, k, _ in _gp_sources(leaves):
            gen = f"nes-dsc-gen-{topology.replace('/', '-')}-{variant}-bw{bw_label}-gp-{leaf['name']}-{k}"
            generators.append(
                start_generator(leaf["name"], config.GP_PORT_BASE + k, gen,
                                os.path.join(work_dir, f"gen_gp_{leaf['name']}_{k}.log"), rate=config.GP_GENERATOR_RATE)
            )
        printSuccess(f"started {len(generators)} generators")

        ### Submit the statistic workload (background) then the GP queries (measured).
        for gidx, leaf, j, source_name in _sources(leaves):
            query_file = os.path.join(work_dir, f"query_stat_{source_name}.yaml")
            if variant == "prometheus":
                yaml = render_prometheus_query(
                    source_name, leaf["host"], config.TCP_PORT_BASE + j, config.SINK_PORT_BASE + gidx, root["host"], workers
                )
            else:
                yaml = render_statistic_query(
                    variant, source_name, leaf["host"], config.TCP_PORT_BASE + j, gidx + 1, root["host"], workers
                )
            with open(query_file, "w") as f:
                f.write(yaml)
            last_query_file = query_file
            stat_ids += submit_query(query_file, cli_log)

        for _, leaf, k, source_name in _gp_sources(leaves):
            query_file = os.path.join(work_dir, f"query_gp_{source_name}.yaml")
            with open(query_file, "w") as f:
                f.write(render_gp_query(source_name, leaf["host"], config.GP_PORT_BASE + k, root["host"], workers))
            last_query_file = query_file
            gp_ids += submit_query(query_file, cli_log)
        printSuccess(f"submitted {len(stat_ids)} statistic + {len(gp_ids)} GP queries")

        ### Apply the cap AFTER deployment so the CLI's gRPC deploy traffic to the root isn't policed;
        ### the measurement hold then runs under the capped link.
        apply_ingress_cap(root["name"], bandwidth_kbit)
        ### Settle first (let the shaper drain the pre-cap backlog), then count delivered rows over a
        ### fixed window: rows written to the root's File sinks are tuples that actually crossed the
        ### capped link, so (rows_end - rows_start)/elapsed is the true delivered GP throughput.
        time.sleep(config.MEASURE_SETTLE_MS / 1000.0)
        rows_start = count_gp_rows(root["name"])
        t0 = time.monotonic()
        printInfo(f"holding {config.RUN_DURATION_SECONDS}s under {bandwidth_kbit or 'uncapped'} kbit cap")
        time.sleep(config.RUN_DURATION_SECONDS)
        rows_end = count_gp_rows(root["name"])
        measured_s = max(1e-3, time.monotonic() - t0)

    finally:
        ### Remove the cap before teardown so stop/control gRPC to the root isn't throttled.
        subprocess.run(
            ["docker", "exec", root["name"], "sh", "-c",
             f"tc qdisc del dev {config.NET_IFACE} ingress 2>/dev/null; ip link del ifb0 2>/dev/null; true"],
            capture_output=True,
        )
        all_ids = stat_ids + gp_ids
        if all_ids and last_query_file is not None:
            for proc in stop_queries(all_ids, last_query_file, cli_log):
                proc.wait()
        for gen_name, logs_proc, log_stream in generators:
            stop_generator(gen_name, logs_proc, log_stream)
        for (name, logs_proc), _ in started_workers:
            stop_worker(name, logs_proc)
        root_log.close()
        other_log.close()
        cli_log.close()

    ### Delivered GP throughput = rows that reached the root's File sinks during the capped window.
    gp_tps = (rows_end - rows_start) / measured_s
    printSuccess(f"[{topology}/{variant}/{bandwidth_kbit}kbit] delivered GP throughput={gp_tps:.0f} tup/s")
    return {
        "topology": topology,
        "variant": variant,
        "bandwidth_kbit": bandwidth_kbit,
        "gp_throughput_tps": gp_tps,
        "gp_num_queries": len(gp_ids),
        "num_leaves": len(leaves),
    }


def run_topology(topology, output_root, mode):
    workers = config.build_workers(topology)  ### raises ValueError unless exactly one root
    out_dir = os.path.join(output_root, config.topology_dir(topology))
    ### exist_ok (not wipe): both modes share the per-topology dir with distinct file/subdir names, so
    ### running contention after traffic must not clobber the traffic results.
    os.makedirs(out_dir, exist_ok=True)
    printInfo(f"[{mode}] topology {topology}: {len(workers)} workers ({[w['name'] for w in workers]})")
    if mode == "contention":
        return [
            run_contention(topology, v, workers, bw, out_dir)
            for v in config.VARIANTS
            for bw in config.BANDWIDTH_LIMITS_KBIT
        ]
    return [run_variant(topology, v, workers, out_dir) for v in config.VARIANTS]


def write_summary(output_root, results, mode):
    if mode == "contention":
        path = os.path.join(output_root, "contention_summary.csv")
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["topology", "variant", "bandwidth_kbit", "gp_throughput_tps", "gp_num_queries", "num_leaves"])
            for r in results:
                writer.writerow(
                    [r["topology"], r["variant"], r["bandwidth_kbit"], f"{r['gp_throughput_tps']:.1f}",
                     r["gp_num_queries"], r["num_leaves"]]
                )
        return path
    path = os.path.join(output_root, "summary.csv")
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["topology", "variant", "total_bytes", "peak_bps", "num_queries", "num_leaves"])
        for r in results:
            writer.writerow(
                [r["topology"], r["variant"], r["total_bytes"], f"{r['peak_bps']:.1f}", r["num_queries"], r["num_leaves"]]
            )
    return path


def copy_result_csvs(output_root, dest):
    """Copy every result CSV (summary.csv, contention_summary.csv, and the per-topology
    <topology>/<variant>_traffic.csv files) from output_root into dest, preserving the relative layout
    so the notebook can read them directly. Logs and the in-container GP files are left behind."""
    if os.path.abspath(dest) == os.path.abspath(output_root):
        return
    count = 0
    for dirpath, _dirs, files in os.walk(output_root):
        for f in files:
            if not f.endswith(".csv"):
                continue
            rel = os.path.relpath(os.path.join(dirpath, f), output_root)
            target = os.path.join(dest, rel)
            os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
            shutil.copy2(os.path.join(dirpath, f), target)
            count += 1
    printSuccess(f"copied {count} result CSV(s) to {os.path.abspath(dest)}")


def main():
    output_root = os.environ.get("OUTPUT_DIR") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "results"
    )
    create_folder_and_remove_if_exists(output_root)
    printInfo(f"output dir: {output_root}")
    printInfo(f"modes: {config.MODES} | topologies: {config.TOPOLOGIES}")
    if "contention" in config.MODES:
        printInfo(f"bandwidth caps (kbit, 0=uncapped): {config.BANDWIDTH_LIMITS_KBIT}")

    if not os.path.exists(SINGLE_NODE_BINARY):
        printError(f"worker binary not found: {SINGLE_NODE_BINARY} (set BUILD_DIR / build first)")
        raise SystemExit(1)

    for mode in config.MODES:
        results = []
        for topology in config.TOPOLOGIES:
            results += run_topology(topology, output_root, mode)
        summary_path = write_summary(output_root, results, mode)
        printSuccess(f"[{mode}] summary -> {summary_path}")

    ### Also drop the result CSVs into a second location (default: this experiment's plots/ dir, which is
    ### in the synced repo tree) so the notebook can read them without a manual copy. Set RESULTS_COPY_DIR
    ### to override, or to OUTPUT_DIR to disable the second copy.
    results_copy_dir = os.environ.get(
        "RESULTS_COPY_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "plots")
    )
    copy_result_csvs(output_root, results_copy_dir)
    printSuccess("done.")


if __name__ == "__main__":
    main()
