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

"""Network-sink benchmark driver.

Spawns two single-node-workers locally (worker-1 = sink side, worker-2 = source side), submits one
HIGH-priority and one LOW-priority generator → Network sink query through nes-cli, lets them run for
a fixed duration, and parses per-query throughput from the worker logs.

The sweep iterates over (max_network_rate, generator_rate_type, sending_strategy). Optional `tc qdisc
tbf` throttling on the loopback interface is best-effort: the run logs a warning and proceeds
unthrottled if it lacks NET_ADMIN.
"""

import argparse
import csv
import itertools
import os
import re
import subprocess
import time
from string import Template

from scripts.benchmarking.common._console import banner, step
from scripts.benchmarking.common.config import (
    NEBULI_EXECUTABLE,
    SINGLE_NODE_EXECUTABLE,
    THROUGHPUT_LISTENER_INTERVAL,
    WAIT_BETWEEN_COMMANDS_LONG,
)
from scripts.benchmarking.common.worker_lifecycle import (
    parse_log_to_throughput_csv,
    terminate_process_if_exists,
)
from scripts.benchmarking.network_sink.config import (
    GENERATOR_RATE_CONFIGS,
    GENERATOR_RATE_TYPES,
    MAX_NETWORK_RATES,
    NUM_RUNS_PER_EXPERIMENT,
    QUERY_DURATION_SEC,
    QUERY_TEMPLATES_DIR,
    RESULT_FIELDNAMES,
    SENDING_STRATEGIES,
    WORKER_1_DATA,
    WORKER_1_GRPC,
    WORKER_2_DATA,
    WORKER_2_GRPC,
)
from scripts.benchmarking.utils import (
    check_repository_root,
    create_folder_and_remove_if_exists,
    printError,
    printInfo,
    printSuccess,
)


def _spawn_worker(grpc_addr, data_addr, strategy, log_path):
    """Start a single-node-worker. Returns the Popen object."""
    cmd = [
        SINGLE_NODE_EXECUTABLE,
        f"--grpc={grpc_addr}",
        f"--data_address={data_addr}",
        f"--network_sink_sending_strategy={strategy}",
        f"--worker.throughput_listener_interval_in_ms={THROUGHPUT_LISTENER_INTERVAL}",
    ]
    log_file = open(log_path, "w")
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
    return proc, log_file


def _apply_tc_throttle(rate, ctx):
    """Best-effort tc qdisc throttle on loopback. Returns True on success, False otherwise.

    Stores any allocated state in *ctx* (dict) so _remove_tc_throttle can clean up.
    """
    if rate == "none":
        return True
    cmd = ["sudo", "-n", "tc", "qdisc", "add", "dev", "lo", "root", "tbf",
           "rate", rate, "burst", "32kbit", "latency", "400ms"]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=5)
        if result.returncode != 0:
            printError(f"tc qdisc add failed (rc={result.returncode}): {result.stderr.decode().strip()}")
            return False
        ctx["tc_active"] = True
        return True
    except FileNotFoundError:
        printError("tc/sudo not found; running unthrottled")
        return False
    except subprocess.TimeoutExpired:
        printError("tc qdisc add timed out; running unthrottled")
        return False


def _remove_tc_throttle(ctx):
    if not ctx.get("tc_active"):
        return
    subprocess.run(["sudo", "-n", "tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True, timeout=5)
    ctx["tc_active"] = False


def _render_template(template_path, dest_path, substitutions):
    with open(template_path) as fh:
        rendered = Template(fh.read()).safe_substitute(substitutions)
    with open(dest_path, "w") as fh:
        fh.write(rendered)


def _submit_query(yaml_path, target_grpc):
    """Submit a query via nes-cli and return its query id (as printed on stdout). None on failure."""
    cmd = list(NEBULI_EXECUTABLE) + ["-t", yaml_path, "start"]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
    except subprocess.TimeoutExpired:
        printError(f"nes-cli start timed out for {yaml_path}")
        return None
    stdout_text = result.stdout.decode(errors="replace").strip()
    stderr_text = result.stderr.decode(errors="replace").strip()
    if result.returncode != 0:
        printError(
            f"nes-cli start failed (rc={result.returncode}) for {yaml_path}\n"
            f"  stdout: {stdout_text[-500:]}\n"
            f"  stderr: {stderr_text[-500:]}")
        return None
    # The CLI prints the query id on stdout. Take the last non-empty line.
    for line in reversed(stdout_text.splitlines()):
        line = line.strip()
        if line:
            return line
    return None


def _parse_throughput_by_local_id_in_order(log_path):
    """Parse the worker log for per-(local-query-id) average throughput in tuples/s.

    Returns a list of (local_id, mean_throughput) ordered by first appearance in the log. The order
    matches the order in which queries were registered on the worker, which lets the caller attribute
    the first entry to the first-submitted (HIGH) query and the second to the LOW query.
    """
    if not os.path.exists(log_path):
        return []
    pattern = re.compile(
        r"Throughput for queryId QueryId\(local=(?P<local>[0-9a-f-]+).*?\) in window .*? is (?P<val>[0-9.]+) (?P<unit>[kMG]?)Tup/s"
    )
    sums = {}
    counts = {}
    order = []
    for line in open(log_path):
        match = pattern.search(line)
        if not match:
            continue
        local_id = match.group("local")
        unit = match.group("unit")
        scale = {"": 1, "k": 1e3, "M": 1e6, "G": 1e9}.get(unit, 1.0)
        value = float(match.group("val")) * scale
        if local_id not in sums:
            sums[local_id] = 0.0
            counts[local_id] = 0
            order.append(local_id)
        sums[local_id] += value
        counts[local_id] += 1
    return [(local_id, sums[local_id] / counts[local_id]) for local_id in order if counts[local_id] > 0]


def run_single_trial(*, run_idx, max_rate, rate_type, strategy, output_dir):
    """Execute one (max_rate, rate_type, strategy) trial. Returns a list of result rows."""
    rate_config = GENERATOR_RATE_CONFIGS.get(rate_type, "emit_rate 100000")
    run_label = f"run{run_idx}_{strategy}_{rate_type}_{max_rate}"
    run_dir = os.path.join(output_dir, run_label)
    os.makedirs(run_dir, exist_ok=True)

    high_yaml = os.path.join(run_dir, "high.yaml")
    low_yaml = os.path.join(run_dir, "low.yaml")
    substitutions = {"GENERATOR_RATE_TYPE": rate_type, "GENERATOR_RATE_CONFIG": rate_config}
    _render_template(os.path.join(QUERY_TEMPLATES_DIR, "HighPriority_Generator.yaml.template"), high_yaml, substitutions)
    _render_template(os.path.join(QUERY_TEMPLATES_DIR, "LowPriority_Generator.yaml.template"), low_yaml, substitutions)

    worker1_log = os.path.join(run_dir, "worker-1.log")
    worker2_log = os.path.join(run_dir, "worker-2.log")
    workers = []
    tc_ctx = {}
    issue = ""

    try:
        with step(f"start workers ({strategy})") as info:
            workers.append(_spawn_worker(WORKER_1_GRPC, WORKER_1_DATA, strategy, worker1_log))
            workers.append(_spawn_worker(WORKER_2_GRPC, WORKER_2_DATA, strategy, worker2_log))
            time.sleep(WAIT_BETWEEN_COMMANDS_LONG)
            info(f"workers up: 1@{WORKER_1_GRPC} 2@{WORKER_2_GRPC}")

        with step(f"apply tc qdisc rate={max_rate}"):
            if not _apply_tc_throttle(max_rate, tc_ctx) and max_rate != "none":
                issue = "tc-throttle-failed"

        with step("submit HIGH and LOW queries") as info:
            high_qid = _submit_query(high_yaml, WORKER_1_GRPC) or "high-submit-failed"
            low_qid = _submit_query(low_yaml, WORKER_1_GRPC) or "low-submit-failed"
            info(f"high={high_qid} low={low_qid}")

        with step(f"run for {QUERY_DURATION_SEC}s"):
            time.sleep(QUERY_DURATION_SEC)

    finally:
        with step("teardown"):
            _remove_tc_throttle(tc_ctx)
            for proc, log_file in workers:
                terminate_process_if_exists(proc)
                log_file.close()

    # The throughput listener emits per-local-query-id measurements on the source-side worker (worker-2).
    # We parse those, ordered by first appearance, and attribute the first to HIGH (submitted first)
    # and the second to LOW.
    rows = []
    parse_log_to_throughput_csv(worker1_log, os.path.join(run_dir, "worker-1_throughput.csv"))
    parse_log_to_throughput_csv(worker2_log, os.path.join(run_dir, "worker-2_throughput.csv"))
    ordered = _parse_throughput_by_local_id_in_order(worker2_log)
    high_local = ordered[0][0] if len(ordered) >= 1 else high_qid
    low_local = ordered[1][0] if len(ordered) >= 2 else low_qid
    high_throughput = ordered[0][1] if len(ordered) >= 1 else 0.0
    low_throughput = ordered[1][1] if len(ordered) >= 2 else 0.0

    rows.append({
        "run_idx": run_idx,
        "max_network_rate": max_rate,
        "rate_type": rate_type,
        "rate_config": rate_config,
        "strategy": strategy,
        "query_id": high_local,
        "priority": "HIGH",
        "throughput_tuples_per_s": high_throughput,
        "duration_s": QUERY_DURATION_SEC,
        "issue": issue,
    })
    rows.append({
        "run_idx": run_idx,
        "max_network_rate": max_rate,
        "rate_type": rate_type,
        "rate_config": rate_config,
        "strategy": strategy,
        "query_id": low_local,
        "priority": "LOW",
        "throughput_tuples_per_s": low_throughput,
        "duration_s": QUERY_DURATION_SEC,
        "issue": issue,
    })
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=".", help="Where to write results_network_sink.csv and per-run logs")
    parser.add_argument("--num-runs", type=int, default=NUM_RUNS_PER_EXPERIMENT)
    parser.add_argument("--strategies", nargs="+", default=SENDING_STRATEGIES)
    parser.add_argument("--rate-types", nargs="+", default=GENERATOR_RATE_TYPES)
    parser.add_argument("--max-network-rates", nargs="+", default=MAX_NETWORK_RATES)
    args = parser.parse_args()

    check_repository_root()
    create_folder_and_remove_if_exists(args.output_dir)
    csv_path = os.path.join(args.output_dir, "results_network_sink.csv")
    with open(csv_path, "w", newline="") as fh:
        csv.DictWriter(fh, fieldnames=RESULT_FIELDNAMES).writeheader()

    trials = list(itertools.product(args.strategies, args.rate_types, args.max_network_rates))
    banner(f"network-sink benchmark: {len(trials) * args.num_runs} runs")

    for run_idx in range(args.num_runs):
        for strategy, rate_type, max_rate in trials:
            printInfo(f"--- run {run_idx + 1}/{args.num_runs}  strategy={strategy}  rate_type={rate_type}  rate={max_rate} ---")
            rows = run_single_trial(
                run_idx=run_idx,
                max_rate=max_rate,
                rate_type=rate_type,
                strategy=strategy,
                output_dir=args.output_dir,
            )
            with open(csv_path, "a", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=RESULT_FIELDNAMES)
                for row in rows:
                    writer.writerow(row)

    printSuccess(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()
