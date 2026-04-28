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
Single-node-worker lifecycle helpers shared by run_statistic_build.py and
run_statistic_probe.py.

Wraps the standalone-worker flow used by both benchmarks: starting the worker
under systemd-run, submitting queries via nes-cli, polling the worker stdout
log for throughput/latency lines, stopping queries, and tearing the worker
down.
"""

import csv
import json
import os
import re
import subprocess
import time

from scripts.benchmarking.utils import convert_unit_prefix, printError
from scripts.benchmarking.e2e.configs import (
    WAIT_BETWEEN_COMMANDS_LONG,
    WAIT_CHECK_INTERVAL_S,
    WAIT_STABLE_CHECKS,
    nebuli_executable,
    single_node_executable,
    throughputListenerInterval,
)


def terminate_process_if_exists(process):
    try:
        process.terminate()
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        printError(f"Process with PID {process.pid} did not terminate within timeout. Sending SIGKILL.")
        process.kill()
        try:
            process.wait(timeout=10)
            printError(f"Process with PID {process.pid} forcefully killed.")
        except subprocess.TimeoutExpired:
            printError(f"Process with PID {process.pid} could not be killed even with SIGKILL (D-state?).")
    # systemd-run --scope does not forward signals to the child process, so the
    # actual nes-single-node-worker may still be running after we kill systemd-run.
    # Forcibly kill any remaining instances to avoid blocking the next experiment.
    subprocess.run(["pkill", "-9", "-x", "nes-single-node-worker"], capture_output=True)
    # Brief pause after process exit to let the OS fully release the port.
    time.sleep(2)


def start_single_node_worker(file_path_stdout, numberOfWorkerThreads, executionMode,
                             joinStrategy, pageSize, bufferSizeInBytes,
                             buffersInGlobalBufferManager, enableLatency=False,
                             statisticStoreType="SUB_STORES", cli_log_file=None):
    """Start the single node worker with the given configuration.

    When *cli_log_file* is given (a writable file handle), the launching
    command is recorded there alongside the nes-cli submit/stop commands so a
    run's full command history lives in a single log file.
    """
    worker_config = (f"--grpc=localhost:8080 "
                     f"--data_address=localhost:9090 "
                     f"--worker.query_engine.number_of_worker_threads={numberOfWorkerThreads} "
                     f"--worker.default_query_execution.execution_mode={executionMode} "
                     f"--worker.number_of_buffers_in_global_buffer_manager={buffersInGlobalBufferManager} "
                     f"--worker.default_query_optimization.join_strategy={joinStrategy} "
                     f"--worker.query_engine.admission_queue_size=1000000 "
                     f"--worker.default_query_execution.page_size={pageSize} "
                     f"--worker.default_query_execution.operator_buffer_size={bufferSizeInBytes} "
                     f"--worker.latency_listener={enableLatency} "
                     f"--worker.statistic_store_type={statisticStoreType} "
                     f"--worker.throughput_listener_interval_in_ms={throughputListenerInterval}")

    cmd = f"systemd-run --user --scope --quiet {single_node_executable} {worker_config}"
    if cli_log_file is not None:
        cli_log_file.write(f"=== Start worker: {cmd} ===\n")
        cli_log_file.flush()
    process = subprocess.Popen(cmd.split(" "), stdout=file_path_stdout, stderr=subprocess.STDOUT)
    pid = process.pid

    # Verify the worker is still alive after startup
    time.sleep(3)
    if process.poll() is not None:
        raise RuntimeError(f"Worker process exited immediately with code {process.returncode}")

    return process


_QUERY_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _extract_query_ids(stdout):
    """Pull query ids out of nes-cli stdout, ignoring debug log lines.

    Query ids are snake_case identifiers (e.g. ``noble_appaloosa``); when
    ``--debug`` is in effect nes-cli also emits ANSI-colored ``[D]`` log lines
    plus the operator-tree visualization to stdout, neither of which match the
    id regex.
    """
    return [line.strip() for line in stdout.splitlines() if _QUERY_ID_RE.fullmatch(line.strip())]


def submit_query(query_file, cli_log_file, retries=3, retry_delay=5):
    """Submit queries via nes-cli and return a list of query ids.

    The YAML file may contain multiple queries; nes-cli emits one id per line
    on stdout, but with ``--debug`` it also emits debug logs and the
    operator-tree visualization there. We filter to snake_case ids only.
    """
    cmd = nebuli_executable + ["-t", query_file, "start"]
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True, timeout=60)
            query_ids = _extract_query_ids(result.stdout)
            cli_log_file.write(f"=== Submit query: {cmd} ===\n")
            cli_log_file.write(f"stdout: {result.stdout}\n")
            cli_log_file.write(f"stderr: {result.stderr}\n")
            cli_log_file.flush()
            if not query_ids:
                printError(f"No query id parsed from nes-cli stdout. Raw stdout below:\n{result.stdout}")
                raise RuntimeError(f"nes-cli returned no parseable query id for {query_file}")
            return query_ids
        except subprocess.CalledProcessError as e:
            last_error = e
            cli_log_file.write(f"=== Submit query FAILED (attempt {attempt}/{retries}): {cmd} ===\n")
            cli_log_file.write(f"exit status: {e.returncode}\n")
            cli_log_file.write(f"stdout: {e.stdout}\n")
            cli_log_file.write(f"stderr: {e.stderr}\n")
            cli_log_file.flush()
            if attempt < retries:
                printError(f"Submit attempt {attempt}/{retries} failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                printError(f"Command failed with exit status: {e.returncode}")
                printError(f"Standard output: {e.stdout}")
                printError(f"Error output: {e.stderr}")
                raise RuntimeError(f"nes-cli submit failed for {query_file}")


def stop_queries(query_ids, query_file, cli_log_file):
    """Stop one or more queries via nes-cli.  Returns list of Popen processes."""
    processes = []
    for qid in query_ids:
        cmd = nebuli_executable + ["-t", query_file, "stop", qid]
        cli_log_file.write(f"=== Stop query: {' '.join(cmd)} ===\n")
        cli_log_file.flush()
        processes.append(subprocess.Popen(cmd, stdout=cli_log_file, stderr=cli_log_file))
    return processes


def get_query_status(query_id, query_file):
    """Query the status of a single query via nes-cli. Returns the status string or None."""
    cmd = nebuli_executable + ["-t", query_file, "status", query_id]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            statuses = json.loads(result.stdout)
            if statuses and len(statuses) > 0:
                return statuses[0].get("query_status")
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
        pass
    return None


def stop_queries_and_wait(query_ids, query_file, cli_log_file, timeout=30):
    """Stop queries and poll until all report status 'Stopped'.

    Returns True if all queries stopped within the timeout, False otherwise.
    """
    stop_procs = stop_queries(query_ids, cli_log_file=cli_log_file, query_file=query_file)
    for proc in stop_procs:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            printError(f"Stop command timed out")

    deadline = time.time() + timeout
    remaining = set(query_ids)
    while remaining and time.time() < deadline:
        for qid in list(remaining):
            status = get_query_status(qid, query_file)
            if status == "Stopped":
                remaining.discard(qid)
        if remaining:
            time.sleep(1)

    if remaining:
        printError(f"Queries did not stop within {timeout}s: {remaining}")
        return False
    return True


def _line_matches_query_ids(line, id_set):
    """Return True if *line* mentions one of the given (distributed) query ids.

    The worker emits throughput/latency lines using the new QueryId format,
    e.g. ``QueryId(local=<UUID>, distributed=<NAME>)``.  The ids returned by
    nes-cli are the *distributed* names (e.g. ``free_haflinger``), so we
    match on the substring ``distributed=<NAME>)``.
    """
    if id_set is None:
        return True
    return any(f"distributed={qid})" in line for qid in id_set)


def wait_for_query_to_finish(query_ids, query_file, *, max_wait=300, worker_process=None,
                             stable_checks=WAIT_STABLE_CHECKS,
                             check_interval_s=WAIT_CHECK_INTERVAL_S):
    """Wait until every query in *query_ids* reports status 'Stopped' on
    *stable_checks* consecutive polls, *check_interval_s* apart.

    A file-source query flips its status to "Stopped" as soon as the source
    exhausts; this loop spots that within ~``stable_checks * check_interval_s``
    of it happening (default ~500 ms).

    Returns a (success, reason) tuple:
      (True, "ok")           — query finished and reported Stopped consistently
      (False, "crashed:N")   — worker exited with code N before the query stopped
      (False, "timeout")     — max_wait exceeded
    """
    if isinstance(query_ids, str):
        query_ids = [query_ids]

    wait_start = time.time()
    consecutive_stopped = 0

    while True:
        if time.time() - wait_start > max_wait:
            printError(f"Max wait time ({max_wait}s) exceeded waiting for queries {query_ids} to stop.")
            return False, "timeout"

        if worker_process is not None and worker_process.poll() is not None:
            printError(f"Worker process exited with code {worker_process.returncode} (likely crashed).")
            return False, f"crashed:{worker_process.returncode}"

        all_stopped = all(get_query_status(qid, query_file) == "Stopped" for qid in query_ids)
        if all_stopped:
            consecutive_stopped += 1
            if consecutive_stopped >= stable_checks:
                return True, "ok"
        else:
            consecutive_stopped = 0

        time.sleep(check_interval_s)


def check_log_for_buffer_exhaustion(log_file_path):
    """Check if the worker log contains buffer exhaustion markers."""
    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                if 'BUFFER_EXHAUSTION' in line:
                    return True
    except FileNotFoundError:
        pass
    return False


def dump_worker_log_tail(log_file_path, lines=80):
    """Print the tail of the worker stdout log so silent failures (e.g. with
    NES_LOG_LEVEL=LEVEL_NONE) leave at least some breadcrumbs in the script
    output.  When the log is empty or missing we say so explicitly.
    """
    try:
        with open(log_file_path, 'r') as f:
            content = f.readlines()
    except FileNotFoundError:
        printError(f"Worker log {log_file_path} does not exist.")
        return
    if not content:
        printError(f"Worker log {log_file_path} is empty (worker produced no stdout).")
        return
    tail = content[-lines:]
    printError(f"--- Last {len(tail)} lines of {log_file_path} ---")
    for line in tail:
        print(line.rstrip())
    printError(f"--- end of {log_file_path} ---")


def parse_average_throughput_from_throughput_listener(log_file_path, query_ids=None):
    """Parse throughput from the worker log file for one or more query ids.

    *query_ids* may be a single id string or a list.  When multiple ids are
    given the throughput measurements of all matching queries are combined
    (summed per window, then averaged across windows).
    """
    if query_ids is not None:
        if isinstance(query_ids, str):
            query_ids = [query_ids]
        id_set = set(str(qid) for qid in query_ids)
    else:
        id_set = None

    # Worker emits queryId in the form QueryId(local=<UUID>[, distributed=<NAME>])
    log_pattern = re.compile(
        r'Throughput for queryId QueryId\([^)]*\) in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s'
    )

    data = []
    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                match = log_pattern.match(line)
                if match:
                    if not _line_matches_query_ids(line, id_set):
                        continue
                    throughput_value = float(match.group(3))
                    unit_prefix = match.group(4)
                    throughput_value = convert_unit_prefix(throughput_value, unit_prefix)
                    data.append(throughput_value)
    except FileNotFoundError:
        printError(f"Log file {log_file_path} not found.")
        return -1

    # Drop the last measurement (may be partial), but keep at least one
    if len(data) > 1:
        data = data[:-1]

    if len(data) == 0:
        printError(f"No throughput measurements found in {log_file_path}.")
        return -1
    return sum(data) / len(data)


def parse_average_latency_from_latency_listener(log_file_path, query_ids=None):
    """Parse latency measurements from the worker log file for one or more query ids.

    Expects lines like:
      Latency for queryId QueryId(local=<UUID>, distributed=<NAME>) and 1 tasks over duration 12345-12346 is 1.234 ms

    Returns the average latency in seconds, or -1 if no measurements found.
    """
    if query_ids is not None:
        if isinstance(query_ids, str):
            query_ids = [query_ids]
        id_set = set(str(qid) for qid in query_ids)
    else:
        id_set = None

    log_pattern = re.compile(
        r'Latency for queryId QueryId\([^)]*\) and (\d+) tasks over duration (\d+)-(\d+) is (\d+\.\d+) (\w*)s'
    )
    unit_multipliers = {'': 1.0, 'm': 1e-3, 'u': 1e-6, 'n': 1e-9}
    data = []
    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                match = log_pattern.match(line)
                if match:
                    if not _line_matches_query_ids(line, id_set):
                        continue
                    latency_value = float(match.group(4))
                    unit_prefix = match.group(5)
                    multiplier = unit_multipliers.get(unit_prefix, 1.0)
                    data.append(latency_value * multiplier)
    except FileNotFoundError:
        printError(f"Log file {log_file_path} not found.")
        return -1

    if len(data) == 0:
        return -1
    return sum(data) / len(data)


def parse_log_to_throughput_csv(log_file_path, csv_file_path):
    """Parse throughput from the worker log and write to CSV."""
    # Worker emits queryId in the form QueryId(local=<UUID>[, distributed=<NAME>])
    log_pattern = re.compile(
        r'Throughput for queryId QueryId\(local=([^,)]+)(?:, distributed=([^)]+))?\) '
        r'in window (\d+)-(\d+) is (\d+\.\d+) (\w*)Tup/s'
    )

    data = []
    with open(log_file_path, mode='r') as log_file:
        for line in log_file:
            match = log_pattern.match(line)
            if match:
                # Prefer the human-readable distributed name when present, else local UUID
                query_id = match.group(2) or match.group(1)
                start_timestamp = int(match.group(3))
                throughput_value = float(match.group(5))
                unit_prefix = match.group(6)
                throughput_value = convert_unit_prefix(throughput_value, unit_prefix)
                data.append((start_timestamp, query_id, throughput_value))

    if len(data) == 0:
        return

    min_timestamp = min(data, key=lambda x: x[0])[0]

    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['normalized_timestamp', 'query_id', 'throughput'])
        for start_timestamp, query_id, throughput in data:
            normalized_timestamp = start_timestamp - min_timestamp
            writer.writerow([normalized_timestamp, query_id, throughput])
