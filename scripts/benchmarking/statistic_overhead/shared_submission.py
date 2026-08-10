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

"""Shared-source submission: one nes-repl per analytical query, k of them carrying a companion.

The analytical query is ClusterMonitoring Q2 — a sliding-window SUM grouped by `jobId`. The
statistic query is an equi-width histogram over that same `jobId`, deployed as a *companion*:
nes-repl hands the request to StatisticCoordinator::collectWorkloadStatistic, which splices the
histogram build branch onto the data query's own source operator. The branch is still its own query,
but its source carries SpliceToRunningSourceTrait, so the worker redirects it into the data query's
already-running source thread instead of spawning a second one.

That is the whole point of the experiment: the histograms cost NO extra TCP connections, so the run
holds a constant NUM_ANALYTICAL_QUERIES connections at every point of the sweep. Verify it with
`grep -c '^accept' generator.log`.

Two structural constraints, both learned the hard way:

- **One REPL process per analytical query.** StatisticCoordinator is constructed per REPL process
  (ReplStarter.cpp) and its statistic registry is per-instance. Ten queries in one session would
  collide: every `jobId` companion hashes to the same registry key (the REPL leaves
  WorkloadDomain::queryId invalid), so only the first would get a build branch.
- **One companion per query.** Two companions on one query trip a defect where the first never
  splices — see KNOWN_ISSUE_companion_splice.md. Single-source + single-companion is the
  configuration the adaptive-optimization benchmark exercises today.

Companions are all-or-nothing per session, so the sweep varies how many of the ten sessions run with
`--companion-statistic`: k sessions with, 10-k without, giving exactly k statistic queries.
"""

import json
import os
import re
import subprocess
import threading
import time

from scripts.benchmarking.statistic_overhead import config
from scripts.benchmarking.utils import printError, printInfo

REPL_EXECUTABLE = os.path.join(config.build_dir, "nes-frontend/apps/nes-repl")

WORKER_GRPC = "localhost:8080"
WORKER_DATA = "localhost:9090"

# nes-repl -f JSON prints the deployed query id as a single-element list.
_JSON_QUERY_ID_RE = re.compile(r'\[\{"query_id":\s*"([^"]+)"\}\]')


def _stream_output(proc, sink_lines, log_file):
    """Drain a REPL's stdout into a list (for id parsing) and a log file (for postmortems)."""
    for raw in iter(proc.stdout.readline, b""):
        line = raw.decode(errors="replace").rstrip("\n")
        sink_lines.append(line)
        log_file.write(line + "\n")
        log_file.flush()


def source_name(session):
    """Logical source name for one session. Must be unique worker-wide — see setup_sql."""
    return f"cluster_{session}"


def setup_sql(session):
    """DDL for one session: its own logical source `cluster_<i>`.

    The per-session naming is REQUIRED, not cosmetic. RunningSourceRegistry is keyed by logical
    source name and is worker-wide, and it strictly refuses a second live source for a name that
    already has one ("the splice contract assumes a single source thread per logical name",
    RunningSourceRegistry.cpp). Ten queries all reading a source called `cluster` would abort the
    worker on the second registration. Distinct names give each query its own registry entry.

    Every session's physical source dials the same generator port, so the connection count is one
    per analytical query however many companions are attached.

    Schema mirrors monitoringClusterData in nes-systests/benchmark/ClusterMonitoring.test, except
    that `constraints` is INT16 rather than BOOLEAN — Q2 never reads it, and this avoids depending
    on how the CSV parser spells booleans. Sink schema fields are dot-qualified and uppercase.

    The window-bound columns must be backtick-quoted: START and END are reserved tokens in
    AntlrSQL.g4, and unquoted they fail to parse. The REPL reports nothing at all when that
    happens — it simply stops consuming statements — so a bare stall after the last CREATE
    PHYSICAL SOURCE is the signature of a bad DDL statement, not of a hung worker.
    """
    source = source_name(session)
    qualifier = source.upper()
    return f"""\
CREATE WORKER "{WORKER_GRPC}" SET ('{WORKER_DATA}' AS DATA);
CREATE LOGICAL SOURCE {source}(creationTS UINT64 NOT NULL, jobId UINT64 NOT NULL, taskId UINT64 NOT NULL, \
machineId INT64 NOT NULL, eventType INT16 NOT NULL, userId INT16 NOT NULL, category INT16 NOT NULL, \
priority INT16 NOT NULL, cpu FLOAT64 NOT NULL, ram FLOAT64 NOT NULL, disk FLOAT64 NOT NULL, \
constraints INT16 NOT NULL);
CREATE PHYSICAL SOURCE FOR {source}
TYPE TCP
SET(
    'CSV' as PARSER.`TYPE`,
    '127.0.0.1' AS `SOURCE`.SOCKET_HOST,
    '{config.CLUSTER_PORT}' AS `SOURCE`.SOCKET_PORT,
    '{config.TCP_FLUSH_INTERVAL_MS}' AS `SOURCE`.FLUSH_INTERVAL_MS,
    '{config.TCP_CONNECT_TIMEOUT_SECONDS}' AS `SOURCE`.CONNECT_TIMEOUT_SECONDS,
    '{WORKER_GRPC}' AS `SOURCE`.HOST
);
CREATE SINK agg_void_sink({qualifier}.`START` UINT64 NOT NULL, {qualifier}.`END` UINT64 NOT NULL, \
{qualifier}.TOTALCPU FLOAT64 NOT NULL, {qualifier}.JOBID UINT64 NOT NULL)
TYPE Void
SET(
    '{WORKER_GRPC}' AS `SINK`.HOST
);
"""


def analytical_sql(session):
    """ClusterMonitoring Q2, verbatim in shape from nes-systests/benchmark/ClusterMonitoring.test."""
    source = source_name(session)
    return (
        "SELECT start, end, SUM(cpu) AS totalCpu, jobId "
        f"FROM {source} "
        "WHERE eventType == INT16(3) "
        "GROUP BY jobId "
        f"WINDOW SLIDING(creationTS, SIZE {config.WINDOW_SIZE_SEC} SEC, "
        f"ADVANCE BY {config.WINDOW_ADVANCE_SEC} SEC) "
        "INTO agg_void_sink;\n"
    )


def _companion_args(session):
    """Flags attaching one histogram over the GROUP BY key to this session's query.

    No --companion-source: the data query has a single source, so StatisticCoordinator's existing
    single-source path resolves the splice target without help. MinVal maps to Equi_Width_Histogram
    (DefaultStatisticQueryGenerator::toStatisticType), and the histogram bounds options are
    documented against it.

    DO NOT REMOVE --companion-switch-to-sql AND --companion-condition. They look redundant — we pass
    the query's own text as the "alternate" plan, and a bin-count threshold no histogram can ever
    reach, so no workload switch ever happens. They are here because Repl.cpp has two companion
    paths and only the switchable one works: without a paired SQL the merged plan takes the plain
    "observability without runtime swap" path, and on that path the data query's source never starts
    at all — it waits on DeferSourceStartTrait forever and the query produces nothing. Measured on
    one query with one companion: 0 throughput windows on the plain path, 1649 with the switch.
    See KNOWN_ISSUE_companion_splice.md.
    """
    return [
        "--companion-statistic",
        "--companion-metric", config.COMPANION_METRIC,
        "--companion-field", "jobId",
        "--companion-window-size-ms", str(config.COMPANION_WINDOW_SIZE_SEC * 1000),
        "--companion-event-time-field", config.COMPANION_EVENT_TIME_FIELD,
        "--companion-histogram-min", "0",
        "--companion-histogram-max", str(config.JOB_DOMAIN),
        "--companion-host", WORKER_GRPC,
        # Engages deployWithSwitchableAlternate; the "switch" is to the identical plan and is gated
        # on a condition that never fires.
        "--companion-switch-to-sql", analytical_sql(session).strip(),
        "--companion-condition", f"BINCOUNTER > UINT64({config.COMPANION_NEVER_FIRE_THRESHOLD})",
    ]


def submit_shared(num_statistic_queries, run_dir):
    """Start one nes-repl per analytical query; the first k carry a companion pair.

    Returns (processes, threads, log_handles, analytical_query_ids). Everything else the worker
    reports throughput for is a spliced statistic branch.
    """
    sessions_with_companions = num_statistic_queries
    if sessions_with_companions > config.NUM_ANALYTICAL_QUERIES:
        raise ValueError(f"{num_statistic_queries} statistic queries needs as many sessions, but "
                         f"only {config.NUM_ANALYTICAL_QUERIES} analytical queries exist "
                         f"(one companion per query — see KNOWN_ISSUE_companion_splice.md)")

    processes, threads, logs, all_lines = [], [], [], []

    for i in range(config.NUM_ANALYTICAL_QUERIES):
        with_companion = i < sessions_with_companions
        sql = setup_sql(i) + analytical_sql(i)
        cmd = [REPL_EXECUTABLE, "-f", "JSON", "-s", WORKER_GRPC,
               "--on-exit", "STOP_QUERIES"]
        if with_companion:
            cmd += _companion_args(i)

        log_path = os.path.join(run_dir, f"repl_{i:02d}{'_companion' if with_companion else ''}.log")
        log_file = open(log_path, "w")
        log_file.write(f"=== {' '.join(cmd)} ===\n")

        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        lines = []
        thread = threading.Thread(target=_stream_output, args=(proc, lines, log_file), daemon=True)
        thread.start()

        try:
            proc.stdin.write(sql.encode())
            proc.stdin.flush()
        except BrokenPipeError as e:
            raise RuntimeError(f"nes-repl session {i} closed stdin immediately; see {log_path}") from e

        processes.append(proc)
        threads.append(thread)
        logs.append(log_file)
        all_lines.append(lines)

    # Each session prints exactly one query id (its Q8). Companions do not print their own — the
    # build branch is submitted inside the coordinator — so every other id in the worker log is a
    # statistic branch.
    analytical_ids = []
    deadline = time.time() + 120
    while time.time() < deadline and len(analytical_ids) < config.NUM_ANALYTICAL_QUERIES:
        analytical_ids = []
        for lines in all_lines:
            for line in list(lines):
                if match := _JSON_QUERY_ID_RE.search(line):
                    analytical_ids.append(match.group(1))
                    break
        if len(analytical_ids) < config.NUM_ANALYTICAL_QUERIES:
            time.sleep(0.5)

    if len(analytical_ids) != config.NUM_ANALYTICAL_QUERIES:
        printError(f"only {len(analytical_ids)}/{config.NUM_ANALYTICAL_QUERIES} REPL sessions "
                   f"reported a query id; see repl_*.log in {run_dir}")
    printInfo(f"    {len(analytical_ids)} analytical queries deployed, "
              f"{sessions_with_companions} with a companion "
              f"({num_statistic_queries} statistic queries)")
    return processes, threads, logs, analytical_ids


def shutdown_shared(processes, logs):
    """Close each REPL's stdin so --on-exit STOP_QUERIES runs, then reap."""
    for proc in processes:
        try:
            proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass
    for proc in processes:
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            printError(f"nes-repl pid {proc.pid} did not exit; killing")
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
    for log_file in logs:
        try:
            log_file.close()
        except OSError:
            pass
