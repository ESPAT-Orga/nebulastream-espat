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

"""Shared-source submission path: one nes-repl per analytical query.

The statistic queries are deployed as *companions*. nes-repl hands each companion request to
StatisticCoordinator::collectWorkloadStatistic, which splices the histogram build branch onto the
join's own source operator — the branch is still a separate query, but its source carries
SpliceToRunningSourceTrait, so the worker redirects it into the data query's already-running source
thread instead of spawning a second one. Net effect: the histograms cost no extra TCP connections,
so the run holds a constant 10 x 2 = 20 connections at every point of the sweep.

Why one REPL process per analytical query rather than one for all ten: StatisticCoordinator is
constructed per REPL process (ReplStarter.cpp), and both its statistic registry and its
deployed-data-query cache are per-instance. Ten Q8 queries in one session would collide — every
`person.id` companion hashes to the same registry key (the REPL leaves WorkloadDomain::queryId
invalid), so only the first would get a build branch. Separate processes give each query its own
registry and sidestep that entirely.

Each session also declares its OWN logical sources (`person_<i>` / `auction_<i>`) — required, because
the worker-wide RunningSourceRegistry refuses a second live source per logical name. See setup_sql.

Companions are all-or-nothing per session, so the sweep varies how many of the ten sessions run with
`--companion-statistic`: k sessions with, 10-k without, giving 2k statistic queries.
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


def setup_sql(session):
    """DDL for one session: its own logical sources `person_<i>` / `auction_<i>`.

    The per-session naming is REQUIRED, not cosmetic. RunningSourceRegistry is keyed by logical
    source name and is worker-wide, and it strictly refuses a second live source for a name that
    already has one ("the splice contract assumes a single source thread per logical name",
    RunningSourceRegistry.cpp). Ten queries all reading a source called `person` therefore abort the
    worker on the second registration. Distinct names give each query its own registry entry.

    Both physical sources still dial the same two generator ports, so the connection count is
    unchanged: 2 per analytical query, 20 in total, whatever N is.

    Schemas mirror nes-systests/benchmark/Nexmark_with_varsized.test. Sink schema fields are
    dot-qualified and uppercase, matching the form the adaptive-optimization benchmark uses.

    The join's window-bound columns must be backtick-quoted: START and END are reserved tokens in
    AntlrSQL.g4, and an unquoted `PERSONAUCTION.START` fails to parse. The REPL reports nothing at
    all when that happens — it simply stops consuming statements — so a bare stall after the last
    CREATE PHYSICAL SOURCE is the signature of a bad DDL statement, not of a hung worker.
    """
    person_port = config.NEXMARK_PORT_BASE
    auction_port = config.NEXMARK_PORT_BASE + 1
    person, auction = source_names(session)
    # The join's output columns are qualified by the concatenation of its input source names.
    joined = f"{person}{auction}".upper()
    return f"""\
CREATE WORKER "{WORKER_GRPC}" SET ('{WORKER_DATA}' AS DATA);
CREATE LOGICAL SOURCE {person}(id INT32 NOT NULL, name VARSIZED NOT NULL, email_address VARSIZED NOT NULL, \
credit_card VARSIZED NOT NULL, city VARSIZED NOT NULL, state VARSIZED NOT NULL, timestamp UINT64 NOT NULL, \
extra VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR {person}
TYPE TCP
SET(
    'CSV' as PARSER.`TYPE`,
    '127.0.0.1' AS `SOURCE`.SOCKET_HOST,
    '{person_port}' AS `SOURCE`.SOCKET_PORT,
    '{config.TCP_FLUSH_INTERVAL_MS}' AS `SOURCE`.FLUSH_INTERVAL_MS,
    '10' AS `SOURCE`.CONNECT_TIMEOUT_SECONDS,
    '{WORKER_GRPC}' AS `SOURCE`.HOST
);
CREATE LOGICAL SOURCE {auction}(timestamp UINT64 NOT NULL, id INT32 NOT NULL, initialbid INT32 NOT NULL, \
reserve INT32 NOT NULL, expires UINT64 NOT NULL, seller INT32 NOT NULL, category INT32 NOT NULL);
CREATE PHYSICAL SOURCE FOR {auction}
TYPE TCP
SET(
    'CSV' as PARSER.`TYPE`,
    '127.0.0.1' AS `SOURCE`.SOCKET_HOST,
    '{auction_port}' AS `SOURCE`.SOCKET_PORT,
    '{config.TCP_FLUSH_INTERVAL_MS}' AS `SOURCE`.FLUSH_INTERVAL_MS,
    '10' AS `SOURCE`.CONNECT_TIMEOUT_SECONDS,
    '{WORKER_GRPC}' AS `SOURCE`.HOST
);
CREATE SINK join_void_sink({joined}.`START` UINT64 NOT NULL, {joined}.`END` UINT64 NOT NULL, \
{person.upper()}.ID INT32 NOT NULL, {person.upper()}.NAME VARSIZED NOT NULL)
TYPE Void
SET(
    '{WORKER_GRPC}' AS `SINK`.HOST
);
"""


def source_names(session):
    """Logical source names for one session. Must be unique worker-wide — see setup_sql."""
    return f"person_{session}", f"auction_{session}"


def q8_sql(session):
    """Nexmark Q8, same shape as nes-systests/benchmark/Nexmark_with_varsized.test."""
    person, auction = source_names(session)
    return (
        "SELECT start, end, id, name FROM ( "
        f"SELECT * FROM (SELECT * FROM {person}) "
        f"INNER JOIN (SELECT * FROM {auction}) "
        f"ON id = seller WINDOW TUMBLING (timestamp, size {config.WINDOW_SIZE_SEC} sec) "
        ") INTO join_void_sink;\n"
    )


def _companion_args(session):
    """Flags that attach one histogram to each of the join's two inputs.

    --companion-source / --companion-source-2 name which of the join's sources each branch splices
    onto. Without them StatisticCoordinator rejects the request: the field name cannot disambiguate,
    since person and auction both have an `id`.
    """
    person, auction = source_names(session)
    return [
        "--companion-statistic",
        "--companion-metric", config.COMPANION_METRIC,
        "--companion-source", person,
        "--companion-field", "id",
        "--companion-source-2", auction,
        "--companion-field-2", "seller",
        "--companion-window-size-ms", str(config.WINDOW_SIZE_SEC * 1000),
        "--companion-event-time-field", config.COMPANION_EVENT_TIME_FIELD,
        "--companion-histogram-min", "0",
        "--companion-histogram-max", str(config.PERSON_DOMAIN),
        "--companion-host", WORKER_GRPC,
    ]


def submit_shared(num_statistic_queries, run_dir):
    """Start one nes-repl per analytical query; the first k carry a companion pair.

    Returns (processes, threads, log_handles, analytical_query_ids). Everything else the worker
    reports throughput for is a spliced statistic branch.
    """
    if num_statistic_queries % 2 != 0:
        raise ValueError(f"shared mode adds histograms in person/auction pairs, so "
                         f"num_statistic_queries must be even (got {num_statistic_queries})")
    sessions_with_companions = num_statistic_queries // 2
    if sessions_with_companions > config.NUM_ANALYTICAL_QUERIES:
        raise ValueError(f"{num_statistic_queries} statistic queries needs "
                         f"{sessions_with_companions} sessions but only "
                         f"{config.NUM_ANALYTICAL_QUERIES} analytical queries exist")

    processes, threads, logs, all_lines = [], [], [], []

    for i in range(config.NUM_ANALYTICAL_QUERIES):
        with_companion = i < sessions_with_companions
        sql = setup_sql(i) + q8_sql(i)
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
              f"{sessions_with_companions} with a companion pair "
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
