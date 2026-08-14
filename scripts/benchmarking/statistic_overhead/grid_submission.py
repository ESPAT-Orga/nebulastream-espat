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

"""Isolated-grid submission: k analytical + j statistic queries, every one independent.

No companions and no splicing here — each query declares its own logical source and its own TCP
physical source. That is the point: it lifts the cap on j (a companion attaches to a data query, so
the shared experiment could never exceed one statistic query per analytical one) and it means the
statistic queries pay for their own ingestion, which is exactly the cost being measured.

Everything goes through ONE nes-repl session rather than one per query. The shared experiment needed
a process per query because StatisticCoordinator is per-process and its statistic registry would
collide; with no companions there is no coordinator involvement, so a single session submitting
k + j queries is both correct and far lighter at 60 queries.

Source names must still be unique per query: RunningSource::create registers by logical source name
worker-wide and refuses a duplicate, companions or not.
"""

import os
import subprocess
import threading

from scripts.benchmarking.statistic_overhead import config
from scripts.benchmarking.statistic_overhead.shared_submission import (
    REPL_EXECUTABLE,
    WORKER_DATA,
    WORKER_GRPC,
    _JSON_QUERY_ID_RE,
    _stream_output,
)
from scripts.benchmarking.utils import printError, printInfo

ANALYTICAL_SINK = "agg_void_sink"
STATISTIC_SINK_PREFIX = "stat_void_sink"


def analytical_source(i):
    return f"cluster_a{i}"


def statistic_source(i):
    return f"cluster_s{i}"


def _tcp_source_ddl(source):
    """One logical source + its own TCP physical source, all dialing the same generator port.

    The generator serves unlimited clients, each getting an identical independently-paced stream, so
    one connection per query gives real per-query ingestion without needing a file per query.
    """
    return f"""\
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
"""


def _analytical_sink_ddl(source):
    """Q2's output sink. START/END must be backtick-quoted — they are reserved tokens in
    AntlrSQL.g4, and nes-repl reports NOTHING on a parse error, it simply stops consuming
    statements, which looks exactly like a hung worker."""
    q = source.upper()
    return f"""\
CREATE SINK {ANALYTICAL_SINK}_{source}({q}.`START` UINT64 NOT NULL, {q}.`END` UINT64 NOT NULL, \
{q}.TOTALCPU FLOAT64 NOT NULL, {q}.JOBID UINT64 NOT NULL)
TYPE Void
SET('{WORKER_GRPC}' AS `SINK`.HOST);
"""


def _statistic_sink_ddl(source):
    """The 4-column synopsis metadata every StatisticBuild emits, qualified by its own source."""
    q = source.upper()
    return f"""\
CREATE SINK {STATISTIC_SINK_PREFIX}_{source}({q}.STATISTICID UINT64 NOT NULL, \
{q}.STATISTICSTART UINT64 NOT NULL, {q}.STATISTICEND UINT64 NOT NULL, \
{q}.STATISTICNUMBEROFSEENTUPLES UINT64 NOT NULL)
TYPE Void
SET('{WORKER_GRPC}' AS `SINK`.HOST);
"""


def analytical_sql(i):
    """ClusterMonitoring Q2, same shape as nes-systests/benchmark/ClusterMonitoring.test."""
    source = analytical_source(i)
    return (
        "SELECT start, end, SUM(cpu) AS totalCpu, jobId "
        f"FROM {source} "
        "WHERE eventType == INT16(3) "
        "GROUP BY jobId "
        f"WINDOW SLIDING(creationTS, SIZE {config.WINDOW_SIZE_SEC} SEC, "
        f"ADVANCE BY {config.WINDOW_ADVANCE_SEC} SEC) "
        f"INTO {ANALYTICAL_SINK}_{source};\n"
    )


def statistic_sql(i):
    """Standalone equi-width histogram over the grouping key, in the form proven by
    nes-systests/benchmark_statistic_build/StatisticBuild.test:23.

    Third argument is a memory budget in BYTES, not a bucket count. Statistic ids must be unique
    across every query alive in the run.
    """
    source = statistic_source(i)
    return (
        f"SELECT EQUIWIDTHHISTOGRAM({config.STATISTIC_ID_BASE + i}, jobId, "
        f"{config.MEMORY_BUDGET}, 0, {config.JOB_DOMAIN}) "
        f"FROM {source} "
        f"WINDOW TUMBLING(creationTS, size {config.GRID_STATISTIC_WINDOW_SEC} sec) "
        f"INTO {STATISTIC_SINK_PREFIX}_{source};\n"
    )


def build_sql(num_analytical, num_statistic):
    """Full statement stream for one grid point: DDL for every source and sink, then the queries.

    Analytical queries are emitted FIRST so their ids come back first — the runner splits the id
    list by position to tell analytical from statistic.
    """
    parts = [f'CREATE WORKER "{WORKER_GRPC}" SET (\'{WORKER_DATA}\' AS DATA);\n']
    for i in range(num_analytical):
        parts.append(_tcp_source_ddl(analytical_source(i)))
        parts.append(_analytical_sink_ddl(analytical_source(i)))
    for i in range(num_statistic):
        parts.append(_tcp_source_ddl(statistic_source(i)))
        parts.append(_statistic_sink_ddl(statistic_source(i)))
    for i in range(num_analytical):
        parts.append(analytical_sql(i))
    for i in range(num_statistic):
        parts.append(statistic_sql(i))
    return "".join(parts)


def submit_grid(num_analytical, num_statistic, run_dir, deploy_timeout=180):
    """Start one nes-repl and submit every query. Returns (proc, log_file, analytical, statistic).

    `analytical` / `statistic` are the deployed query ids, split by submission order. Unlike the
    shared experiment, statistic queries here DO report throughput — each owns a source, so the
    throughput listener sees its first pipeline.
    """
    sql = build_sql(num_analytical, num_statistic)
    expected = num_analytical + num_statistic

    log_path = os.path.join(run_dir, "repl.log")
    log_file = open(log_path, "w")
    cmd = [REPL_EXECUTABLE, "-f", "JSON", "-s", WORKER_GRPC, "--on-exit", "STOP_QUERIES"]
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
        raise RuntimeError(f"nes-repl closed stdin immediately; see {log_path}") from e

    # Deliberately do NOT close stdin: on EOF the REPL exits and --on-exit STOP_QUERIES tears every
    # query down before it can produce a single throughput window.
    import time
    deadline = time.time() + deploy_timeout
    ids = []
    while time.time() < deadline and len(ids) < expected:
        ids = [m.group(1) for line in list(lines) if (m := _JSON_QUERY_ID_RE.search(line))]
        if len(ids) < expected:
            time.sleep(0.5)

    if len(ids) != expected:
        printError(f"only {len(ids)}/{expected} queries deployed; see {log_path}")
    printInfo(f"    {len(ids)}/{expected} queries deployed "
              f"({num_analytical} analytical + {num_statistic} statistic)")
    return proc, log_file, ids[:num_analytical], ids[num_analytical:expected]


def shutdown_grid(proc, log_file):
    """Close stdin so --on-exit STOP_QUERIES runs, then reap."""
    try:
        proc.stdin.close()
    except (BrokenPipeError, OSError):
        pass
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        printError(f"nes-repl pid {proc.pid} did not exit; killing")
        proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    try:
        log_file.close()
    except OSError:
        pass
