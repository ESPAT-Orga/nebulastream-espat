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

"""Self-check for the throughput-listener parsing in run_statistic_overhead.

This is the part that turns a worker log into the numbers the paper plots, and a mistake in the
warm-up filter or the SI-prefix decoding would produce plausible-but-wrong results rather than an
error. Run from the repository root:

    python3 -m scripts.benchmarking.statistic_overhead.test_throughput_parsing
"""

import os
import tempfile

from scripts.benchmarking.statistic_overhead.run_statistic_overhead import (
    parse_throughput_samples,
    steady_state_means,
)
from scripts.benchmarking.statistic_overhead import config, shared_submission


def _line(query_id, start, end, value, prefix):
    return (f"Throughput for queryId QueryId(local=0a1b2c3d-dead-beef-0000-000000000001, "
            f"distributed={query_id}) in window {start}-{end} is {value} {prefix}Tup/s\n")


def test_parsing_and_warmup():
    log = [
        "some unrelated worker chatter\n",
        # Inside the 1 s warm-up: must be dropped.
        _line("alpha", 0, 200, "2.000000", "M"),
        _line("beta", 0, 200, "1.000000", "k"),
        # Steady state.
        _line("alpha", 1000, 1200, "4.000000", "M"),
        _line("alpha", 1200, 1400, "6.000000", "M"),
        # Final window of alpha — partial, dropped by steady_state_means.
        _line("alpha", 1400, 1600, "8.000000", "M"),
        # Single steady-state window for gamma: kept, since there is nothing else to drop.
        _line("gamma", 1000, 1200, "500.000000", ""),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "worker.log")
        with open(path, "w") as f:
            f.writelines(log)
        samples = parse_throughput_samples(path)

    assert len(samples) == 6, samples
    assert ("beta", 0, 1_000.0) in samples, "k prefix must decode to 1e3"
    assert ("gamma", 1000, 500.0) in samples, "empty prefix must decode to 1"

    means = steady_state_means(samples, warmup_seconds=1)
    assert set(means) == {"alpha", "gamma"}, means
    # Windows at 1000 and 1200 survive; 1400 is dropped as the partial last one.
    assert means["alpha"] == 5_000_000.0, means["alpha"]
    assert means["gamma"] == 500.0, means["gamma"]


def test_warmup_can_discard_everything():
    """A run too short to leave any post-warm-up window must yield nothing, not a bogus number."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "worker.log")
        with open(path, "w") as f:
            f.write(_line("alpha", 0, 200, "2.000000", "M"))
        means = steady_state_means(parse_throughput_samples(path), warmup_seconds=60)
    assert means == {}, means


def test_sessions_are_independent():
    """Each session must declare its own logical source: RunningSourceRegistry is keyed by logical
    source name worker-wide and aborts the worker on a duplicate."""
    names = [shared_submission.source_name(i) for i in range(config.NUM_ANALYTICAL_QUERIES)]
    assert len(set(names)) == len(names), f"logical source names collide: {names}"

    for i, name in enumerate(names):
        sql = shared_submission.setup_sql(i) + shared_submission.analytical_sql(i)
        assert f"CREATE LOGICAL SOURCE {name}(" in sql
        assert f"FROM {name} " in sql, "the query must read its own source"
        # START/END are reserved tokens; unquoted they fail to parse and nes-repl dies silently.
        assert f"{name.upper()}.`START`" in sql and f"{name.upper()}.`END`" in sql
        # Every other session's source must be absent from this session's SQL.
        for other in names:
            if other != name:
                assert other not in sql, f"session {i} leaked {other}"


def test_companion_covers_the_group_by_key():
    """The histogram must be over jobId — the GROUP BY key — and bounded by the generator's domain."""
    args = shared_submission._companion_args(0)
    assert "--companion-statistic" in args
    field = args[args.index("--companion-field") + 1]
    assert field == "jobId", f"expected the GROUP BY key, got {field}"
    assert args[args.index("--companion-histogram-max") + 1] == str(config.JOB_DOMAIN)
    # A single-source data query resolves its own splice target, so naming it would be redundant.
    assert "--companion-source" not in args


def test_offered_load_is_independent_of_statistic_count():
    """The experiment's core property: companions splice onto an existing source, so they add no
    ingestion. If this ever changes, a throughput drop stops being attributable to the synopsis."""
    baseline = config.offered_tps(200_000, 0)
    assert baseline == 200_000 * config.NUM_ANALYTICAL_QUERIES
    for n in range(0, config.NUM_ANALYTICAL_QUERIES + 1):
        assert config.offered_tps(200_000, n) == baseline, f"offered load changed at N={n}"


if __name__ == "__main__":
    test_parsing_and_warmup()
    test_warmup_can_discard_everything()
    test_sessions_are_independent()
    test_companion_covers_the_group_by_key()
    test_offered_load_is_independent_of_statistic_count()
    print("ok")
