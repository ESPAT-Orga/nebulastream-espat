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

"""Build-phase helpers shared by run_statistic_build.py and run_statistic_accuracy.py.

Both scripts iterate over the same parameter grid and need to populate the
worker's StatisticStore by submitting an existing ``*Build_*.yaml.template``.
The throughput-measurement script keeps only the throughput; the accuracy
script ignores it and continues with follow-up queries against the populated
synopsis.  Putting the rendering + submit/wait/parse here keeps the two
scripts in sync without one importing from the other.
"""

import os
import time
from typing import NamedTuple

from scripts.benchmarking.e2e._console import step
from scripts.benchmarking.e2e.dataset_quantiles import min_max, predicate_column
from scripts.benchmarking.e2e.configs import QUERY_CONFIGS_DIR
from scripts.benchmarking.e2e.worker_lifecycle import (
    parse_average_throughput_from_throughput_listener,
    stop_queries_and_wait,
    submit_query,
    wait_for_query_to_finish,
)


class BuildPhaseResult(NamedTuple):
    throughput: float
    duration_s: float
    query_ids: list
    ok: bool
    reason: str
    yaml_path: str


def _load_template(name: str) -> str:
    with open(os.path.join(QUERY_CONFIGS_DIR, name), 'r') as f:
        return f.read()


def _write_yaml(filepath: str, content: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        f.write(content)


def build_template_args(statistic_type, dataset_name, dataset_path,
                        memory_budget, window_size, statistic_id) -> dict:
    """Substitution map for the existing *Build_*.yaml.template files."""
    args = {
        'statistic_id': statistic_id,
        'window_size': window_size,
        'build_dataset_path': dataset_path,
        'memory_budget': memory_budget,
    }
    if statistic_type == 'EquiWidthHistogram':
        # The histogram template requires explicit min/max; size them to the
        # actual data so a typical predicate sits within the histogram's domain.
        _, col_idx = predicate_column(dataset_name)
        col_min, col_max = min_max(dataset_path, col_idx)
        args['min_value'] = int(0)
        args['max_value'] = int(100*1000)
    return args


def render_build_yaml(run_dir, statistic_type, dataset_name, dataset_path,
                      memory_budget, window_size, statistic_id) -> str:
    template_name = f"{statistic_type}Build_{dataset_name}.yaml.template"
    template = _load_template(template_name)
    args = build_template_args(
        statistic_type, dataset_name, dataset_path, memory_budget, window_size, statistic_id)
    yaml_path = os.path.join(run_dir, f"{statistic_type}Build.yaml")
    _write_yaml(yaml_path, template.format(**args))
    return yaml_path


def run_build_phase(*, worker_process, run_dir, log_file_path,
                    statistic_type, dataset_name, dataset_path,
                    memory_budget, window_size, statistic_id,
                    cli_log_file) -> BuildPhaseResult:
    """Render the *Build*.yaml, submit it, wait for completion, parse throughput.

    Wraps the work in a ``[build]`` step so the console transcript shows one
    indented line with elapsed time.  The worker is *not* torn down here —
    callers stop it themselves after any follow-up work.
    """
    yaml_path = render_build_yaml(
        run_dir, statistic_type, dataset_name, dataset_path,
        memory_budget, window_size, statistic_id)

    with step("build", detail=os.path.basename(yaml_path)) as add:
        start = time.time()
        query_ids = submit_query(yaml_path, cli_log_file)
        ok, reason = wait_for_query_to_finish(query_ids, yaml_path, worker_process=worker_process)
        duration_s = time.time() - start
        stop_queries_and_wait(query_ids, yaml_path, cli_log_file)
        throughput = parse_average_throughput_from_throughput_listener(log_file_path, query_ids)
        if throughput >= 0:
            add(f"{throughput:,.0f} tup/s")
        else:
            add("no_throughput")
        if not ok:
            add(f"FAIL:{reason}")

    return BuildPhaseResult(
        throughput=throughput,
        duration_s=duration_s,
        query_ids=query_ids,
        ok=ok,
        reason=reason,
        yaml_path=yaml_path,
    )
