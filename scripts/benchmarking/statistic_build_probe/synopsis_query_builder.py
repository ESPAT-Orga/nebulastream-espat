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

"""Render a single build query that maintains N synopses for the amortization experiment.

The amortization experiment needs ONE query carrying a *variable* number of synopses over the first-N
numeric fields of a dataset, so the YAML cannot be one of the static ``*Build_*.yaml.template`` files.
Instead of hardcoding schemas in Python, we keep a per-dataset YAML *skeleton*
(``query-configs/SynopsisAmortization_<Dataset>.yaml.template``) that carries the logical source schema, the
Memory/Native source, and the event-time field (in its placeholder ``WINDOW TUMBLING(...)`` clause). This
module loads that skeleton with PyYAML, derives the numeric fields from the schema, and overwrites ``query``,
the sink ``schema`` and the source ``file_path`` to produce a concrete, ready-to-submit query YAML.

The swept ``memory_budget`` is the TOTAL budget for the whole query; CountMin / EquiWidthHistogram
split it evenly across their N synopses (``per = total // N``), so total state stays fixed as N grows
and the amortization curve isolates per-synopsis maintenance cost rather than total memory.

Per synopsis kind the generated query is (``per = memory_budget // N``):
  - CountMin           : ``COUNTMINSKETCH(id_i, field_i, per)`` for i in 0..N-1
  - EquiWidthHistogram : ``EQUIWIDTHHISTOGRAM(id_i, field_i, per, min, max)``
  - Reservoir          : a single ``RESERVOIR(id, f0, f1, ..., fF, budget)`` over *all*
                         numeric fields (one full-record sample; always N == 1, so per == budget)
  - Sum                : ``SUM(f0) AS sum_f0`` (baseline, N == 1)
  - Passthrough        : ``SELECT *`` (raw-copy ceiling, no window, N == 1)

The i-th synopsis uses statistic id ``base_id + i`` so all ids in one query are distinct (the engine asserts
this). Every synopsis writes a VARSIZED field whose name is derived from its id by the engine, so the chained
store-writers and the build sink agree without us naming anything here. CountMin / EquiWidthHistogram /
Reservoir all emit the same four-column synopsis-metadata sink; the record flows through the chained writers
and the sink reports the last writer's statistic id.
"""

import functools
import os
import re

import yaml

from scripts.benchmarking.statistic_build_probe.configs import (
    QUERY_CONFIGS_DIR,
    SYNOPSIS_AMORT_BASE_IDS,
    SYNOPSIS_AMORT_HISTOGRAM_MAX,
    SYNOPSIS_AMORT_HISTOGRAM_MIN,
    SYNOPSIS_AMORT_N_LADDER,
)


def _skeleton_path(dataset_name: str) -> str:
    return os.path.join(QUERY_CONFIGS_DIR, f"SynopsisAmortization_{dataset_name}.yaml.template")


@functools.lru_cache(maxsize=None)
def _skeleton_text(dataset_name: str) -> str:
    path = _skeleton_path(dataset_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No amortization skeleton for {dataset_name}: {path}")
    with open(path, "r") as f:
        return f.read()


def _is_numeric(nes_type: str) -> bool:
    """True for aggregatable numeric NES types (INT*/UINT*/FLOAT*); excludes BOOLEAN, VARSIZED, etc."""
    return nes_type.upper().startswith(("INT", "UINT", "FLOAT"))


@functools.lru_cache(maxsize=None)
def _schema(dataset_name: str) -> tuple:
    """Ordered ((field_name, nes_type), ...) read from the skeleton's logical source."""
    doc = yaml.safe_load(_skeleton_text(dataset_name))
    fields = doc["logical"][0]["schema"]
    return tuple((f["name"], f["type"]) for f in fields)


@functools.lru_cache(maxsize=None)
def _event_time_field(dataset_name: str) -> str:
    """The window field, parsed from the skeleton query's WINDOW TUMBLING(<field>, ...) clause."""
    doc = yaml.safe_load(_skeleton_text(dataset_name))
    match = re.search(r"TUMBLING\(\s*(\w+)", doc["query"])
    if not match:
        raise ValueError(f"Skeleton for {dataset_name} has no TUMBLING(<field>, ...) to read event time from")
    return match.group(1)


def numeric_fields_for(dataset_name: str) -> list:
    """Aggregatable numeric fields in schema order, excluding the event-time (window) field."""
    event_time = _event_time_field(dataset_name)
    return [name for name, nes_type in _schema(dataset_name) if _is_numeric(nes_type) and name != event_time]


def field_count(dataset_name: str) -> int:
    return len(numeric_fields_for(dataset_name))


def n_ladder_for(dataset_name: str) -> list:
    """The {1,2,4,8,F} ladder clamped to this dataset's field count F, deduped + sorted."""
    f = field_count(dataset_name)
    ladder = {n for n in SYNOPSIS_AMORT_N_LADDER if n <= f}
    ladder.add(f)
    return sorted(ladder)


def _field_type(dataset_name: str, field_name: str) -> str:
    for name, nes_type in _schema(dataset_name):
        if name == field_name:
            return nes_type
    raise ValueError(f"Field {field_name} not in schema of {dataset_name}")


_SYNOPSIS_META_SINK_FIELDS = [
    ("build_source$statisticid", "UINT64"),
    ("build_source$statisticstart", "UINT64"),
    ("build_source$statisticend", "UINT64"),
    ("build_source$statisticnumberofseentuples", "UINT64"),
]


def _sink_fields_for(dataset_name: str, synopsis_kind: str,
                     store_backed: bool = False, omit_store_writer: bool = False) -> list:
    """The (name, type) pairs the void sink must declare for this query's output.

    Store-backed statistics (the synopses and, when ``store_backed`` is set, the scalar Count/Avg/Sum)
    emit the statistic-metadata sink. Without the StatisticStoreWriter (``omit_store_writer``) the
    STATISTICID field is not produced, so it is dropped from the sink schema.
    """
    if store_backed or synopsis_kind in ("CountMin", "EquiWidthHistogram", "Reservoir"):
        if omit_store_writer:
            return list(_SYNOPSIS_META_SINK_FIELDS[1:])  # drop statisticid (added by the writer)
        return list(_SYNOPSIS_META_SINK_FIELDS)
    if synopsis_kind == "Sum":
        field = numeric_fields_for(dataset_name)[0]
        return [(f"build_source$sum_{field}", _field_type(dataset_name, field))]
    if synopsis_kind == "Passthrough":
        return [(f"build_source${name}", nes_type) for name, nes_type in _schema(dataset_name)]
    raise ValueError(f"Unknown synopsis kind: {synopsis_kind}")


# Store-backed scalar statistics: the SQL function name per aggregation (all take (statisticId, field)).
_SCALAR_STATISTIC_FUNCS = {"Count": "COUNTSTATISTIC", "Avg": "AVGSTATISTIC", "Sum": "SUMSTATISTIC"}


def _query_string(dataset_name: str, synopsis_kind: str, num_synopses: int,
                  memory_budget: int, window_size: int, store_backed: bool = False) -> str:
    fields = numeric_fields_for(dataset_name)
    window = f"WINDOW TUMBLING({_event_time_field(dataset_name)}, size {window_size} sec)"
    base = SYNOPSIS_AMORT_BASE_IDS[synopsis_kind]

    # Store-backed scalar statistics (Count / Avg / Sum): a single (statisticId, field) synopsis with no
    # memory budget. Checked before the plain-Sum branch so store_backed Sum -> SUMSTATISTIC, not SUM(...).
    if store_backed and synopsis_kind in _SCALAR_STATISTIC_FUNCS:
        return f"SELECT {_SCALAR_STATISTIC_FUNCS[synopsis_kind]}({base}, {fields[0]}) FROM build_source {window} INTO void_sink;"

    # memory_budget is the TOTAL budget for the whole query, split evenly across the N synopses
    # (e.g. 1 KiB / 10 synopses -> ~102 B each). This keeps the total state fixed as N grows,
    # so the amortization curve isolates per-synopsis maintenance cost rather than total memory.
    per_synopsis_budget = max(1, memory_budget // num_synopses)

    if synopsis_kind == "CountMin":
        aggs = ", ".join(
            f"COUNTMINSKETCH({base + i}, {fields[i]}, {per_synopsis_budget})"
            for i in range(num_synopses))
        return f"SELECT {aggs} FROM build_source {window} INTO void_sink;"

    if synopsis_kind == "EquiWidthHistogram":
        aggs = ", ".join(
            f"EQUIWIDTHHISTOGRAM({base + i}, {fields[i]}, {per_synopsis_budget}, "
            f"{SYNOPSIS_AMORT_HISTOGRAM_MIN}, {SYNOPSIS_AMORT_HISTOGRAM_MAX})"
            for i in range(num_synopses))
        return f"SELECT {aggs} FROM build_source {window} INTO void_sink;"

    if synopsis_kind == "Reservoir":
        all_fields = ", ".join(fields)
        return (f"SELECT RESERVOIR({base}, {all_fields}, {memory_budget}) "
                f"FROM build_source {window} INTO void_sink;")

    if synopsis_kind == "Sum":
        field = fields[0]
        return f"SELECT SUM({field}) AS sum_{field} FROM build_source {window} INTO void_sink;"

    if synopsis_kind == "Passthrough":
        return "SELECT * FROM build_source INTO void_sink;"

    raise ValueError(f"Unknown synopsis kind: {synopsis_kind}")


def build_query_yaml(*, dataset_name: str, dataset_path: str, synopsis_kind: str,
                     num_synopses: int, memory_budget: int, window_size: int,
                     store_backed: bool = False, omit_store_writer: bool = False) -> str:
    """Render a full, ready-to-submit single-node-worker query YAML for one (kind, N, budget).

    Loads the per-dataset skeleton (schema + Memory/Native source) and overwrites the query, the void-sink
    schema, and the source file_path. The Memory source + Native parser keep CSV parsing out of the timed
    path (it happens once in setup()).

    ``store_backed`` routes the scalar Count/Avg/Sum kinds through their statistic-store functions
    (SUMSTATISTIC / ...). ``omit_store_writer`` drops STATISTICID from the sink schema to match a query
    submitted to a worker running with NES_STAT_OMIT_STORE_WRITER set (build without the store writer).
    """
    doc = yaml.safe_load(_skeleton_text(dataset_name))
    doc["query"] = _query_string(dataset_name, synopsis_kind, num_synopses, memory_budget, window_size,
                                 store_backed=store_backed)
    doc["sinks"][0]["schema"] = [
        {"name": name, "type": nes_type, "nullable": False}
        for name, nes_type in _sink_fields_for(dataset_name, synopsis_kind,
                                               store_backed=store_backed, omit_store_writer=omit_store_writer)
    ]
    doc["physical"][0]["source_config"]["file_path"] = dataset_path
    per_synopsis_budget = max(1, memory_budget // num_synopses)
    header = (f"# Auto-generated by synopsis_query_builder.py from SynopsisAmortization_{dataset_name}.yaml.template\n"
              f"# kind={synopsis_kind} num_synopses={num_synopses} total_budget={memory_budget} "
              f"per_synopsis_budget={per_synopsis_budget} window={window_size}s\n")
    # width=10**9 keeps the long `query` scalar on one line (no YAML line-folding).
    return header + yaml.safe_dump(doc, sort_keys=False, default_flow_style=False, width=10**9)
