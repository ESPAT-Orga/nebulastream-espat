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
Compute quantiles of a single column from a CSV dataset for the accuracy
benchmark.

The accuracy benchmark uses *actual* data points (not interpolated quantiles)
as point predicates so that ``WHERE col = q0.75`` matches at least one row in
every window.  Therefore every quantile call rounds to the closest value
present in the column (mirrors pandas' ``interpolation='nearest'``).

Per-dataset column metadata (which column the synopses are built on, plus its
0-based index in the source CSV) lives here so callers don't have to duplicate
the schema. Implemented with the stdlib only (csv module) so the script does
not require pandas on the worker host.
"""

import csv
import functools
import os

# Predicate column for the accuracy benchmark, per dataset.
# - Nexmark: bidder (INT32, column index 2 in bid_*.csv)
# - ClusterMonitoring: userId (INT16, column index 5 in google-cluster-data-original_*.csv)
PREDICATE_COLUMN = {
    "Nexmark": ("bidder", 2),
    "ClusterMonitoring": ("userId", 5),
}


def predicate_column(dataset_name):
    """Return (column_name, column_index) for the dataset's predicate column."""
    if dataset_name not in PREDICATE_COLUMN:
        raise ValueError(f"Unknown dataset: {dataset_name}. Add it to PREDICATE_COLUMN.")
    return PREDICATE_COLUMN[dataset_name]


@functools.lru_cache(maxsize=None)
def _read_column_sorted(csv_path_abs, column_index):
    """Read one column from a CSV and return a sorted list of values.

    Cached per (abs_path, column_index). Numeric coercion is best-effort: int
    if it parses, else float, else raw string.
    """
    values = []
    with open(csv_path_abs, 'r', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if column_index >= len(row):
                continue
            v = row[column_index]
            try:
                values.append(int(v))
            except ValueError:
                try:
                    values.append(float(v))
                except ValueError:
                    values.append(v)
    values.sort()
    return values


def _nearest_quantile(sorted_values, q):
    """``interpolation='nearest'`` quantile of an already-sorted list.

    Uses the C/R-1 convention: pick the value at index ``round(q * (N-1))``,
    clamped to the bounds. Matches pandas' interpolation='nearest' on the
    monotonic interior.
    """
    if not sorted_values:
        raise ValueError("Cannot compute quantile of empty sequence")
    n = len(sorted_values)
    idx = int(round(q * (n - 1)))
    idx = max(0, min(n - 1, idx))
    return sorted_values[idx]


@functools.lru_cache(maxsize=None)
def quantiles(csv_path, column_index, q_lo=0.50, q_hi=0.75):
    """Return (q_lo_value, q_hi_value) for *column_index* in the given CSV.

    Both values are guaranteed to be points actually present in the column
    (nearest-quantile semantics), so ``WHERE col = q_hi_value`` matches at
    least one row by construction.

    Cached on (abs_path, column_index, q_lo, q_hi) so repeated calls within a
    benchmark sweep don't re-read the file.
    """
    abs_path = os.path.abspath(csv_path)
    sorted_values = _read_column_sorted(abs_path, column_index)
    return _nearest_quantile(sorted_values, q_lo), _nearest_quantile(sorted_values, q_hi)


@functools.lru_cache(maxsize=None)
def min_max(csv_path, column_index):
    """Return (min, max) of *column_index* in the given CSV.

    Used to size EquiWidthHistogram bin bounds when the build template needs
    explicit min/max values matched to the actual data distribution.
    """
    abs_path = os.path.abspath(csv_path)
    sorted_values = _read_column_sorted(abs_path, column_index)
    return sorted_values[0], sorted_values[-1]
