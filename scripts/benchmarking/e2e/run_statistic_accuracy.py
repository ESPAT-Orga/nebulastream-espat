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

"""Statistic accuracy benchmark — populate the synopsis, then probe it.

For each parameter combination this script
  1. starts a fresh nes-single-node-worker,
  2. runs the build query (via the shared run_build_phase) to populate the
     StatisticStore,
  3. submits four follow-up queries against the populated synopsis: ground-truth
     point and range over the source, plus the synopsis approximation
     (full-dump for EquiWidthHistogram/Reservoir, point-form for CountMin —
     looped once per range value to compose a range estimate),
  4. computes per-window relative error for point and range, averaged across
     windows, and writes one row to results_statistic_accuracy.csv.
"""

import csv
import os
import time
from collections import defaultdict

from scripts.benchmarking.utils import (
    check_repository_root,
    compile_nebulastream,
    create_folder_and_remove_if_exists,
    printInfo,
)
from scripts.benchmarking.e2e._console import ProgressBar, step
from scripts.benchmarking.e2e.build_phase import run_build_phase
from scripts.benchmarking.e2e.configs import (
    ACCURACY_FIELDNAMES,
    COUNTMIN_RANGE_MAX_VALUES,
    QUERY_CONFIGS_DIR,
    STATISTIC_IDS,
    WAIT_BETWEEN_COMMANDS_LONG,
    build_dir,
    cmake_flags,
    enableLatencyForAccuracy,
    kCountMinCounterBytes,
    kCountMinRows,
    kCountMinSeed,
)
from scripts.benchmarking.e2e.dataset_quantiles import min_max, predicate_column, quantiles
from scripts.benchmarking.e2e.runner_utils import (
    RunnerConfig,
    Trial,
    build_basic_trials,
    initialize_csv_file,
    make_argparser,
    resolve_datasets,
    run_grid,
)
from scripts.benchmarking.e2e.worker_lifecycle import (
    check_log_for_buffer_exhaustion,
    dump_worker_log_tail,
    parse_log_to_throughput_csv,
    start_single_node_worker,
    stop_queries_and_wait,
    submit_query,
    terminate_process_if_exists,
    wait_for_query_to_finish,
)


# === Accuracy-specific helpers ================================================

def _load_template(name):
    with open(os.path.join(QUERY_CONFIGS_DIR, name), 'r') as f:
        return f.read()


def _render_template(name, **format_args):
    return _load_template(name).format(**format_args)


def _write_yaml(filepath, content):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        f.write(content)


def render_accuracy_yaml(template_name, output_path, **format_args):
    yaml_path = output_path + '.yaml'
    _write_yaml(yaml_path, _render_template(template_name, **format_args))
    return yaml_path


def countmin_dimensions(memory_budget):
    """Number of (rows, cols) the CountMin sketch builds for a given budget."""
    cols = max(1, memory_budget // (kCountMinRows * kCountMinCounterBytes))
    return kCountMinRows, cols


def generate_probe_csv(probe_csv_path, statistic_id, build_dataset_path, build_window_size_ms, num_windows):
    """Write a probe CSV (one row per window) for the standalone-worker probe queries.

    Schema: STATISTICID, STATISTICSTART, STATISTICEND, STATISTICNUMBEROFSEENTUPLES.
    Window boundaries are aligned to the build query's window grid by reading
    the first timestamp from the dataset.
    """
    with open(build_dataset_path, 'r') as f:
        first_timestamp = int(f.readline().strip().split(',')[0])
    first_window_start = (first_timestamp // build_window_size_ms) * build_window_size_ms

    os.makedirs(os.path.dirname(probe_csv_path), exist_ok=True)
    with open(probe_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        for i in range(num_windows):
            start_ts = first_window_start + i * build_window_size_ms
            end_ts = start_ts + build_window_size_ms
            writer.writerow([statistic_id, start_ts, end_ts, 0])
    return first_window_start


def submit_and_drain(yaml_path, log_file_path, cli_log_file, worker_process, label, *, detail=""):
    with step(label, detail=detail) as add:
        query_ids = submit_query(yaml_path, cli_log_file)
        ok, reason = wait_for_query_to_finish(query_ids, yaml_path, worker_process=worker_process)
        stop_queries_and_wait(query_ids, yaml_path, cli_log_file)
        if not ok:
            add(f"FAIL:{reason}")
    return ok, query_ids


def _read_csv(path):
    """Read a CSV produced by NES FileSink into a list of row-lists, skipping
    the typed header row (``WINDOW_START:UINT64:NOT_NULLABLE,...``)."""
    rows = []
    try:
        with open(path, 'r', newline='') as f:
            for row_idx, row in enumerate(csv.reader(f)):
                if row_idx == 0 and row and any(':' in cell for cell in row):
                    continue
                converted = []
                for v in row:
                    try:
                        converted.append(int(v))
                    except ValueError:
                        try:
                            converted.append(float(v))
                        except ValueError:
                            converted.append(v)
                rows.append(converted)
    except FileNotFoundError:
        pass
    return rows


def _gt_by_window(gt_csv):
    return {(r[0], r[1]): r[2] for r in _read_csv(gt_csv) if len(r) >= 3}


def compute_point_accuracy(gt_csv, approx_csv, statistic_type, *,
                           point_value=None, sample_full_dump_kwargs=None):
    if statistic_type == "EquiWidthHistogram":
        est_by_window = {}
        for ws, we, bin_start, bin_counter, bin_end in _read_csv(approx_csv):
            if bin_start <= point_value < bin_end:
                width = max(1, bin_end - bin_start)
                est_by_window[(ws, we)] = bin_counter / width
    elif statistic_type == "Reservoir":
        cols = (sample_full_dump_kwargs or {})["columns"]
        sample_col = (sample_full_dump_kwargs or {})["sample_column"]
        sample_idx = cols.index(sample_col)
        total_seen_idx = cols.index("total_seen")
        per_window = defaultdict(lambda: {"total_seen": 0, "sample_size": 0, "matches": 0})
        for row in _read_csv(approx_csv):
            ws, we = row[0], row[1]
            entry = per_window[(ws, we)]
            entry["total_seen"] = max(entry["total_seen"], row[total_seen_idx])
            entry["sample_size"] += 1
            if row[sample_idx] == point_value:
                entry["matches"] += 1
        est_by_window = {
            k: (v["matches"] * v["total_seen"] / max(1, v["sample_size"]))
            for k, v in per_window.items()
        }
    elif statistic_type == "CountMin":
        min_by_window = {}
        for ws, we, counter in _read_csv(approx_csv):
            key = (ws, we)
            min_by_window[key] = counter if key not in min_by_window else min(min_by_window[key], counter)
        est_by_window = min_by_window
    else:
        raise ValueError(f"Unknown statistic_type: {statistic_type}")
    return _score(_gt_by_window(gt_csv), est_by_window)


def compute_range_accuracy(gt_csv, approx_inputs, statistic_type, *,
                           range_lo=None, range_hi=None, sample_full_dump_kwargs=None):
    if statistic_type == "EquiWidthHistogram":
        sum_by_window = defaultdict(int)
        for ws, we, bin_start, bin_counter, bin_end in _read_csv(approx_inputs):
            if bin_start >= range_lo and bin_end <= range_hi:
                sum_by_window[(ws, we)] += bin_counter
        est_by_window = dict(sum_by_window)
    elif statistic_type == "Reservoir":
        cols = (sample_full_dump_kwargs or {})["columns"]
        sample_col = (sample_full_dump_kwargs or {})["sample_column"]
        sample_idx = cols.index(sample_col)
        total_seen_idx = cols.index("total_seen")
        per_window = defaultdict(lambda: {"total_seen": 0, "sample_size": 0, "matches": 0})
        for row in _read_csv(approx_inputs):
            ws, we = row[0], row[1]
            entry = per_window[(ws, we)]
            entry["total_seen"] = max(entry["total_seen"], row[total_seen_idx])
            entry["sample_size"] += 1
            if range_lo <= row[sample_idx] <= range_hi:
                entry["matches"] += 1
        est_by_window = {
            k: (v["matches"] * v["total_seen"] / max(1, v["sample_size"]))
            for k, v in per_window.items()
        }
    elif statistic_type == "CountMin":
        sum_by_window = defaultdict(float)
        for value, csv_path in approx_inputs:
            min_for_value = {}
            for ws, we, counter in _read_csv(csv_path):
                key = (ws, we)
                min_for_value[key] = counter if key not in min_for_value else min(min_for_value[key], counter)
            for key, mn in min_for_value.items():
                sum_by_window[key] += mn
        est_by_window = dict(sum_by_window)
    else:
        raise ValueError(f"Unknown statistic_type: {statistic_type}")
    return _score(_gt_by_window(gt_csv), est_by_window)


def _score(gt_by_window, est_by_window):
    if not gt_by_window:
        return float('nan'), 0
    rel_errors = []
    for key, gt_count in gt_by_window.items():
        est = est_by_window.get(key, 0)
        denom = gt_count if gt_count > 0 else 1
        rel_errors.append(abs(gt_count - est) / denom)
    return sum(rel_errors) / len(rel_errors), len(rel_errors)


def range_values(range_lo, range_hi, cap=COUNTMIN_RANGE_MAX_VALUES):
    span = range_hi - range_lo + 1
    if span <= cap:
        return list(range(range_lo, range_hi + 1))
    step_size = span / cap
    return sorted({int(range_lo + i * step_size) for i in range(cap)})


def _reservoir_dump_columns(dataset_name, sample_column):
    if dataset_name == "Nexmark":
        cols = ["window_start", "window_end", "total_seen", "auctionId", "bidder", "price"]
    elif dataset_name == "ClusterMonitoring":
        cols = ["window_start", "window_end", "total_seen",
                "jobId", "taskId", "machineId", "eventType", "userId", "category", "priority",
                "cpu", "ram", "disk", "constraints"]
    else:
        raise ValueError(f"Unknown dataset for Reservoir dump: {dataset_name}")
    if sample_column not in cols:
        raise ValueError(f"Sample column {sample_column} missing from Reservoir dump for {dataset_name}")
    return {"columns": cols, "sample_column": sample_column}


def _estimate_num_build_windows(dataset_path, window_size_sec):
    """Approximate count of build windows by sampling first/last timestamps."""
    window_ms = window_size_sec * 1000
    with open(dataset_path, 'r') as f:
        first_line = f.readline()
    if not first_line:
        return 1
    first_ts = int(first_line.strip().split(',')[0])
    with open(dataset_path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size > 4096:
            f.seek(-4096, os.SEEK_END)
            tail = f.read().decode('utf-8', errors='ignore').splitlines()
        else:
            f.seek(0)
            tail = f.read().decode('utf-8', errors='ignore').splitlines()
    last_line = next((line for line in reversed(tail) if line.strip()), None)
    if not last_line:
        return 1
    last_ts = int(last_line.split(',')[0])
    return max(1, (last_ts - first_ts) // window_ms + 1)


# === Per-trial work ===========================================================

def run_combination(trial: Trial, run_dir: str, cli_log_file) -> tuple:
    """Build + accuracy for one parameter combination. Returns (row, issues)."""
    executionMode, numberOfWorkerThreads, bufferSizeInBytes, \
        buffersInGlobalBufferManager, joinStrategy, pageSize = trial.worker_config
    issues = []
    notes = []
    statistic_id = STATISTIC_IDS[trial.statistic_type]

    col_name, col_idx = predicate_column(trial.dataset_name)
    q50, q75 = quantiles(trial.dataset_path, col_idx, 0.50, 0.75)
    q50, q75 = int(q50), int(q75)
    if q50 == q75:
        notes.append("q50_eq_q75")
        if q75 < int(min_max(trial.dataset_path, col_idx)[1]):
            q75 += 1
    point_value = q75
    range_lo, range_hi = q50, q75

    point_err, point_n = float('nan'), 0
    range_err, range_n = float('nan'), 0
    range_n_samples = 0

    log_file_path = os.path.join(run_dir, "SingleNodeStdout.log")
    stdout_file = open(log_file_path, 'w')
    with step("worker", detail="starting") as add:
        worker_process = start_single_node_worker(
            stdout_file, numberOfWorkerThreads, executionMode,
            joinStrategy, pageSize, bufferSizeInBytes,
            buffersInGlobalBufferManager, trial.enable_latency,
            statisticStoreType=trial.store_type,
            cli_log_file=cli_log_file)
        time.sleep(WAIT_BETWEEN_COMMANDS_LONG)
        add(f"pid {worker_process.pid}")

    try:
        # === Build (throughput recorded but not written to the accuracy CSV) ===
        build_result = run_build_phase(
            worker_process=worker_process, run_dir=run_dir, log_file_path=log_file_path,
            statistic_type=trial.statistic_type, dataset_name=trial.dataset_name,
            dataset_path=trial.dataset_path, memory_budget=trial.memory_budget,
            window_size=trial.build_window_size_sec, statistic_id=statistic_id,
            cli_log_file=cli_log_file)

        if build_result.throughput < 0:
            dump_worker_log_tail(log_file_path)
        if not build_result.ok:
            issues.append(f"build:{build_result.reason}")
            return _row(trial, col_name, point_value, range_lo, range_hi,
                        point_err, point_n, range_err, range_n, range_n_samples, notes), issues

        # === Probe CSV (one row per build window) ===
        probe_csv_path = os.path.join(run_dir, "probe_tuples.csv")
        num_probe_windows = max(1, _estimate_num_build_windows(trial.dataset_path, trial.build_window_size_sec))
        with step("probe-csv", detail=f"{num_probe_windows} windows") as add:
            first_window_start = generate_probe_csv(
                probe_csv_path, statistic_id, trial.dataset_path,
                build_window_size_ms=trial.build_window_size_sec * 1000,
                num_windows=num_probe_windows)
            add(f"start={first_window_start}")
            add(f"step={trial.build_window_size_sec * 1000}ms")

        # === Ground-truth queries ===
        gt_point_csv = os.path.join(run_dir, "gt_point.csv")
        gt_point_yaml = render_accuracy_yaml(
            f"AccuracyGtPoint_{trial.dataset_name}.yaml.template",
            os.path.join(run_dir, "AccuracyGtPoint"),
            point_value=point_value, window_size=trial.build_window_size_sec,
            build_dataset_path=trial.dataset_path, output_csv_path=gt_point_csv)
        submit_and_drain(gt_point_yaml, log_file_path, cli_log_file, worker_process,
                         "gt-point", detail=f"v={point_value}")

        gt_range_csv = os.path.join(run_dir, "gt_range.csv")
        gt_range_yaml = render_accuracy_yaml(
            f"AccuracyGtRange_{trial.dataset_name}.yaml.template",
            os.path.join(run_dir, "AccuracyGtRange"),
            range_lo=range_lo, range_hi=range_hi, window_size=trial.build_window_size_sec,
            build_dataset_path=trial.dataset_path, output_csv_path=gt_range_csv)
        submit_and_drain(gt_range_yaml, log_file_path, cli_log_file, worker_process,
                         "gt-range", detail=f"[{range_lo}, {range_hi}]")

        # === Approximation queries ===
        if trial.statistic_type in ("EquiWidthHistogram", "Reservoir"):
            approx_csv = os.path.join(run_dir, "approx.csv")
            approx_yaml = render_accuracy_yaml(
                f"{trial.statistic_type}AccuracyApprox_{trial.dataset_name}.yaml.template",
                os.path.join(run_dir, f"{trial.statistic_type}AccuracyApprox"),
                statistic_id=statistic_id, probe_csv_path=probe_csv_path,
                output_csv_path=approx_csv)
            submit_and_drain(approx_yaml, log_file_path, cli_log_file, worker_process, "approx")

            with step("accuracy") as add:
                if trial.statistic_type == "Reservoir":
                    reservoir_kwargs = _reservoir_dump_columns(trial.dataset_name, col_name)
                    point_err, point_n = compute_point_accuracy(
                        gt_point_csv, approx_csv, trial.statistic_type,
                        point_value=point_value, sample_full_dump_kwargs=reservoir_kwargs)
                    range_err, range_n = compute_range_accuracy(
                        gt_range_csv, approx_csv, trial.statistic_type,
                        range_lo=range_lo, range_hi=range_hi, sample_full_dump_kwargs=reservoir_kwargs)
                else:
                    point_err, point_n = compute_point_accuracy(
                        gt_point_csv, approx_csv, trial.statistic_type, point_value=point_value)
                    range_err, range_n = compute_range_accuracy(
                        gt_range_csv, approx_csv, trial.statistic_type, range_lo=range_lo, range_hi=range_hi)
                add(f"point={point_err:.3f} (n={point_n})")
                add(f"range={range_err:.3f} (n={range_n})")
        elif trial.statistic_type == "CountMin":
            cm_rows, cm_cols = countmin_dimensions(trial.memory_budget)
            approx_point_csv = os.path.join(run_dir, "approx_point.csv")
            approx_point_yaml = render_accuracy_yaml(
                f"CountMinAccuracyApproxPoint_{trial.dataset_name}.yaml.template",
                os.path.join(run_dir, "CountMinAccuracyApproxPoint"),
                statistic_id=statistic_id, probe_csv_path=probe_csv_path,
                output_csv_path=approx_point_csv,
                point_value=point_value, number_of_rows=cm_rows, number_of_cols=cm_cols,
                count_min_seed=kCountMinSeed)
            submit_and_drain(approx_point_yaml, log_file_path, cli_log_file, worker_process,
                             "approx-point", detail=f"v={point_value}")
            point_err, point_n = compute_point_accuracy(
                gt_point_csv, approx_point_csv, trial.statistic_type, point_value=point_value)

            sampled_values = range_values(range_lo, range_hi)
            range_inputs = []
            with step("approx-range", detail=f"{len(sampled_values)} sub-queries") as add:
                bar = ProgressBar(len(sampled_values), label="approx-range")
                for v in sampled_values:
                    v_csv = os.path.join(run_dir, f"approx_range_v{v}.csv")
                    v_yaml = render_accuracy_yaml(
                        f"CountMinAccuracyApproxPoint_{trial.dataset_name}.yaml.template",
                        os.path.join(run_dir, f"CountMinAccuracyApproxRange_v{v}"),
                        statistic_id=statistic_id, probe_csv_path=probe_csv_path,
                        output_csv_path=v_csv, point_value=v,
                        number_of_rows=cm_rows, number_of_cols=cm_cols,
                        count_min_seed=kCountMinSeed)
                    qids = submit_query(v_yaml, cli_log_file)
                    wait_for_query_to_finish(qids, v_yaml, worker_process=worker_process)
                    stop_queries_and_wait(qids, v_yaml, cli_log_file)
                    range_inputs.append((v, v_csv))
                    bar.tick()
            range_n_samples = len(range_inputs)
            if range_n_samples < (range_hi - range_lo + 1):
                notes.append(f"countmin_range_discretised_to_{range_n_samples}")

            with step("accuracy") as add:
                range_err, range_n = compute_range_accuracy(
                    gt_range_csv, range_inputs, trial.statistic_type)
                add(f"point={point_err:.3f} (n={point_n})")
                add(f"range={range_err:.3f} (n={range_n})")

        if check_log_for_buffer_exhaustion(log_file_path):
            issues.append("buffer_exhaustion")

        return _row(trial, col_name, point_value, range_lo, range_hi,
                    point_err, point_n, range_err, range_n, range_n_samples, notes), issues
    finally:
        terminate_process_if_exists(worker_process)
        stdout_file.close()
        parse_log_to_throughput_csv(log_file_path, os.path.join(run_dir, "throughput.csv"))


def _row(trial, col_name, point_value, range_lo, range_hi,
         point_err, point_n, range_err, range_n, range_n_samples, notes):
    executionMode, nThreads, bufSize, buffersGBM, joinStrat, pageSize = trial.worker_config
    return {
        'dataset': trial.dataset_name,
        'statistic_type': trial.statistic_type,
        'memory_budget': trial.memory_budget,
        'build_window_size_sec': trial.build_window_size_sec,
        'executionMode': executionMode,
        'numberOfWorkerThreads': nThreads,
        'buffersInGlobalBufferManager': buffersGBM,
        'joinStrategy': joinStrat,
        'bufferSizeInBytes': bufSize,
        'pageSize': pageSize,
        'enableLatency': trial.enable_latency,
        'statisticStoreType': trial.store_type,
        'point_column': col_name,
        'point_value': point_value,
        'range_lo': range_lo,
        'range_hi': range_hi,
        'point_avg_relative_error': point_err,
        'point_num_windows': point_n,
        'range_avg_relative_error': range_err,
        'range_num_windows': range_n,
        'range_n_samples': range_n_samples,
        'accuracy_notes': ';'.join(notes) if notes else '',
    }


if __name__ == "__main__":
    args = make_argparser("Build statistics, then measure selectivity accuracy.").parse_args()

    output_dir = args.output_dir if args.output_dir else "."
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "results_statistic_accuracy.csv")

    printInfo("Parsed arguments:")
    for k, v in vars(args).items():
        printInfo(f"  {k}: {v}")
    print()

    check_repository_root()

    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    if not args.skip_build:
        compile_nebulastream(cmake_flags, build_dir)

    resolve_datasets(args)

    if os.geteuid() == 0:
        try:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write("-1000")
        except OSError:
            pass

    trials = build_basic_trials(args, enable_latency_list=enableLatencyForAccuracy)
    initialize_csv_file(csv_path, ACCURACY_FIELDNAMES)

    runner_config = RunnerConfig(
        runner_name="accuracy",
        csv_filename="results_statistic_accuracy.csv",
        fieldnames=ACCURACY_FIELDNAMES,
    )

    run_grid(args=args, runner_config=runner_config, trials=trials,
             run_combination=run_combination, output_dir=output_dir, csv_path=csv_path)
