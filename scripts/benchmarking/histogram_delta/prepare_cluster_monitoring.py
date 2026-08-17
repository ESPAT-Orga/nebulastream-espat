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

"""Prepare the real Google-cluster-monitoring trace for the histogram delta throughput benchmark.

Downloads the 1 GB ClusterMonitoring trace (the same ``ENABLE_LARGE_TESTS`` dataset the systests use,
fetched by its ExternalData MD5 hash) and projects it to the harness's 2-column ``value,timestamp``
schema:

  * ``value``     = ``taskId``     (col 3) -- a real, non-degenerate integer field (range 0..20009).
  * ``timestamp`` = ``creationTS`` (col 1) -- the real event time in milliseconds.

Why a projection and not the full 12-column schema: the harness query only reads one field to histogram
(``taskId``) and one to window on (``creationTS``); the other 10 columns are dead weight. Projecting keeps
the proven UINT64 File+CSV path used by the templates unchanged. The values and timestamps are the real
trace -- only the unused columns are dropped.

Why ``taskId`` and not the template's ``userId``: in this trace ``userId`` is anonymized to the constant
32767, which would collapse the histogram to a single bucket. ``taskId`` spans 0..20009 (mean ~3457).

Window-size note: the benchmark defaults to ``--window-size 60`` purely for throughput reasons (fewer,
fuller windows). The trace's ~29k 1-second gaps and its non-zero start epoch are harmless -- keyframes are
picked by window ordinal, so they land only on windows that actually occur.

Idempotent: skips work whose output already exists. Caches the raw download next to the projected file.
"""

import hashlib
import os
import sys
import urllib.request

# The ClusterMonitoring 1 GB trace, addressed exactly as the systests' ExternalData does:
# nes-systests/testdata/large/cluster_monitoring/google-cluster-data-original_1G.csv.md5 holds this hash,
# and nes-systests/systest/CMakeLists.txt sets the URL template below.
RAW_MD5 = "1543213c4f95ade501aee5b931d92c44"
RAW_URL = f"https://tubcloud.tu-berlin.de/s/28Tr2wTd73Ggeed/download?files=MD5_{RAW_MD5}"

TASK_ID_COL = 3  # 1-based column in the raw CSV -> becomes `value`
CREATION_TS_COL = 1  # 1-based column in the raw CSV -> becomes `timestamp`
VALUE_MIN = 0
VALUE_MAX = 20009  # global max of taskId across the full trace (bins span [VALUE_MIN, VALUE_MAX])

# The projection keeps the REAL event time unchanged: the raw epoch runs as-is.


def _md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_raw(raw_path):
    """Fetch the raw 1 GB trace (cached; verified against RAW_MD5)."""
    if os.path.exists(raw_path) and _md5(raw_path) == RAW_MD5:
        print(f"raw cache hit: {raw_path}")
        return raw_path
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    print(f"downloading ClusterMonitoring 1 GB trace -> {raw_path} (this can take a while)")
    urllib.request.urlretrieve(RAW_URL, raw_path)
    got = _md5(raw_path)
    if got != RAW_MD5:
        raise RuntimeError(f"MD5 mismatch for {raw_path}: got {got}, expected {RAW_MD5}")
    print("download verified")
    return raw_path


def project(raw_path, out_path):
    """Project (taskId, creationTS) -> value,timestamp CSV (no header, matching the query templates).

    The real event time is written unchanged.
    """
    ti, tsi = TASK_ID_COL - 1, CREATION_TS_COL - 1
    rows = 0
    tmp = out_path + ".tmp"
    with open(raw_path, "r") as fin, open(tmp, "w") as fout:
        for line in fin:
            parts = line.rstrip("\n").split(",")
            if len(parts) <= max(ti, tsi):
                continue
            fout.write(f"{parts[ti]},{parts[tsi]}\n")
            rows += 1
    os.replace(tmp, out_path)
    print(f"projected {rows} rows -> {out_path}")
    return rows


def prepare(out_path, raw_path=None):
    """Ensure the projected value,timestamp CSV exists at out_path; return its path.

    raw_path defaults to a cache sibling of out_path. Both the raw download and the projection are skipped
    when their outputs already exist.
    """
    if os.path.exists(out_path):
        print(f"projected cache hit: {out_path}")
        return out_path
    if raw_path is None:
        raw_path = os.path.join(os.path.dirname(out_path), "cluster_monitoring_raw.csv")
    download_raw(raw_path)
    project(raw_path, out_path)
    return out_path


def replicate(src_path, out_path, copies, window_ms):
    """Write `copies` back-to-back copies of the projected trace, each shifted in event time.

    Why this exists: the trace is only 18.65 M rows, which a Memory source drains in a few seconds, so a
    throughput run measures a window short enough that warm-up is a large fraction of it. Replaying the
    same rows lengthens the run without inventing data.

    The shift MUST be a whole number of windows, and it must cover the trace's own span:

      shift = ceil((max_ts - min_ts + 1) / window_ms) * window_ms

    Covering the span keeps event time strictly increasing across the seam (a shift smaller than the span
    would put copy k+1 behind the end of copy k, i.e. behind the watermark -- which is exactly why
    LoopingMemory's built-in offset, one tuple per row, does not work for a real-epoch trace). Rounding up
    to a whole window means no window straddles a seam, so every window's contents stay byte-identical to
    the original run's and only the labels shift.

    Idempotent: skips the work when out_path already exists.
    """
    if os.path.exists(out_path):
        print(f"replicated cache hit: {out_path}")
        return out_path

    lo, hi, rows = None, None, 0
    with open(src_path) as f:
        for line in f:
            _, _, ts = line.rstrip("\n").partition(",")
            if not ts:
                continue
            t = int(ts)
            lo = t if lo is None or t < lo else lo
            hi = t if hi is None or t > hi else hi
            rows += 1
    if rows == 0:
        raise RuntimeError(f"{src_path} has no usable rows")
    span = hi - lo + 1
    shift = -(-span // window_ms) * window_ms  # ceil division, then whole windows
    print(f"{rows} rows, ts span {span} ms -> shift {shift} ms per copy ({shift // window_ms} windows)")

    tmp = out_path + ".tmp"
    with open(tmp, "w") as fout:
        for k in range(copies):
            delta = k * shift
            with open(src_path) as fin:
                if delta == 0:
                    fout.writelines(fin)
                    continue
                out = []
                for line in fin:
                    value, _, ts = line.rstrip("\n").partition(",")
                    if not ts:
                        continue
                    out.append(f"{value},{int(ts) + delta}\n")
                    if len(out) >= 1_000_000:
                        fout.write("".join(out))
                        out.clear()
                fout.write("".join(out))
            print(f"  copy {k + 1}/{copies} written (+{delta} ms)")
    os.replace(tmp, out_path)
    print(f"replicated {rows * copies} rows -> {out_path}")
    return out_path


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "cluster_monitoring.csv"
    prepare(os.path.abspath(target))
