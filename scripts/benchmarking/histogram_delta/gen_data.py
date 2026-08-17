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

"""Generate a synthetic CSV for the histogram-delta throughput benchmark.

Columns (no header): ``value,timestamp`` (both UINT64).
  - ``value``     : the field the histogram is built over, spread across [0, value_range).
  - ``timestamp`` : event time (ms), monotonically increasing, ``rows_per_window`` rows per
                    1-second window, so a WINDOW TUMBLING(timestamp, size 1 sec) yields
                    ``rows / rows_per_window`` populated windows (=> the delta GEN/RESOLVER
                    lower runs that many times, exercising the keyframe machinery).

Pure-Python (no numpy) so it runs anywhere. The file is read by a File source with a CSV parser
(see histogram_delta/queries/*.yaml.template).
"""

import argparse
import random


def generate(path, rows, rows_per_window, value_range, seed=1):
    rng = random.Random(seed)
    randrange = rng.randrange
    buf = []
    buf_append = buf.append
    written = 0
    with open(path, "w") as f:
        for i in range(rows):
            window = i // rows_per_window
            offset = i % rows_per_window
            ts = window * 1000 + (offset % 1000)
            buf_append(f"{randrange(value_range)},{ts}\n")
            if len(buf) >= 1_000_000:
                f.write("".join(buf))
                buf.clear()
            written += 1
        if buf:
            f.write("".join(buf))
    return written


def main():
    ap = argparse.ArgumentParser(description="Generate synthetic value,timestamp CSV for the histogram-delta benchmark.")
    ap.add_argument("path", help="output CSV path")
    ap.add_argument("--rows", type=int, default=10_000_000, help="number of tuples (default 10M)")
    ap.add_argument("--rows-per-window", type=int, default=1000,
                    help="tuples per 1-second window (default 1000 => rows/1000 windows)")
    ap.add_argument("--value-range", type=int, default=100_000, help="values are uniform in [0, this) (default 100000)")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    if not 1 <= args.rows_per_window <= 1000:
        raise SystemExit("rows-per-window must be in [1, 1000] so tuples stay within their 1-second window")
    n = generate(args.path, args.rows, args.rows_per_window, args.value_range, args.seed)
    windows = (args.rows + args.rows_per_window - 1) // args.rows_per_window
    print(f"wrote {n} rows ({windows} windows) to {args.path}")


if __name__ == "__main__":
    main()
