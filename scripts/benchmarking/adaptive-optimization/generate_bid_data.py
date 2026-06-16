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
Pre-generate the synthetic bid dataset(s) that the adaptive-optimization benchmarks ingest via
the Memory source.

Schema (matches the logical `bid` source in the benchmark scripts):
    timestamp UINT64  -- monotonic sequence starting at 0
    auctionId INT32   -- monotonic sequence starting at 0
    bidValue  FLOAT64 -- low/high cluster straddling BID_THRESHOLD
    price     FLOAT64 -- low/high cluster straddling PRICE_THRESHOLD

The CSV is comma-separated, newline-terminated, no header, no quoting.

Selectivity is controlled EXACTLY, not via a distribution tail. Each field is drawn from one
of two tight clusters (a "pass" cluster below the filter threshold and a "fail" cluster above
it). A regime says what fraction of rows land in each field's pass cluster, and exactly that
fraction is assigned per chunk — so the realized selectivity hits the target precisely. The
clusters are narrow (CLUSTER_STDDEV) so the boundary is sharp: no values sit near the
threshold and jitter never crosses it. The two fields are assigned independently, so the
combined (AND) selectivity is the product of the two pass fractions.

    A (default): bidValue 1% pass (selective),  price 99% pass (non-selective)
        => bid-first is the cheap order.

    B (flipped): bidValue 99% pass (non-selective), price 1% pass (selective)
        => price-first is the cheap order.

ensure_dataset_a() and ensure_dataset_b() are convenience wrappers used by the benchmark scripts.

Usage:
    python -m scripts.benchmarking.adaptive-optimization.generate_bid_data
    python -m scripts.benchmarking.adaptive-optimization.generate_bid_data --regime B
    python -m scripts.benchmarking.adaptive-optimization.generate_bid_data --regime custom \
        --bid-pass 0.1 --price-pass 0.9 --output data/custom.csv
"""

import argparse
import os
import sys

import numpy as np

DEFAULT_COUNT = 30_000_000
DEFAULT_SEED = 1
_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Filter thresholds — must match the SQL filters in run_adaptive_optimization_benchmark.py
# (bidValue < 20.45, price < 888.49). The absolute values are arbitrary; only the pass
# fractions below matter, since selectivity is set by construction.
BID_THRESHOLD = 20.45
PRICE_THRESHOLD = 888.49

# Low (pass, below threshold) and high (fail, above threshold) cluster centers, placed well
# clear of each threshold on both sides so the sharp clusters never straddle it. The bid pass
# cluster is held at 10 (>=0 by >5 stddev) so every bidValue stays non-negative: the statistic
# histogram has min=0 and indexes bins via an unsigned (value-min)/binWidth, so a negative value
# would hit an undefined float->uint clamp. A non-negative field keeps binning well-defined.
BID_PASS_MEAN, BID_FAIL_MEAN = 10.0, 30.0
PRICE_PASS_MEAN, PRICE_FAIL_MEAN = 800.0, 980.0

# Cluster spread. Small => sharp boundary. Kept far below the gap from each cluster center to
# its threshold (~10 for bid, ~88/91 for price) so a value never lands on the wrong side and
# the per-chunk pass count stays exact.
CLUSTER_STDDEV = 1.0

# Regime parameters: fraction of rows whose field lands in the pass cluster (i.e. selectivity).
REGIME_A = {"bid_pass": 0.01, "price_pass": 0.99}
REGIME_B = {"bid_pass": 0.99, "price_pass": 0.01}

DEFAULT_OUTPUT_A = os.path.join(_DATA_DIR, "bid_A_30M.csv")
DEFAULT_OUTPUT_B = os.path.join(_DATA_DIR, "bid_B_30M.csv")


def _make_field(n, pass_frac, pass_mean, fail_mean, stddev, mask_rng, jitter_rng):
    """Build `n` values where EXACTLY round(pass_frac * n) land in the (low) pass cluster.

    `mask_rng` picks which rows pass (independent per field); `jitter_rng` adds the narrow
    spread. The exact count comes from the assignment, not from a distribution tail.
    """
    n_pass = int(round(pass_frac * n))
    centers = np.full(n, fail_mean, dtype=np.float64)
    if n_pass > 0:
        pass_idx = mask_rng.choice(n, size=n_pass, replace=False)
        centers[pass_idx] = pass_mean
    return centers + jitter_rng.normal(0.0, stddev, size=n)


def _generate(
    output_path: str,
    count: int,
    seed: int,
    bid_pass: float,
    price_pass: float,
    chunk_size: int = 1_000_000,
) -> None:
    """Write `count` rows of synthetic bid data to `output_path` as CSV."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    # Independent RNG streams: which bid rows pass, bid jitter, which price rows pass, price
    # jitter. Independent pass masks => bidValue and price selectivities are uncorrelated.
    bid_mask_rng, bid_jit_rng, price_mask_rng, price_jit_rng = (
        np.random.default_rng(s) for s in np.random.SeedSequence(seed).spawn(4)
    )

    tmp_path = output_path + ".tmp"
    written = 0
    with open(tmp_path, "wb") as f:
        while written < count:
            n = min(chunk_size, count - written)
            timestamps = np.arange(written, written + n, dtype=np.uint64)
            auction_ids = np.arange(written, written + n, dtype=np.int32)
            bid_values = _make_field(n, bid_pass, BID_PASS_MEAN, BID_FAIL_MEAN, CLUSTER_STDDEV, bid_mask_rng, bid_jit_rng)
            prices = _make_field(n, price_pass, PRICE_PASS_MEAN, PRICE_FAIL_MEAN, CLUSTER_STDDEV, price_mask_rng, price_jit_rng)

            lines = (
                np.char.add(
                    np.char.add(
                        np.char.add(
                            np.char.add(
                                np.char.add(
                                    np.char.add(timestamps.astype(str), ","),
                                    auction_ids.astype(str),
                                ),
                                ",",
                            ),
                            np.char.mod("%.10g", bid_values),
                        ),
                        ",",
                    ),
                    np.char.mod("%.10g", prices),
                )
                + "\n"
            )
            f.write("".join(lines.tolist()).encode("ascii"))

            written += n
            if written % (chunk_size * 10) == 0 or written == count:
                pct = 100 * written / count
                print(f"  generated {written:,} / {count:,} rows ({pct:.1f}%)", flush=True)

    os.replace(tmp_path, output_path)


def ensure_dataset(
    path: str,
    count: int = DEFAULT_COUNT,
    seed: int = DEFAULT_SEED,
    bid_pass: float = REGIME_A["bid_pass"],
    price_pass: float = REGIME_A["price_pass"],
) -> str:
    """Generate the dataset if it does not already exist. Returns the absolute path."""
    abs_path = os.path.abspath(path)
    if os.path.exists(abs_path) and os.path.getsize(abs_path) > 0:
        print(f"[generate_bid_data] reusing existing dataset: {abs_path} ({os.path.getsize(abs_path):,} bytes)", flush=True)
        return abs_path
    print(
        f"[generate_bid_data] generating {count:,} rows "
        f"(bidValue {bid_pass:.0%} pass, price {price_pass:.0%} pass) -> {abs_path}",
        flush=True,
    )
    _generate(abs_path, count, seed, bid_pass, price_pass)
    print(f"[generate_bid_data] done: {abs_path} ({os.path.getsize(abs_path):,} bytes)", flush=True)
    return abs_path


def ensure_dataset_a(path: str = DEFAULT_OUTPUT_A, count: int = DEFAULT_COUNT, seed: int = DEFAULT_SEED) -> str:
    """Convenience wrapper: generate the regime-A dataset (bid-first wins)."""
    return ensure_dataset(path=path, count=count, seed=seed, **REGIME_A)


def ensure_dataset_b(path: str = DEFAULT_OUTPUT_B, count: int = DEFAULT_COUNT, seed: int = DEFAULT_SEED + 1) -> str:
    """Convenience wrapper: generate the regime-B dataset (price-first wins)."""
    return ensure_dataset(path=path, count=count, seed=seed, **REGIME_B)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate the synthetic bid CSV for the memory-source benchmarks.")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help=f"Number of rows to generate (default: {DEFAULT_COUNT}).")
    parser.add_argument("--output", default=None, help="Output CSV path (default: depends on --regime).")
    parser.add_argument("--seed", type=int, default=None, help=f"Random seed (default: {DEFAULT_SEED} for A, {DEFAULT_SEED + 1} for B).")
    parser.add_argument("--force", action="store_true", help="Regenerate even if the output file already exists.")
    parser.add_argument(
        "--regime",
        choices=["A", "B", "both", "custom"],
        default="A",
        help="Predefined selectivity regime to generate. 'custom' uses --bid-pass / --price-pass.",
    )
    parser.add_argument("--bid-pass", type=float, default=None, help="Fraction of rows passing the bidValue filter (custom only).")
    parser.add_argument("--price-pass", type=float, default=None, help="Fraction of rows passing the price filter (custom only).")
    args = parser.parse_args()

    def maybe_force(path: str) -> None:
        abs_path = os.path.abspath(path)
        if args.force and os.path.exists(abs_path):
            os.remove(abs_path)

    if args.regime == "A":
        out = args.output or DEFAULT_OUTPUT_A
        maybe_force(out)
        ensure_dataset_a(path=out, count=args.count, seed=args.seed if args.seed is not None else DEFAULT_SEED)
    elif args.regime == "B":
        out = args.output or DEFAULT_OUTPUT_B
        maybe_force(out)
        ensure_dataset_b(path=out, count=args.count, seed=args.seed if args.seed is not None else DEFAULT_SEED + 1)
    elif args.regime == "both":
        maybe_force(DEFAULT_OUTPUT_A)
        maybe_force(DEFAULT_OUTPUT_B)
        ensure_dataset_a(count=args.count)
        ensure_dataset_b(count=args.count)
    else:  # custom
        if args.output is None:
            print("--output is required with --regime custom", file=sys.stderr)
            return 2
        if None in (args.bid_pass, args.price_pass):
            print("--bid-pass and --price-pass are both required with --regime custom", file=sys.stderr)
            return 2
        maybe_force(args.output)
        ensure_dataset(
            path=args.output,
            count=args.count,
            seed=args.seed if args.seed is not None else DEFAULT_SEED,
            bid_pass=args.bid_pass,
            price_pass=args.price_pass,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
