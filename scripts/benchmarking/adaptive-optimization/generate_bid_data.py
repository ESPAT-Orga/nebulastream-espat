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
    bidValue  FLOAT64 -- Normal(mean, stddev) per --bid-mean / --bid-stddev
    price     FLOAT64 -- Normal(mean, stddev) per --price-mean / --price-stddev

The CSV is comma-separated, newline-terminated, no header, no quoting.

Two predefined "regimes" are used by the adaptive benchmark to simulate a workload-distribution
shift between bid-first and price-first being the optimal filter ordering:

    A (default): bidValue ~ N(50, 17), price ~ N(500, 167)
        bidValue < 10.45  → ~1% pass  (selective)
        price    < 888.49 → ~99% pass (non-selective)
        => bid-first is the cheap order.

    B (flipped): bidValue ~ N(-30, 17), price ~ N(1277, 167)
        bidValue < 10.45  → ~99% pass (non-selective)
        price    < 888.49 → ~1% pass  (selective)
        => price-first is the cheap order.

ensure_dataset_a() and ensure_dataset_b() are convenience wrappers used by the benchmark scripts.

Usage:
    python -m scripts.benchmarking.adaptive-optimization.generate_bid_data
    python -m scripts.benchmarking.adaptive-optimization.generate_bid_data --regime B
    python -m scripts.benchmarking.adaptive-optimization.generate_bid_data --bid-mean 0 --bid-stddev 50
"""

import argparse
import os
import sys

import numpy as np

DEFAULT_COUNT = 30_000_000
DEFAULT_SEED = 1
_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Regime parameters (mean / stddev for bidValue and price).
REGIME_A = {"bid_mean": 50.0, "bid_stddev": 17.0, "price_mean": 500.0, "price_stddev": 167.0}
REGIME_B = {"bid_mean": -30.0, "bid_stddev": 17.0, "price_mean": 1277.0, "price_stddev": 167.0}

DEFAULT_OUTPUT_A = os.path.join(_DATA_DIR, "bid_A_30M.csv")
DEFAULT_OUTPUT_B = os.path.join(_DATA_DIR, "bid_B_30M.csv")


def _generate(
    output_path: str,
    count: int,
    seed: int,
    bid_mean: float,
    bid_stddev: float,
    price_mean: float,
    price_stddev: float,
    chunk_size: int = 1_000_000,
) -> None:
    """Write `count` rows of synthetic bid data to `output_path` as CSV."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    rng = np.random.default_rng(seed)

    tmp_path = output_path + ".tmp"
    written = 0
    with open(tmp_path, "wb") as f:
        while written < count:
            n = min(chunk_size, count - written)
            timestamps = np.arange(written, written + n, dtype=np.uint64)
            auction_ids = np.arange(written, written + n, dtype=np.int32)
            bid_values = rng.normal(loc=bid_mean, scale=bid_stddev, size=n).astype(np.float64)
            prices = rng.normal(loc=price_mean, scale=price_stddev, size=n).astype(np.float64)

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
    bid_mean: float = REGIME_A["bid_mean"],
    bid_stddev: float = REGIME_A["bid_stddev"],
    price_mean: float = REGIME_A["price_mean"],
    price_stddev: float = REGIME_A["price_stddev"],
) -> str:
    """Generate the dataset if it does not already exist. Returns the absolute path."""
    abs_path = os.path.abspath(path)
    if os.path.exists(abs_path) and os.path.getsize(abs_path) > 0:
        print(f"[generate_bid_data] reusing existing dataset: {abs_path} ({os.path.getsize(abs_path):,} bytes)", flush=True)
        return abs_path
    print(
        f"[generate_bid_data] generating {count:,} rows "
        f"(bid~N({bid_mean},{bid_stddev}), price~N({price_mean},{price_stddev})) -> {abs_path}",
        flush=True,
    )
    _generate(abs_path, count, seed, bid_mean, bid_stddev, price_mean, price_stddev)
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
        help="Predefined distribution regime to generate. 'custom' uses the --bid-* / --price-* flags.",
    )
    parser.add_argument("--bid-mean", type=float, default=None, help="Mean of the bidValue normal distribution (custom only).")
    parser.add_argument("--bid-stddev", type=float, default=None, help="Stddev of the bidValue normal distribution (custom only).")
    parser.add_argument("--price-mean", type=float, default=None, help="Mean of the price normal distribution (custom only).")
    parser.add_argument("--price-stddev", type=float, default=None, help="Stddev of the price normal distribution (custom only).")
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
        if None in (args.bid_mean, args.bid_stddev, args.price_mean, args.price_stddev):
            print("--bid-mean, --bid-stddev, --price-mean, --price-stddev are all required with --regime custom", file=sys.stderr)
            return 2
        maybe_force(args.output)
        ensure_dataset(
            path=args.output,
            count=args.count,
            seed=args.seed if args.seed is not None else DEFAULT_SEED,
            bid_mean=args.bid_mean,
            bid_stddev=args.bid_stddev,
            price_mean=args.price_mean,
            price_stddev=args.price_stddev,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
