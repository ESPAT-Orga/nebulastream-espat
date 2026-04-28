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

"""Console formatting helpers for the statistic build/probe runners.

A run iterates over a large parameter grid; the helpers here keep the per-step
output short, indented under a per-experiment banner, and labelled with elapsed
time so the user can scan a transcript and see what's slow.
"""

import contextlib
import sys
import time

LABEL_WIDTH = 14
RIGHT_COL = 56

# Module-level counter incremented every time a ``step()`` block exits
# (success or exception). The runners use it to know how many printed lines
# to wipe before re-running the same combination — see ``clear_lines()``.
_step_counter = 0


def reset_step_counter() -> None:
    global _step_counter
    _step_counter = 0


def get_step_counter() -> int:
    return _step_counter


def clear_lines(n: int) -> None:
    """Move cursor up *n* lines and erase from there to the end of the screen.

    No-op when stdout is not a TTY (e.g., piped to a file): the in-place
    overwrite would leak escape sequences into the log, so we just leave the
    previous run's lines in place.
    """
    if n <= 0 or not sys.stdout.isatty():
        return
    sys.stdout.write(f"\r\033[{n}A\033[0J")
    sys.stdout.flush()


def banner(line: str) -> None:
    bar = "=" * 80
    sys.stdout.write(bar + "\n")
    sys.stdout.write(line + "\n")
    sys.stdout.write(bar + "\n")
    sys.stdout.flush()


def format_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:5.1f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s"


@contextlib.contextmanager
def step(label: str, *, detail: str = ""):
    """Print a labelled, indented step header and overwrite it on exit with elapsed time.

    Yields a callable used to attach trailing key=value info that gets appended
    to the closing line (e.g. throughput, accuracy numbers).
    """
    global _step_counter
    extra: list[str] = []
    start = time.time()
    head = f"  [{label:<{LABEL_WIDTH}}] {detail}"
    sys.stdout.write(head)
    sys.stdout.flush()
    try:
        try:
            yield extra.append
        except BaseException:
            sys.stdout.write("\n")
            sys.stdout.flush()
            raise
        elapsed = format_elapsed(time.time() - start)
        tail = elapsed + (("   " + "  ".join(extra)) if extra else "")
        pad = max(1, RIGHT_COL - len(head))
        sys.stdout.write("\r" + head + (" " * pad) + tail + "\n")
        sys.stdout.flush()
    finally:
        _step_counter += 1


class ProgressBar:
    """Single-line in-place progress bar for tight inner loops.

    The bar is drawn with carriage returns so each tick overwrites the prior
    state. The enclosing ``step`` rewrites the whole line on exit, so the final
    transcript shows just the step's closing ``done`` line — the bar is purely
    a live feedback affordance during execution.
    """

    def __init__(self, total: int, *, label: str = "progress", width: int = 30):
        self.total = max(1, total)
        self.label = label
        self.width = width
        self.done = 0

    def tick(self, n: int = 1) -> None:
        self.done = min(self.total, self.done + n)
        filled = int(self.width * self.done / self.total)
        bar = "." * filled + " " * (self.width - filled)
        sys.stdout.write(
            f"\r  [{self.label:<{LABEL_WIDTH}}] [{bar}] {self.done}/{self.total}"
        )
        sys.stdout.flush()
