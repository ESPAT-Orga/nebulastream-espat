# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script prints the volume of data arriving at a file sink
# It can be used to for visualization when reasoning about beckpressure behaviour
import time
import os
import sys
import argparse
import shutil

def wait_for_file(path, poll_interval=0.5):
    while not os.path.isfile(path):
        time.sleep(poll_interval)

def clear_screen():
    print("\033[2J\033[H", end="")

def draw_plot(file, values, width=60, height=15):
    if len(values) < 2:
        return

    values = values[-width:]
    vmin = min(values)
    vmax = max(values)

    # Avoid division by zero
    if vmax == vmin:
        vmax += 1e-6

    def scale(v):
        return int((v - vmin) / (vmax - vmin) * (height - 1))

    grid = [[" " for _ in range(width)] for _ in range(height)]

    prev_x, prev_y = None, None

    for x, v in enumerate(values):
        y = height - 1 - scale(v)

        grid[y][x] = "●"

        if prev_x is not None:
            # draw vertical continuity
            y0, y1 = sorted((prev_y, y))
            for yy in range(y0 + 1, y1):
                grid[yy][x] = "│"

        prev_x, prev_y = x, y

    print("\033[2J\033[H", end="")
    print(f"{file}: Lines/sec   min={vmin:.2f}  max={vmax:.2f}")
    for row in grid:
        print("".join(row))


def follow(
        file_path,
        poll_interval=0.1,
        report_interval=1.0,
        print_lines=True,
        plot=False,
):
    wait_for_file(file_path)

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        f.seek(0, os.SEEK_END)

        buffer = ""
        line_count = 0
        last_report = time.monotonic()
        rates = []

        term_width = shutil.get_terminal_size((80, 20)).columns
        plot_width = min(60, term_width - 2)

        while True:
            chunk = f.read()
            if not chunk:
                time.sleep(poll_interval)
            else:
                buffer += chunk
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if print_lines:
                        print(line)
                    line_count += 1

            now = time.monotonic()
            elapsed = now - last_report

            if elapsed >= report_interval:
                rate = line_count / elapsed
                line_count = 0
                last_report = now

                if plot:
                    rates.append(rate)
                    draw_plot(file_path, rates, width=plot_width)
                elif not print_lines:
                    print(f"{rate:.2f} lines/sec")

def main():
    parser = argparse.ArgumentParser(description="Follow a file as it grows.")
    parser.add_argument("file", help="Path to the file to watch")
    parser.add_argument("--freq", action="store_true", help="Show line frequency")
    parser.add_argument("--plot", action="store_true", help="Live plot frequency")

    args = parser.parse_args()

    follow(
        args.file,
        print_lines=not (args.freq or args.plot),
        plot=args.plot,
    )

if __name__ == "__main__":
    main()
