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
Python script that builds and runs the statistic-store-benchmark target,
then cleans the CSV output.
"""

import argparse
import os

from scripts.benchmarking.utils import *

#### Benchmark Configurations
build_dir = os.path.join(".", "build_dir")
csv_file_path = os.path.abspath(os.path.join(os.getcwd(), "statistic_store_benchmark.csv"))
benchmark_executable = os.path.join(build_dir, "nes-statistics", "src", "StatisticStore", "statistic-store-benchmark")
cmake_flags = ("-G Ninja "
               "-DCMAKE_BUILD_TYPE=Release "
               f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
               "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
               "-DENABLE_LARGE_TESTS=0 "
               "-DNES_BUILD_NATIVE:BOOL=ON "
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
               "-DNES_BUILD_NATIVE:BOOL=ON")


def clean_csv(csv_path):
    """Remove all lines from the head of the CSV until a line starting with 'name,iterations,real_time'."""
    with open(csv_path, 'r') as f:
        lines = f.readlines()

    start_idx = None
    for idx, line in enumerate(lines):
        if line.startswith("name,iterations,real_time"):
            start_idx = idx
            break

    if start_idx is None:
        printError(f"Could not find header line starting with 'name,iterations,real_time' in {csv_path}")
        return

    with open(csv_path, 'w') as f:
        f.writelines(lines[start_idx:])

    printSuccess(f"Cleaned CSV: removed {start_idx} header lines.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run statistic-store-benchmark.")
    parser.add_argument("--clean", action="store_true",
                        help="Remove and recreate the build directory before building.")
    args = parser.parse_args()

    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Optionally clean the build directory
    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    # Build NebulaStream
    compile_nebulastream(cmake_flags, build_dir)

    # Run the benchmark
    benchmark_command = (f"{benchmark_executable} "
                         f"--benchmark_out={csv_file_path} "
                         f"--benchmark_out_format=csv")

    printInfo("Running statistic-store-benchmark...")
    run_command(benchmark_command)

    # Clean the CSV output
    clean_csv(csv_file_path)

    printInfo(f"\nCSV Measurement file can be found in {csv_file_path}")
