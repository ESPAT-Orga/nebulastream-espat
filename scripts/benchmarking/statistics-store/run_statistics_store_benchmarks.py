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
import shlex
import os

from scripts.benchmarking.utils import *

#### This script builds and then runs the StatisticStoreBenchmark target. For changing the benchmark params, please see
#### nes-statistics/src/StatisticStore/StatisticStoreBenchmark.cpp
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
               "-DNES_LOG_LEVEL:STRING=LEVEL_NONE ")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run statistic-store-benchmark.")
    parser.add_argument("--clean", action="store_true",
                        help="Remove and recreate the build directory before building.")
    parser.add_argument("--filter",
                        help="Comma-separated keywords; only configs whose report line contains ALL keywords are run. Example: get,ws=60000")
    parser.add_argument("--exclude", action="append", default=[],
                    help="Comma-separated keywords per group; a config is skipped if ALL keywords in ANY group match. Can be repeated. Example: --exclude DEFAULT,threads=16 --exclude Get,ids=    1")
    args = parser.parse_args()

    # Checking if the script has been executed from the repository root
    check_repository_root()

    # Optionally clean the build directory
    if args.clean:
        create_folder_and_remove_if_exists(build_dir)

    # Build NebulaStream
    compile_nebulastream(cmake_flags, build_dir)

    # Run the benchmark
    benchmark_command = benchmark_executable
    if args.filter:
        benchmark_command += f" --filter '{args.filter}'"
    for exc in args.exclude:
        benchmark_command += f" --exclude {shlex.quote(exc)}"

    printInfo("Running statistic-store-benchmark...")
    run_command_and_show_output(benchmark_command)
    printInfo(f"\nCSV Measurement file can be found in {csv_file_path}")
