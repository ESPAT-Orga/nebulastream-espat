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

"""Shared worker-startup configuration consumed by every benchmark in scripts/benchmarking/.

These constants are NOT statistic-store specific. They live here so that worker_lifecycle.py and
new benchmarks can use them without depending on a particular benchmark's configs.py.
"""

import os


# Path to the local build dir relative to the cwd the benchmark is invoked from.
BUILD_DIR = os.path.join(".", "build_dir")
WORKING_DIR = os.path.abspath(os.path.join(BUILD_DIR, "working_dir"))

# Built executables.
SINGLE_NODE_EXECUTABLE = os.path.join(BUILD_DIR, "nes-single-node-worker/nes-single-node-worker")
NEBULI_EXECUTABLE = [os.path.join(BUILD_DIR, "nes-frontend/apps/nes-cli"), "--debug"]

# Throughput listener emits a measurement every X ms.
THROUGHPUT_LISTENER_INTERVAL = 100

# Seconds to wait between starting the worker and submitting the first query.
WAIT_BETWEEN_COMMANDS_LONG = 5

# A query that finishes naturally (file source exhausted) flips its status to "Stopped". We poll status
# WAIT_STABLE_CHECKS times, WAIT_CHECK_INTERVAL_S apart; only when all polls report "Stopped" do we
# treat the query as done.
WAIT_STABLE_CHECKS = 5
WAIT_CHECK_INTERVAL_S = 0.1
