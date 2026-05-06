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

"""Sweep dimensions for the network-sink benchmark.

The benchmark drives two single-node-workers on localhost. worker-1 runs a GeneratorSource and a
Network sink that ships tuples to worker-2. We sweep over:
  * MAX_NETWORK_RATES          — cap on the network channel (applied via tc qdisc tbf on `lo` if
                                  available; otherwise the run logs a warning and proceeds without).
  * GENERATOR_RATE_TYPES       — GeneratorRate::Type (FIXED, SINUS, ...). Drives ingestion shape.
  * GENERATOR_RATE_CONFIGS     — emit-rate string forwarded as `generator_rate_config`.
  * SENDING_STRATEGIES         — NetworkSinkSendingStrategy chosen by the worker (ALWAYS_SEND,
                                  ADAPTIVE_DIFFERENT_PRIO).
  * QUERY_DURATION_SEC         — how long each query is allowed to run before we stop and read logs.
"""

import os


## Worker port assignment ######################################################

# worker-1: source-side, hosts the GeneratorSource.
# worker-2: sink-side, the Network sink ships data to its data port.
WORKER_1_GRPC = "127.0.0.1:18080"
WORKER_1_DATA = "127.0.0.1:19090"
WORKER_2_GRPC = "127.0.0.1:18081"
WORKER_2_DATA = "127.0.0.1:19091"


## Sweep dimensions ############################################################

# tc qdisc tbf rate values. "none" disables the throttle entirely. Applied to the loopback interface;
# requires CAP_NET_ADMIN (run with sudo) — otherwise the run logs a warning and proceeds unthrottled.
MAX_NETWORK_RATES = ["none", "10kbit", "100kbit"]

# GeneratorRate::Type variants the sweep iterates over.
GENERATOR_RATE_TYPES = ["FIXED", "SINUS"]

# Per-rate-type config string forwarded as `generator_rate_config` in the source descriptor.
GENERATOR_RATE_CONFIGS = {
    "FIXED": "emit_rate 100000",
    "SINUS": "emit_rate 100000 amplitude 50000 period_ms 5000",
}

# NetworkSinkSendingStrategy variants the sweep iterates over.
SENDING_STRATEGIES = ["ALWAYS_SEND", "ADAPTIVE_DIFFERENT_PRIO"]

# Repetitions per sweep point.
NUM_RUNS_PER_EXPERIMENT = 1

# How long each query runs before we tear down and parse logs.
QUERY_DURATION_SEC = 10


## Query templates #############################################################

QUERY_TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query-templates")


## Result file #################################################################

RESULT_FIELDNAMES = [
    "run_idx",
    "max_network_rate",
    "rate_type",
    "rate_config",
    "strategy",
    "query_id",
    "priority",
    "throughput_tuples_per_s",
    "duration_s",
    "issue",
]
