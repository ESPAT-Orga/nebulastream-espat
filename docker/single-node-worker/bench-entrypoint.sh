#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Combined-runtime entrypoint for the over-time Prometheus benchmark. Runs
# nes-single-node-worker as PID 1 so `docker stop` delivers SIGTERM directly to
# the worker; optionally backgrounds the Prometheus server inside the same
# container (and therefore the same cgroup) when NES_RUN_PROMETHEUS=1.
#
# Environment knobs (set by the benchmark runner via `docker run -e`):
#   NES_RUN_PROMETHEUS       0|1, default 0. When 1, spawn the Prometheus server
#                            before exec'ing the worker.
#   NES_PROM_PORT_BASE       Loopback port of the first sink exposer (e.g. 8800).
#   NES_PROM_NUM_TARGETS     How many contiguous ports starting at PORT_BASE; the
#                            entrypoint generates targets PORT_BASE..PORT_BASE+N-1
#                            on 127.0.0.1. Used in place of NES_PROM_TARGETS when
#                            set, keeping `docker run` argv compact.
#   NES_PROM_TARGETS         Fallback: explicit comma-separated host:port list.
#                            Honored when NES_PROM_NUM_TARGETS is unset (e.g. for
#                            heterogeneous targets or one-offs).
#   NES_PROM_SCRAPE_INTERVAL Prometheus scrape interval, default 1s.
#   NES_PROM_WEB_LISTEN      Prometheus HTTP listen address, default :9090.
#   PROM_DATA_DIR            TSDB path, default /tmp/prom-data (set in Dockerfile).
#
# Anything after the env block (i.e. the docker-run CMD) is exec'd as the
# nes-single-node-worker command line.

set -euo pipefail

if [[ "${NES_RUN_PROMETHEUS:-0}" == "1" ]]; then
    ### Targets come from either NES_PROM_PORT_BASE+NES_PROM_NUM_TARGETS (compact range form, used by
    ### the runner so docker-run argv doesn't blow up to N×16 bytes) or NES_PROM_TARGETS (explicit
    ### CSV, used when ports aren't a contiguous range).
    if [[ -n "${NES_PROM_NUM_TARGETS:-}" && -n "${NES_PROM_PORT_BASE:-}" ]]; then
        targets=""
        for ((i=0; i<NES_PROM_NUM_TARGETS; i++)); do
            port=$((NES_PROM_PORT_BASE + i))
            targets="${targets:+$targets,}127.0.0.1:${port}"
        done
    elif [[ -n "${NES_PROM_TARGETS:-}" ]]; then
        targets="$NES_PROM_TARGETS"
    else
        echo "ERROR: NES_RUN_PROMETHEUS=1 but neither NES_PROM_NUM_TARGETS+NES_PROM_PORT_BASE nor NES_PROM_TARGETS is set" >&2
        exit 2
    fi

    scrape_interval="${NES_PROM_SCRAPE_INTERVAL:-1s}"
    web_listen="${NES_PROM_WEB_LISTEN:-:9090}"
    data_dir="${PROM_DATA_DIR:-/tmp/prom-data}"
    config="/tmp/prometheus.yml"

    ### Convert "host1:p1,host2:p2,..." into "'host1:p1', 'host2:p2', ..."
    formatted=$(echo "$targets" | sed -E "s/([^,]+)/'\\1'/g")

    cat > "$config" <<EOF
global:
  scrape_interval: ${scrape_interval}
  evaluation_interval: ${scrape_interval}
scrape_configs:
  - job_name: 'nes'
    static_configs:
      - targets: [${formatted}]
EOF

    mkdir -p "$data_dir"
    echo "Starting Prometheus (web=${web_listen}, scrape_interval=${scrape_interval}, $(echo "$targets" | tr ',' '\n' | wc -l) targets)"
    /usr/bin/prometheus \
        --config.file="$config" \
        --web.listen-address="$web_listen" \
        --storage.tsdb.path="$data_dir" \
        >/var/log/prometheus.log 2>&1 &
fi

### exec so the worker becomes PID 1 — docker stop's SIGTERM lands directly on it.
exec /usr/bin/nes-single-node-worker "$@"
