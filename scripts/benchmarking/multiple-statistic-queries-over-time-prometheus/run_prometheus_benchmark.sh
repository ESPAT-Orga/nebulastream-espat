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

set -euo pipefail

# Output dir: respect a pre-set OUTPUT_DIR. Defaults to a timestamped sibling.
OUTPUT_DIR="${OUTPUT_DIR:-benchmark_run_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"
export OUTPUT_DIR

# Conditions are four deployment tiers; each one launches the combined nes-single-node-worker
# + (optional) Prometheus container with a different cgroup budget (config.CONDITION_BUDGETS):
#   cloud      — no --cpus / --memory caps (server-class).
#   edge_heavy — EDGE_HEAVY_CPUS / EDGE_HEAVY_MEMORY (default 8 cpus, 8g; workstation class).
#   edge_light — EDGE_LIGHT_CPUS / EDGE_LIGHT_MEMORY (default 4 cpus, 4g; NUC/gateway class).
#   sensor     — SENSOR_CPUS / SENSOR_MEMORY (default 1 cpu, 1g; Pi/Jetson class).
# statistic_build keeps Prometheus disabled, prometheus_sink starts both processes inside the
# same container so they share one cgroup.
echo "Conditions:         ${CONDITIONS:-${CONDITION:-<default: cloud,edge_heavy,edge_light,sensor>}}"
echo "EDGE_HEAVY_CPUS:    ${EDGE_HEAVY_CPUS:-<default 8>}"
echo "EDGE_HEAVY_MEMORY:  ${EDGE_HEAVY_MEMORY:-<default 8g>}"
echo "EDGE_LIGHT_CPUS:    ${EDGE_LIGHT_CPUS:-<default 4>}"
echo "EDGE_LIGHT_MEMORY:  ${EDGE_LIGHT_MEMORY:-<default 4g>}"
echo "SENSOR_CPUS:        ${SENSOR_CPUS:-<default 1>}"
echo "SENSOR_MEMORY:      ${SENSOR_MEMORY:-<default 1g>}"
echo "Total queries/run:  ${TOTAL_QUERIES_PER_RUN:-<default 20>}"
echo "Launch interval:    ${QUERY_LAUNCH_INTERVAL_SECONDS:-<default 3s>}"
echo "TCP_PORT_BASE:      ${TCP_PORT_BASE:-<default 9100>}"

# Build the runtime base image first. The combined image FROMs nes-runtime-base:test and that
# tag is NOT on Docker Hub, so a fresh host needs it built locally. Idempotent / layer-cached.
RUNTIME_BASE_DOCKERFILE="docker/runtime/RuntimeBase.dockerfile"
RUNTIME_BASE_IMAGE="nes-runtime-base:test"
if [[ -f "$RUNTIME_BASE_DOCKERFILE" ]]; then
    if ! docker image inspect "$RUNTIME_BASE_IMAGE" >/dev/null 2>&1; then
        echo "Building Docker image $RUNTIME_BASE_IMAGE (one-time, layer-cached afterwards)..."
        docker build -t "$RUNTIME_BASE_IMAGE" -f "$RUNTIME_BASE_DOCKERFILE" docker/runtime
    fi
else
    echo "WARNING: $RUNTIME_BASE_DOCKERFILE not found; the runner will try to build the image itself." >&2
fi

# Build the combined Docker image up front (idempotent, layer-cached). The Python runner also
# does this on demand, but doing it here makes first-time setup explicit in the log.
DOCKERFILE="docker/single-node-worker/SingleNodeWorkerWithPrometheus.dockerfile"
IMAGE="${WORKER_DOCKER_IMAGE:-nes-bench-prom-combined:local}"
if [[ -f "$DOCKERFILE" ]]; then
    echo "Building Docker image $IMAGE (cached if unchanged)..."
    docker build -t "$IMAGE" -f "$DOCKERFILE" docker/single-node-worker
else
    echo "WARNING: $DOCKERFILE not found; the runner will try to build the image itself." >&2
fi

# Build the Rust TCP producer image (idempotent, layer-cached). Pushes tuple generation out of
# the worker process so the cgroup CPU budget can go to query execution.
TCPGEN_IMAGE="${TCP_GENERATOR_IMAGE:-nes-bench-tcp-gen:local}"
TCPGEN_CTX="rust-tcp-generator"
if [[ -f "$TCPGEN_CTX/Dockerfile" ]]; then
    echo "Building Docker image $TCPGEN_IMAGE (cached if unchanged)..."
    docker build -t "$TCPGEN_IMAGE" "$TCPGEN_CTX"
else
    echo "WARNING: $TCPGEN_CTX/Dockerfile not found; the runner will try to build it itself." >&2
fi

# Python venv: run_prometheus_benchmark.py only uses stdlib + scripts.benchmarking.utils, so
# no third-party deps are required. The venv is for isolation only and is torn down on exit.
python3 -m venv myenv
source myenv/bin/activate
trap 'deactivate 2>/dev/null || true; rm -rf myenv' EXIT

# The runner cmake-configures + builds NebulaStream into $BUILD_DIR (defaults to ./build_dir)
# before running. Set SKIP_BUILD=1 to point at an already-built tree without reconfiguring it.
#
# Common overrides:
#   TOTAL_QUERIES_PER_RUN=5 MEASUREMENT_WINDOW_SECONDS=15 SKIP_BUILD=1 \
#       bash scripts/.../run_prometheus_benchmark.sh        # quick smoke
#   CONDITION=sensor SENSOR_CPUS=0.5 SENSOR_MEMORY=512m SKIP_BUILD=1 \
#       bash scripts/.../run_prometheus_benchmark.sh        # custom-tight sensor tier
myenv/bin/python3 -m scripts.benchmarking.multiple-statistic-queries-over-time-prometheus.run_prometheus_benchmark 2>&1 \
    | tee "$OUTPUT_DIR/run.log"
