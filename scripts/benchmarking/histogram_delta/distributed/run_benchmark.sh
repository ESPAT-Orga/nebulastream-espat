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

# Distributed statistic-collection benchmark runner. Expects NebulaStream already built into
# BUILD_DIR (defaults to ./build_dir); the worker binary is bind-mounted into the containers, so a
# rebuild does not require rebuilding the Docker image. Builds the reused Docker images if missing.
#
# Quick smoke:
#   SOURCES_PER_LEAF=1 RUN_DURATION_SECONDS=15 VARIANTS=local \
#       bash scripts/benchmarking/histogram_delta/distributed/run_benchmark.sh

set -euo pipefail

# If BUILD_DIR is unset, default it to a `build_dir` next to this script. NES is configured+built INSIDE
# the development Docker image so we reuse its prebuilt dependencies (/vcpkg) instead of compiling them.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(pwd)"
if [[ -z "${BUILD_DIR:-}" ]]; then
    BUILD_DIR="$SCRIPT_DIR/build_dir"
    echo "BUILD_DIR not set; using $BUILD_DIR"
fi
export BUILD_DIR
WORKER_BIN="$BUILD_DIR/nes-single-node-worker/nes-single-node-worker"

# Always build: the image (docker layer cache makes an unchanged rebuild cheap) and then NES inside it
# (cmake --build is incremental against the persisted BUILD_DIR). Gating this on "$WORKER_BIN missing"
# meant a stale binary from an earlier commit was silently reused. SKIP_BUILD=1 opts out.
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    DEV_IMAGE="${NES_DEV_IMAGE:-nebulastream/nes-development:local}"
    # -l builds the dependency + development images locally rather than pulling a published hash, so
    # the image ALWAYS matches the current dependency set. --libstdcxx keeps the in-image build on the
    # same standard library as the native half (USE_LIBCXX_IF_AVAILABLE=OFF below and in the sibling
    # run.sh); --no-sanitizer is what yields the x64-linux-none triplet (there is no "--none" flag).
    INSTALL_ARGS="${NES_INSTALL_ARGS:--y --libstdcxx --no-sanitizer -l}"
    # Rebuild the dev image every time, on purpose. The previous version consulted a dependency-hash
    # sentinel and skipped the install when it matched -- but the hash silently degrades to the literal
    # "unknown" when hash_dependencies.sh fails, and once "unknown" is written to the sentinel it
    # matches forever. A stale :local then fails the in-image configure with a missing dependency
    # (e.g. nlohmann_json), which reads like a source error rather than a stale image.
    # Set NES_SKIP_DEV_IMAGE_BUILD=1 to reuse the existing image when you know it is current.
    if [[ "${NES_SKIP_DEV_IMAGE_BUILD:-0}" == "1" ]]; then
        echo "NES_SKIP_DEV_IMAGE_BUILD=1: reusing existing $DEV_IMAGE without rebuilding"
    else
        echo "Building $DEV_IMAGE via install-local-docker-environment.sh $INSTALL_ARGS ..."
        # shellcheck disable=SC2086
        scripts/install-local-docker-environment.sh $INSTALL_ARGS
    fi

    # The in-image build mounts the repo at its host path, so BUILD_DIR must live inside the repo.
    if [[ "$BUILD_DIR" != "$REPO_ROOT"/* ]]; then
        echo "ERROR: for the in-image build, BUILD_DIR must be inside the repo ($REPO_ROOT); got $BUILD_DIR" >&2
        exit 1
    fi
    echo "Configuring + building nes-cli + nes-single-node-worker inside $DEV_IMAGE ..."
    docker run --rm --entrypoint= -e CCACHE_DIR=/ccache -v ccache:/ccache \
        -v "$REPO_ROOT":"$REPO_ROOT" -w "$REPO_ROOT" "$DEV_IMAGE" /bin/sh -c \
        "cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake \
            -DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF -DENABLE_LARGE_TESTS=0 -DNES_LOG_LEVEL:STRING=DEBUG \
            -S '$REPO_ROOT' -B '$BUILD_DIR' \
         && MOLD_JOBS=1 cmake --build '$BUILD_DIR' --target nes-cli nes-single-node-worker -- -j ${BUILD_JOBS:-$(nproc 2>/dev/null || echo 4)}"
fi
if [[ ! -x "$WORKER_BIN" ]]; then
    echo "ERROR: worker binary missing: $WORKER_BIN (build failed, or SKIP_BUILD set without a build)." >&2
    exit 1
fi

OUTPUT_DIR="${OUTPUT_DIR:-benchmark_run_dsc_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $(realpath "$OUTPUT_DIR")"
export OUTPUT_DIR

echo "BUILD_DIR:          ${BUILD_DIR:-<default ./build_dir>}"
echo "MODES:              ${MODES:-${MODE:-<default traffic,contention>}}"
echo "TOPOLOGIES:         ${TOPOLOGIES:-<default 1/2>}"
echo "VARIANTS:           ${VARIANTS:-<default prometheus,split,delta,local>}"
echo "DATASET:            ${DATASET:-<default cluster_monitoring>}"
echo "SOURCES_PER_LEAF:   ${SOURCES_PER_LEAF:-<default 4>}"
echo "RUN_DURATION_SECONDS: ${RUN_DURATION_SECONDS:-<default 30>}"

# The combined worker+Prometheus image FROMs nes-runtime-base:test (not on Docker Hub); build it.
RUNTIME_BASE_IMAGE="nes-runtime-base:test"
if [[ -f "docker/runtime/RuntimeBase.dockerfile" ]] && ! docker image inspect "$RUNTIME_BASE_IMAGE" >/dev/null 2>&1; then
    echo "Building $RUNTIME_BASE_IMAGE (one-time, layer-cached)..."
    docker build -t "$RUNTIME_BASE_IMAGE" -f docker/runtime/RuntimeBase.dockerfile docker/runtime
fi

IMAGE="${WORKER_DOCKER_IMAGE:-nes-bench-prom-combined:local}"
if [[ -f "docker/single-node-worker/SingleNodeWorkerWithPrometheus.dockerfile" ]]; then
    echo "Building $IMAGE (cached if unchanged)..."
    docker build -t "$IMAGE" -f docker/single-node-worker/SingleNodeWorkerWithPrometheus.dockerfile docker/single-node-worker
fi

TCPGEN_IMAGE="${TCP_GENERATOR_IMAGE:-nes-bench-tcp-gen:local}"
if [[ -f "rust-tcp-generator/Dockerfile" ]]; then
    echo "Building $TCPGEN_IMAGE (cached if unchanged)..."
    docker build -t "$TCPGEN_IMAGE" rust-tcp-generator
fi

# stdlib-only runner; venv is for isolation and is torn down on exit.
python3 -m venv myenv
source myenv/bin/activate
trap 'deactivate 2>/dev/null || true; rm -rf myenv' EXIT

myenv/bin/python3 -m scripts.benchmarking.histogram_delta.distributed.run_benchmark 2>&1 \
    | tee "$OUTPUT_DIR/run.log"
