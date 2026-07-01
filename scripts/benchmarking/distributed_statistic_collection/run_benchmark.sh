#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
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
#       bash scripts/benchmarking/distributed_statistic_collection/run_benchmark.sh

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

if [[ ! -x "$WORKER_BIN" && "${SKIP_BUILD:-0}" != "1" ]]; then
    DEV_IMAGE="${NES_DEV_IMAGE:-nebulastream/nes-development:local}"
    INSTALL_ARGS="${NES_INSTALL_ARGS:--y --libstdcxx --no-sanitizer}"
    # Obtain the dev image (toolchain + prebuilt deps) instead of compiling deps, and make sure it
    # matches the CURRENT dependency hash — a present-but-stale :local (older dep set) would fail the
    # in-image build (e.g. missing nlohmann_json). We track the hash the dev image was built for in a
    # sentinel; on a mismatch (or missing image) we re-run the installer: it downloads the published
    # dependency image by hash, or, if that hash isn't published, falls back to -l (builds locally,
    # still pulling prebuilt vcpkg binaries from the public cache). Override std lib / sanitizer via
    # NES_INSTALL_ARGS; set NES_SKIP_DEV_IMAGE_CHECK=1 to trust an existing :local as-is.
    DEP_HASH="$(docker/dependency/hash_dependencies.sh 2>/dev/null || echo unknown)"
    SENTINEL="${XDG_CACHE_HOME:-$HOME/.cache}/nes-dsc/dev_image_hash"
    need_install=0
    docker image inspect "$DEV_IMAGE" >/dev/null 2>&1 || need_install=1
    { [[ -f "$SENTINEL" ]] && [[ "$(cat "$SENTINEL")" == "$DEP_HASH" ]]; } || need_install=1
    if [[ "${NES_SKIP_DEV_IMAGE_CHECK:-0}" == "1" ]]; then need_install=0; fi
    if [[ "$need_install" == 1 ]]; then
        echo "Obtaining $DEV_IMAGE for dependency hash $DEP_HASH via install-local-docker-environment.sh ..."
        # shellcheck disable=SC2086
        scripts/install-local-docker-environment.sh $INSTALL_ARGS \
            || { echo "Dependency hash not downloadable; building images locally (-l)..."; \
                 scripts/install-local-docker-environment.sh $INSTALL_ARGS -l; }
        mkdir -p "$(dirname "$SENTINEL")"
        echo "$DEP_HASH" > "$SENTINEL"
    else
        echo "Dev image $DEV_IMAGE already matches dependency hash $DEP_HASH"
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
echo "VARIANTS:           ${VARIANTS:-<default prometheus,split,local>}"
echo "SOURCES_PER_LEAF:   ${SOURCES_PER_LEAF:-<default 4>}"
echo "RUN_DURATION_SECONDS: ${RUN_DURATION_SECONDS:-<default 60>}"

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

myenv/bin/python3 -m scripts.benchmarking.distributed_statistic_collection.run_benchmark 2>&1 \
    | tee "$OUTPUT_DIR/run.log"
