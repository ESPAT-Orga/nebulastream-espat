#!/usr/bin/env bash
# Demonstrates the ported StatisticInterface observing the query engine's own task queue.
#
#   TaskStatisticListener -> InProcessFeed -> InProcessSource -> WindowedAggregation(ScalarStatistic Avg)
#                         -> StatisticStoreWriter -> GrpcSink -> StatisticInterface
#
# This is the in-process counterpart of run-engine-stats-demo.sh: where that one drives the REPL with SQL, this
# drives the statistic interface through its C++ API, which is the only interface this port provides.
#
# The demonstration is the statistic-task-queue-test binary, so that it stays runnable in CI rather than being a
# script that only a human can judge.
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build-port}"
JOBS="${JOBS:-24}"

usage() {
    cat <<USAGE
Usage: $0 [--build-dir <dir>] [--jobs <n>] [--no-build]

  --build-dir  CMake build directory (default: ${BUILD_DIR})
  --jobs       Build parallelism (default: ${JOBS}); kept well below nproc on purpose, a
               full-width build of this tree can exhaust memory.
  --no-build   Run the existing binary without rebuilding.
USAGE
}

DO_BUILD=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir) BUILD_DIR="$2"; shift 2 ;;
        --jobs)      JOBS="$2";      shift 2 ;;
        --no-build)  DO_BUILD=0;     shift ;;
        -h|--help)   usage; exit 0 ;;
        *)           echo "unknown argument: $1" >&2; usage; exit 2 ;;
    esac
done

BINARY="${BUILD_DIR}/nes-statistics/tests/statistic-task-queue-test"

if [[ "${DO_BUILD}" -eq 1 ]]; then
    echo "==> Building statistic-task-queue-test (-j ${JOBS})"
    # A memory-capped scope, so an overshoot kills the build rather than the session.
    if command -v systemd-run >/dev/null 2>&1; then
        systemd-run --user --scope -q -p MemoryMax=48G -p MemorySwapMax=0 \
            cmake --build "${BUILD_DIR}" --target statistic-task-queue-test -j "${JOBS}"
    else
        cmake --build "${BUILD_DIR}" --target statistic-task-queue-test -j "${JOBS}"
    fi
fi

if [[ ! -x "${BINARY}" ]]; then
    echo "error: ${BINARY} not found; build it first or drop --no-build" >&2
    exit 1
fi

echo "==> Running the demonstration"
echo "    A Generator query supplies load; the engine's own task events are aggregated"
echo "    into a statistic whose every closed window is reported to the statisticInterface."
"${BINARY}" --gtest_color=yes
