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

# Idempotent Prometheus binary resolver. Used by the over-time benchmark
# (scripts/benchmarking/multiple-statistic-queries-over-time-prometheus/).
# Echoes the absolute path to the resolved prometheus binary on stdout.

set -eo pipefail

PROMETHEUS_VERSION=${PROMETHEUS_VERSION:-2.53.5}
PROMETHEUS_DIR=${PROMETHEUS_DIR:-$HOME/.cache/nebulastream/prometheus-${PROMETHEUS_VERSION}}

# 1. system PATH
if command -v prometheus >/dev/null 2>&1; then
    command -v prometheus
    exit 0
fi

# 2. cache
if [ -x "${PROMETHEUS_DIR}/prometheus" ]; then
    echo "${PROMETHEUS_DIR}/prometheus"
    exit 0
fi

# 3. download
arch=$(uname -m)
case "$arch" in
    x86_64|amd64) PROM_ARCH=amd64 ;;
    aarch64|arm64) PROM_ARCH=arm64 ;;
    *) echo "unsupported architecture: $arch" >&2; exit 1 ;;
esac

TARBALL="prometheus-${PROMETHEUS_VERSION}.linux-${PROM_ARCH}.tar.gz"
URL="https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/${TARBALL}"

mkdir -p "${PROMETHEUS_DIR}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading ${URL}" >&2
curl -fsSL "${URL}" -o "${tmp}/${TARBALL}"
tar -xzf "${tmp}/${TARBALL}" -C "${tmp}"
cp "${tmp}/prometheus-${PROMETHEUS_VERSION}.linux-${PROM_ARCH}/prometheus" "${PROMETHEUS_DIR}/prometheus"
chmod +x "${PROMETHEUS_DIR}/prometheus"

echo "${PROMETHEUS_DIR}/prometheus"
