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

# Wraps create_compose.sh and injects a Prometheus server service that
# scrapes the worker-2 sink endpoint. The Prometheus container reads its
# scrape config from /workdir/tests/good/prometheus.yml (mounted via the
# shared test volume).

set -eo pipefail

if [ $# -ne 1 ]; then
  echo "Error: Exactly one argument required"
  echo "Usage: $0 <topology-file>"
  exit 1
fi

if [ -z "${TEST_VOLUME:-}" ]; then
  echo "ERROR: TEST_VOLUME is not set" >&2
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROMETHEUS_IMAGE=${PROMETHEUS_IMAGE:-prom/prometheus:v2.53.5}

PROMETHEUS_BLOCK=$(cat <<EOF
  prometheus:
    image: ${PROMETHEUS_IMAGE}
    pull_policy: missing
    command:
      - --config.file=/workdir/tests/good/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --web.listen-address=0.0.0.0:9090
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:9090/-/ready"]
      interval: 1s
      timeout: 5s
      retries: 30
      start_period: 30s
    volumes:
      - ${TEST_VOLUME}:/workdir
EOF
)

# Insert PROMETHEUS_BLOCK right before the top-level 'networks:' key so it
# becomes the last entry under 'services:'.
"$SCRIPT_DIR/create_compose.sh" "$1" \
  | awk -v block="$PROMETHEUS_BLOCK" '
      /^networks:/ && !inserted { print block; inserted=1 }
      { print }
    '
