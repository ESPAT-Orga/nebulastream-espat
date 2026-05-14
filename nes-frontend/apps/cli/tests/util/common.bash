# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Shared bats helpers for nes-cli docker-compose tests. Source this from a
# bats file's top-level (before any setup_file/setup definitions) and pass the
# test-network label as the first argument to nes_cli_setup_file so leaked
# resources from previous runs can be reaped per-suite.

nes_cli_setup_file() {
  local network_label=$1

  # Clean up leaked containers and networks from previous crashed runs.
  for net in $(docker network ls --filter label=nes-test=${network_label} -q 2>/dev/null); do
    docker network inspect "$net" -f '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker network prune -f --filter label=nes-test=${network_label} 2>/dev/null || true
  # Remove containers referencing test images from previous runs so the
  # images themselves can be deleted afterwards.
  for img in $(docker images --filter reference='nes-worker-cli-test-*' --filter reference='nes-cli-image-*' -q 2>/dev/null); do
    docker ps -aq --filter ancestor="$img" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker images --filter reference='nes-worker-cli-test-*' --filter reference='nes-cli-image-*' -q | xargs -r docker image rm -f 2>/dev/null || true

  # Validate environment variables
  if [ -z "$NES_CLI" ]; then
    echo "ERROR: NES_CLI environment variable must be set" >&2
    exit 1
  fi
  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    exit 1
  fi
  if [ -z "$NES_CLI_TESTDATA" ]; then
    echo "ERROR: NES_CLI_TESTDATA environment variable must be set" >&2
    exit 1
  fi
  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    exit 1
  fi
  if [ ! -f "$NES_CLI" ]; then
    echo "ERROR: NES_CLI file does not exist: $NES_CLI" >&2
    exit 1
  fi
  if [ ! -f "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file does not exist: $NEBULASTREAM" >&2
    exit 1
  fi
  if [ ! -x "$NES_CLI" ]; then
    echo "ERROR: NES_CLI file is not executable: $NES_CLI" >&2
    exit 1
  fi
  if [ ! -x "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file is not executable: $NEBULASTREAM" >&2
    exit 1
  fi
  if [ -z "$NES_RUNTIME_BASE_IMAGE" ]; then
    echo "ERROR: NES_RUNTIME_BASE_IMAGE environment variable must be set" >&2
    exit 1
  fi

  # Build Docker images with unique tags to avoid collisions when test suites run in parallel
  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  export WORKER_IMAGE="nes-worker-cli-test-${suffix}"
  local worker_ctx=$(mktemp -d)
  cp $(realpath $NEBULASTREAM) "$worker_ctx/nes-single-node-worker"
  docker build --load -t $WORKER_IMAGE -f - "$worker_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
  rm -rf "$worker_ctx"
  export CLI_IMAGE="nes-cli-image-${suffix}"
  local cli_ctx=$(mktemp -d)
  cp $(realpath $NES_CLI) "$cli_ctx/nes-cli"
  docker build --load -t $CLI_IMAGE -f - "$cli_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-cli /usr/bin
EOF
  rm -rf "$cli_ctx"

  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using CLI_IMAGE: $CLI_IMAGE" >&3
}

nes_cli_teardown_file() {
  echo "# Test suite completed" >&3
  docker rmi $WORKER_IMAGE || true
  docker rmi $CLI_IMAGE || true
}

nes_cli_setup() {
  # Create temp directory within the mounted workspace (not /tmp)
  # so it's accessible from docker-compose containers running on the host
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit
  echo "# Using TEST_DIR: $TMP_DIR" >&3

  volume=$(docker volume create)
  volume_host_container=$(docker run -d --rm -v $volume:/data alpine sleep infinite)
  docker cp . $volume_host_container:/data
  docker stop -t0 $volume_host_container
  export TEST_VOLUME=$volume
  echo "# Using test volume: $TEST_VOLUME" >&3
}

sync_workdir() {
  volume_host_container=$(docker run -d --rm -v $TEST_VOLUME:/data alpine sleep infinite)
  docker cp $volume_host_container:/data/. .
  docker stop -t0 $volume_host_container
}

nes_cli_teardown() {
  sync_workdir || true
  docker compose down -v || true
  docker volume rm $TEST_VOLUME || true
}

# Bring up a topology via the standard create_compose.sh.
setup_distributed() {
  tests/util/create_compose.sh "$1" > docker-compose.yaml
  local compose_output exit_code=0
  compose_output=$(docker compose up -d --wait 2>&1) || exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "# [docker compose up] (status=$exit_code):" >&3
    while IFS= read -r line; do echo "#   $line" >&3; done <<< "$compose_output"
  fi
  return $exit_code
}

DOCKER_NES_CLI() {
  # docker compose exec v2 disconnects the session when its stdin reaches EOF
  # (docker/compose#10418). In bats subshells stdin is closed, so we pipe from
  # tail to keep the connection alive.
  tail -f /dev/null | docker compose exec -T nes-cli nes-cli "$@"
}

assert_json_equal() {
  local expected="$1"
  local actual="$2"

  diff <(echo "$expected" | jq --sort-keys .) \
    <(echo "$actual" | jq --sort-keys .)
}

assert_json_contains() {
  local expected="$1"
  local actual="$2"

  local result=$(echo "$actual" | jq --argjson exp "$expected" 'contains($exp)')

  if [ "$result" != "true" ]; then
    echo "JSON subset check failed"
    echo "Expected (subset): $expected"
    echo "Actual: $actual"
    return 1
  fi
}
