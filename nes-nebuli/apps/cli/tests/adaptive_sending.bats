#!/usr/bin/env bats

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

setup_file() {

  # Validate environment variables
  if [ -z "$NES_CLI" ]; then
    echo "ERROR: NES_CLI environment variable must be set" >&2
    echo "Usage: NES_CLI=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    echo "Usage: NEBULASTREAM=/path/to/nes-single-node-worker bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NES_CLI_TESTDATA" ]; then
    echo "ERROR: NES_CLI_TESTDATA environment variable must be set" >&2
    echo "Usage: NES_CLI_TESTDATA=/path/to/cli/testdata" >&2
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

  # Print environment info for debugging
  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3

  # Build Docker images
  docker build -t worker-image -f - $(dirname $(realpath $NEBULASTREAM)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg iproute2 -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-\$(dpkg --print-architecture) && \
    chmod +x /bin/grpc_health_probe

    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker", "enable_backpressure_statistics_tcp_emission=true"]
EOF
  docker build -t nes-cli-image -f - $(dirname $(realpath $NES_CLI)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg iproute2 -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    COPY nes-cli /usr/bin
EOF
}

setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3

  "$TMP_DIR/tests/util/check_tool_installs.sh"
  status=$?

  if [ "$status" -ne 0 ]; then
    echo "Dependency check failed, aborting."
    exit "$status"
  fi
}

teardown() {
  # Close any tmux panes we spawned for this test
  if [ -n "${TMUX:-}" ] && command -v tmux >/dev/null 2>&1; then
    if [ "${#TMUX_PANES[@]}" -gt 0 ]; then
      for pane_id in "${TMUX_PANES[@]}"; do
        tmux kill-pane -t "$pane_id" 2>/dev/null || true
      done
    fi
  fi

  docker compose down -v || true
}

function setup_distributed() {
  echo "# $PWD" >&3
  tests/util/create_compose.sh "$1" > docker-compose.yaml

  # Add NET_ADMIN capability to all worker containers for network throttling
  # This modifies the docker-compose.yaml to add cap_add: NET_ADMIN to each worker service
  local temp_file=$(mktemp)
  awk '
    /^  worker-[0-9]+:/ {
      in_worker = 1
      print
      next
    }
    in_worker && /^    image:/ {
      print
      print "    cap_add:"
      print "      - NET_ADMIN"
      in_worker = 0
      next
    }
    /^  [a-z]/ && !/^  worker-[0-9]+:/ {
      in_worker = 0
    }
    { print }
  ' docker-compose.yaml > "$temp_file"
  mv "$temp_file" docker-compose.yaml

  docker compose up -d --wait
}

function throttle_network() {
  local container=$1
  local rate=$2 # e.g., 10kbit
  docker compose exec -t "$container" tc qdisc add dev eth0 root tbf rate "$rate" burst 32kbit latency 400ms
}

function reset_network() {
  local container=$1
  docker compose exec -t "$container" tc qdisc del dev eth0 root || true
}

DOCKER_NES_CLI() {
  COMPOSE_PROGRESS=quiet docker compose run --rm nes-cli nes-cli "$@"
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

extract_last_line() {
  # Use the last line of stdout as the actual CLI result and strip CR if present.
  printf '%s\n' "$1" | tail -n 1 | tr -d '\r'
}

@test "statistics query" {
  setup_distributed tests/good/statistics_query.yaml

  # Set SHOW_OUTPUT to 0 by default if not set
  : "${SHOW_OUTPUT:=0}"

    # Only open terminals if SHOW_OUTPUT is set to 1
    if [ "$SHOW_OUTPUT" -eq 1 ]; then
        open_viewer() {
          # Usage: open_viewer <command...>
          if [ -n "${TMUX:-}" ] && command -v tmux >/dev/null 2>&1; then
            # Split the current tmux pane and run the command in the new pane.
            # -d: don't switch focus, -P: print pane id (useful for debugging), -F: format
            tmux split-window -d -v -P -F "#{pane_id}" "$@"
          else
            # Fallback: spawn a new gnome-terminal window
            gnome-terminal -- "$@" &
          fi
        }

        # open terminal/pane and print live output
        open_viewer python3 "$TMP_DIR"/tests/util/print_output.py "$TMP_DIR"/worker-1/out_stat.csv
        open_viewer python3 "$TMP_DIR"/tests/util/print_output.py "$TMP_DIR"/worker-2/out.csv --freq
        open_viewer python3 "$TMP_DIR"/tests/util/print_output.py "$TMP_DIR"/worker-2/out.csv --plot

        # open terminal/pane and print live logs
        open_viewer tail -F "$TMP_DIR"/worker-1/singleNodeWorker.log
        open_viewer tail -F "$TMP_DIR"/worker-2/singleNodeWorker.log
    fi

  # Apply heavy throttling to the network connection to trigger backpressure
  echo "# Throttling worker-2 to 10kbit" >&3
  throttle_network worker-2 "10kbit"
  echo "# Throttling worker-1 to 10kbit" >&3
  throttle_network worker-1 "10kbit"

  run DOCKER_NES_CLI -t tests/good/statistics_query.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO FILE_SINK'

  [ "$status" -eq 0 ]
  result="$(extract_last_line "$output")"
  [ -n "$result" ]
  [ -f "$result" ]
  QUERY_ID=$result

  echo "started first query successfully" >&3
  run DOCKER_NES_CLI -t tests/good/statistics_query.yaml start 'select * from TCP_STAT_SOURCE INTO STAT_SINK'
  echo "started statistics query" >&3

  [ "$status" -eq 0 ]
  result="$(extract_last_line "$output")"
  [ -n "$result" ]
  [ -f "$result" ]
  QUERY_ID=$result
  echo "starting of statistics query succeeded" >&3

  echo "sleeping for 20 seconds" >&3
  sleep 20
  echo "wake up" >&3

  # Check that the output of the statistic query contains the expected events at least 4 times
  apply_backpressure_count=$(pcregrep -Mc "^1,[0-9A-Za-z]{8}(?:-[0-9A-Za-z]{4}){3}-[0-9A-Za-z]{12},[0-9]+\n0,[0-9A-Za-z]{8}(?:-[0-9A-Za-z]{4}){3}-[0-9A-Za-z]{12},[0-9]+" "$TMP_DIR"/worker-1/out_stat.csv || true)
  [ "$apply_backpressure_count" -ge 2 ]
}

# bats test_tags=bats:focus
@test "adaptive sending" {
  setup_distributed tests/good/adaptive.yaml

  # Set SHOW_OUTPUT to 0 by default if not set
  : "${SHOW_OUTPUT:=0}"

    # Only open terminals if SHOW_OUTPUT is set to 1
    if [ "$SHOW_OUTPUT" -eq 1 ]; then
        open_viewer() {
          # Usage: open_viewer <command...>
          if [ -n "${TMUX:-}" ] && command -v tmux >/dev/null 2>&1; then
            # Split the current tmux pane and run the command in the new pane.
            # -d: don't switch focus, -P: print pane id (useful for debugging), -F: format
            tmux split-window -d -v -P -F "#{pane_id}" "$@"
          else
            # Fallback: spawn a new gnome-terminal window
            gnome-terminal -- "$@" &
          fi
        }

        # open terminal/pane and print live output
        #open_viewer python3 "$TMP_DIR"/tests/util/print_output.py "$TMP_DIR"/worker-2/out2.csv
        open_viewer python3 "$TMP_DIR"/tests/util/print_output.py "$TMP_DIR"/worker-2/out2.csv --plot
        #open_viewer python3 "$TMP_DIR"/tests/util/print_output.py "$TMP_DIR"/worker-2/out.csv --freq
        open_viewer python3 "$TMP_DIR"/tests/util/print_output.py "$TMP_DIR"/worker-2/out.csv --plot

        # open terminal/pane and print live logs
        #open_viewer tail -F "$TMP_DIR"/worker-1/singleNodeWorker.log
        #open_viewer tail -F "$TMP_DIR"/worker-2/singleNodeWorker.log
    fi

  # Apply heavy throttling to the network connection to trigger backpressure
  echo "# Throttling worker-2 to 10kbit" >&3
  throttle_network worker-2 "10kbit"
  echo "# Throttling worker-1 to 10kbit" >&3
  throttle_network worker-1 "10kbit"

  #run DOCKER_NES_CLI -t tests/good/adaptive.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO FILE_SINK'

  #[ "$status" -eq 0 ]
  #result="$(extract_last_line "$output")"
  #[ -n "$result" ]
  #[ -f "$result" ]
  #QUERY_ID=$result

  #echo "started first query successfully" >&3
  run DOCKER_NES_CLI -t tests/good/adaptive.yaml start 'select * from GENERATOR_SOURCE2 INTO FILE_SINK2'
  echo "started second query" >&3

  [ "$status" -eq 0 ]
  result="$(extract_last_line "$output")"
  [ -n "$result" ]
  [ -f "$result" ]
  QUERY_ID=$result
  echo "starting of second query succeeded" >&3

  echo "sleeping for 100 seconds" >&3
  sleep 100
  echo "wake up" >&3
}
