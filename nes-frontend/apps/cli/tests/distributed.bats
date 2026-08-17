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

source "${BATS_TEST_DIRNAME}/util/common.bash"

setup_file() {
  nes_cli_setup_file "distributed-cli"
}

teardown_file() {
  nes_cli_teardown_file
}

setup() {
  nes_cli_setup
}

teardown() {
  nes_cli_teardown
}

@test "launch query from topology" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
}

@test "launch multiple query from topology" {
  setup_distributed tests/good/multiple-select-gen-into-void.yaml

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  query_ids=("${lines[@]}")

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[0]}"
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[1]}" "${query_ids[2]}" "${query_ids[3]}" "${query_ids[4]}" "${query_ids[5]}"
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[6]}" "${query_ids[7]}"
  [ "$status" -eq 0 ]
}

@test "launch query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
}

@test "launch bad query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'selectaaa DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]
}

@test "launch and stop query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries" {
  setup_distributed tests/good/distributed-query-deployment.yaml

  run DOCKER_NES_CLI -t tests/good/distributed-query-deployment.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  for i in $(seq 1 20); do
    sleep 1
    run DOCKER_NES_CLI -t tests/good/distributed-query-deployment.yaml status "$QUERY_ID"
    [ "$status" -eq 0 ]
    QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    if [ "$QUERY_STATUS" = "Running" ]; then
      break
    fi
  done
  echo "${output}" | jq -e '(. | length) == 3' # 1 global + 2 local
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries crazy join" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI status "$QUERY_ID"
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor distributed queries crazy join with a fast source" {
  setup_distributed tests/good/chained-joins-one-fast-source.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  # Poll until the fast source has stopped and the query becomes PartiallyStopped
  for i in $(seq 1 20); do
    sleep 1
    run DOCKER_NES_CLI status "$QUERY_ID"
    [ "$status" -eq 0 ]
    QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    if [ "$QUERY_STATUS" = "PartiallyStopped" ]; then
      break
    fi
    # If the query already fully stopped, it won't go back to PartiallyStopped
    if [ "$QUERY_STATUS" = "Stopped" ]; then
      break
    fi
  done
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  [ "$QUERY_STATUS" = "PartiallyStopped" ]

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "test worker not available" {
  setup_distributed tests/good/chained-joins.yaml

  docker compose stop worker-1

  run DOCKER_NES_CLI -d start

  sync_workdir
  grep "(5001) : query registration call failed; Status: UNAVAILABLE" nes-cli.log
  [ "$status" -eq 1 ]

  docker compose up -d --wait worker-1
  # now it should work
  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
}

@test "worker goes offline during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  # This has to be kill not stop. Stop will gracefully shutdown the worker and all queries on that worker.
  # This would cause the query to fail as it was unexpectedly stopped. If we kill the worker: upstream and downstream
  # will wait for the "crashed" worker to return. However this test does not test that as it is currently not possible.
  docker compose kill worker-1
  run DOCKER_NES_CLI status "$QUERY_ID"
  [ "$status" -eq 0 ]

  EXPECTED_STATUS_OUTPUT=$(cat <<EOF
[
  {
    "query_id": "$QUERY_ID",
    "query_status": "Unreachable"
  },
  {
    "worker": "worker-2:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-3:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-8:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-7:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-4:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-9:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-5:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-6:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-1:8080",
    "query_status": "ConnectionError"
  }
]
EOF
)

  assert_json_contains "${EXPECTED_STATUS_OUTPUT}" "${output}"
}

@test "worker goes offline and comes back during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  # Simulate a crash by killing worker-1.
  docker compose kill worker-1
  run DOCKER_NES_CLI status "$QUERY_ID"
  [ "$status" -eq 0 ]

  sleep 1

  docker compose up -d --wait worker-1

# While this might not be the most intuitive nor the long-term solution this testcase documents the current behavior.
# The query running on worker-1 is terminated and on restart it is not restarted, this will cause subsequent status
# request to find that the previous local query id is not registered on worker-1, currently this is falsely reported as a ConnectionError.

  run DOCKER_NES_CLI status "$QUERY_ID"
  [ "$status" -eq 0 ]
  EXPECTED_STATUS_OUTPUT=$(cat <<EOF
[
  {
    "query_id": "$QUERY_ID",
    "query_status": "Unreachable"
  },
  {
    "worker": "worker-1:8080",
    "query_status": "ConnectionError"
  }
]
EOF
)

  assert_json_contains "${EXPECTED_STATUS_OUTPUT}" "${output}"

  echo $output
}

@test "worker status" {
  setup_distributed tests/good/select-gen-into-void.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  sleep 1

  run DOCKER_NES_CLI status $query_id
  [ $status -eq 0 ]
  assert_json_contains "[{\"query_id\":\"$query_id\", \"query_status\":\"Running\", \"running\": {}, \"started\": {}}]" "$output"

  local_query_id=$(echo "$output" | jq -r '.[1].local_query_id')
  run DOCKER_NES_CLI status
  [ $status -eq 0 ]

  # Expect to find the local query in the worker status
  assert_json_contains "[{\"local_query_id\":\"$local_query_id\", \"query_status\":\"Running\", \"started\": {}}]" "$output"
}

@test "back pressure using worker config" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run DOCKER_NES_CLI stop $query_id
  # 0 means there is no overwrite and the worker default will be picked.
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "max_pending_acks: 0" worker-2/singleNodeWorker.log
  grep "sender_queue_size: 0" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "back pressure using optimizer flags" {
  setup_distributed tests/good/backpressure-optimizer-flags.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run DOCKER_NES_CLI stop $query_id
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "max_pending_acks: 25" worker-2/singleNodeWorker.log
  grep "sender_queue_size: 32" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "order of worker termination when backpressure is applied. terminate sink" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  docker compose stop worker-1

  # Poll until the sink closure propagates
  for i in $(seq 1 20); do
    sleep 1
    sync_workdir
    if grep -q "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  grep "Backpressure" worker-2/singleNodeWorker.log
  grep "NetworkSink was closed by other side" worker-2/singleNodeWorker.log
  grep "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log

  run DOCKER_NES_CLI status $query_id
  [ $status -eq 0 ]

  expected_json=$(cat <<EOF
  [
    {
      "query_status": "Failed"
    },
    {
      "query_status": "ConnectionError",
      "worker": "worker-1:8080"
    },
    {
      "query_status": "Failed",
      "worker": "worker-2:8080"
    }
  ]
EOF
  )

  assert_json_contains "$expected_json" "$output"
}

@test "order of worker termination when backpressure is applied. terminate source" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done
  grep "Backpressure" worker-2/singleNodeWorker.log

  docker compose stop worker-2
  sleep 2

  run DOCKER_NES_CLI status $query_id
  [ $status -eq 0 ]

  expected_json=$(cat <<EOF
  [
    {
      "query_status": "Unreachable"
    },
    {
      "query_status": "Running",
      "worker": "worker-1:8080"
    },
    {
      "query_status": "ConnectionError",
      "worker": "worker-2:8080"
    }
  ]
EOF
  )

  assert_json_contains "$expected_json" "$output"
}

@test "launch query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start'"
  [ "$status" -eq 0 ]
}

# --- histogram delta compression (GEN/RESOLVER wire) -------------------------------
# See docs/histogram-delta-wire-compression-plan.md. The single-node systests
# (nes-systests/operator/aggregation/statistics/WindowAggregationHistogramDelta*.test)
# co-locate both halves of the split, so nothing there exercises the case the feature
# exists for: the delta blob travelling over a real network channel between two
# workers. The tests below do, on the topology in tests/good/histogram-delta-2-nodes.yaml.
#
# They drive the feature the way production does: a REQUEST STATISTIC with
# `optimizer.enable_histogram_delta_compression` set, so the node cut comes from the
# PlacementHintTrait pins that DefaultStatisticQueryGenerator stamps. Nothing else covers
# those pins end to end. Because the build's terminal sink is a Void sink (nes-cli runs no
# StatisticCoordinator), the reconstructed histogram is read back the way the REPL test does
# it: a second query that probes the node-local store, one row per window key.
PROBE_SQL='SELECT statisticStart, statisticEnd, binStart, binCounter, binEnd FROM ( SELECT EQUIWIDTHHISTOGRAM_PROBE(71, uint64, uint64) FROM keys ) INTO bins'

# Returns the sub-listing of a `nes-cli dump` "Decomposed Plans:" section that belongs to
# one worker. The sections come out in unspecified order (they are iterated from a map),
# so they have to be located by their "<n> plans on <host>:" header rather than by index.
plans_on_worker() {
  local worker=$1 dump=$2
  echo "$dump" | awk -v marker="plans on ${worker}:" '
    index($0, marker) { capture = 1; next }
    /plans on .*:$/   { capture = 0 }
    capture           { print }
  '
}

# Polls a query's global status until it reports one of the terminal states. The build's file
# source ends on its own, and that end-of-stream is what flushes the last window through the
# network channel and into the store -- so this is what says "the store is now populated" and
# the probe may be submitted.
wait_for_query_to_finish() {
  local query_id=$1
  for _ in $(seq 1 40); do
    sleep 1
    local state
    state=$(DOCKER_NES_CLI status "$query_id" \
      | jq -r --arg query_id "$query_id" \
          '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    case "$state" in
      Stopped | PartiallyStopped) return 0 ;;
      Failed) echo "query $query_id failed"; return 1 ;;
    esac
  done
  echo "timed out waiting for query $query_id to finish (last state: ${state:-unknown})"
  return 1
}

# Polls for a sink output file inside the shared volume to reach `expected` lines.
# sync_workdir has to run every iteration: the volume is copied, not bind-mounted, so nothing
# written by a worker is visible before it does.
wait_for_sink_lines() {
  local file=$1 expected=$2
  for _ in $(seq 1 40); do
    sleep 1
    sync_workdir
    if [ "$(cat "$file" 2>/dev/null | wc -l)" -ge "$expected" ]; then
      return 0
    fi
  done
  echo "timed out waiting for $expected lines in $file; got:"
  cat "$file" 2>/dev/null || echo "(file does not exist)"
  return 1
}

# The three reconstructed windows, 5 bins each: statisticStart, statisticEnd, binStart,
# binCounter, binEnd. Identical to the golden block in the single-node systest — the
# reconstruction has to survive the wire unchanged.
EXPECTED_HISTOGRAM_BINS=$(cat <<'EOF'
0,5000,0,1,5
0,5000,5,1,10
0,5000,10,1,15
0,5000,15,1,20
0,5000,20,1,25
5000,10000,0,2,5
5000,10000,5,1,10
5000,10000,10,1,15
5000,10000,15,1,20
5000,10000,20,1,25
10000,15000,0,2,5
10000,15000,5,2,10
10000,15000,10,1,15
10000,15000,15,1,20
10000,15000,20,1,25
EOF
)

# Compares a probe sink's CSV against EXPECTED_HISTOGRAM_BINS. Drops the sink's schema
# header line and sorts both sides, so the assertion is on the set of reconstructed bins
# rather than on the order the windows happen to be emitted in.
assert_reconstructed_bins() {
  local file=$1
  diff <(echo "$EXPECTED_HISTOGRAM_BINS" | sort) <(tail -n +2 "$file" | sort)
}

@test "histogram delta split cuts the wire between GEN and RESOLVER" {
  setup_distributed tests/good/histogram-delta-2-nodes.yaml

  run DOCKER_NES_CLI dump
  [ "$status" -eq 0 ]

  gen_plan=$(plans_on_worker "gen-node:8080" "$output")
  resolver_plan=$(plans_on_worker "resolver-node:8080" "$output")

  # PlacementHintTrait{Source} keeps the GEN half on the source node, and nothing above it:
  # the only thing handed to the network sink is the GEN's sparse per-window delta blob.
  echo "$gen_plan" | grep -q "STAT BUILD(EquiWidthHistogramDeltaGen)"
  ! echo "$gen_plan" | grep -q "EquiWidthHistogramDeltaResolver"
  ! echo "$gen_plan" | grep -q "STATISTIC_STORE_WRITER"

  # PlacementHintTrait{Sink} puts the RESOLVER and the writer on the store-owner node named by
  # the request's `host` option, on the far side of the channel.
  echo "$resolver_plan" | grep -q "STAT BUILD(EquiWidthHistogramDeltaResolver)"
  echo "$resolver_plan" | grep -q "STATISTIC_STORE_WRITER"
  ! echo "$resolver_plan" | grep -q "EquiWidthHistogramDeltaGen"
}

@test "histogram delta split reconstructs the histogram across two nodes" {
  setup_distributed tests/good/histogram-delta-2-nodes.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  [[ "$output" =~ ^[a-z_]+$ ]]
  build_id=$output

  # The split must actually be deployed on both workers: 1 global + 2 local queries.
  run DOCKER_NES_CLI status "$build_id"
  [ "$status" -eq 0 ]
  echo "$output" | jq -e '(. | length) == 3'

  wait_for_query_to_finish "$build_id"

  # Read the reconstructed histogram back out of resolver-node's store.
  run DOCKER_NES_CLI start "$PROBE_SQL"
  [ "$status" -eq 0 ]

  # 3 windows x 5 bins + the file sink's schema header line.
  wait_for_sink_lines resolver-node/probe-out.csv 16
  assert_reconstructed_bins resolver-node/probe-out.csv
}

# Control: the same REQUEST STATISTIC with the flag off builds one plain histogram and ships the
# full synopsis over the same wire to the same store node (see the topology's header comment).
# The probed bins must be identical to the delta run's.
@test "plain histogram build over the same wire yields the same bins" {
  setup_distributed tests/good/histogram-delta-2-nodes-plain.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  build_id=$output

  wait_for_query_to_finish "$build_id"

  run DOCKER_NES_CLI start "$PROBE_SQL"
  [ "$status" -eq 0 ]

  wait_for_sink_lines resolver-node/probe-out.csv 16
  assert_reconstructed_bins resolver-node/probe-out.csv
}

@test "launch query using 3-nodes topology" {
  setup_distributed tests/good/3-nodes.yaml
  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
}

@test "placement fails with reversed downstream edges" {
  setup_distributed tests/bad/3-nodes-reversed-edges.yaml
  run DOCKER_NES_CLI start
  [ "$status" -eq 1 ]

  sync_workdir
  grep "topology is not connected" nes-cli.log
}

@test "launch and stop query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  # Output should be a query ID (numeric)
  QUERY_ID=$output

  sleep 1

  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - stop $QUERY_ID'"
  [ "$status" -eq 0 ]
}

@test "query status with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  # Output should be a query ID (numeric)
  QUERY_ID=$output

  sleep 1

  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - status $QUERY_ID'"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r '.[0].query_status')
  [ "$QUERY_STATUS" = "Running" ]
}
