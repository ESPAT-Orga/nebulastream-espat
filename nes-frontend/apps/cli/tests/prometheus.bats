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
  nes_cli_setup_file "prometheus-cli"
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

# Brings up the topology plus a Prometheus server scraping the sink.
setup_distributed_prometheus() {
  tests/util/create_compose_prometheus.sh "$1" > docker-compose.yaml
  local compose_output exit_code=0
  compose_output=$(docker compose up -d --wait 2>&1) || exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "# [docker compose up] (status=$exit_code):" >&3
    while IFS= read -r line; do echo "#   $line" >&3; done <<< "$compose_output"
  fi
  return $exit_code
}

# Runs wget inside the nes-cli container against prometheus and echoes the
# response body. `tail -f /dev/null |` keeps docker compose exec from
# disconnecting when bats' stdin is closed (docker/compose#10418).
prometheus_api() {
  local path=$1
  tail -f /dev/null | docker compose exec -T nes-cli \
    sh -c "wget -qO- 'http://prometheus:9090${path}'" 2>/dev/null
}

@test "prometheus server scrapes nes sink" {
  setup_distributed_prometheus tests/good/prometheus-sink.yaml

  run DOCKER_NES_CLI -t tests/good/prometheus-sink.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO PROMETHEUS_SINK'
  [ "$status" -eq 0 ]
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  # Wait for the query to be Running on the global record.
  for i in $(seq 1 10); do
    sleep 1
    run DOCKER_NES_CLI -t tests/good/prometheus-sink.yaml status "$QUERY_ID"
    [ "$status" -eq 0 ]
    QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    if [ "$QUERY_STATUS" = "Running" ]; then
      break
    fi
  done
  [ "$QUERY_STATUS" = "Running" ]

  # Poll worker-2's /metrics endpoint until the sink has actually emitted at
  # least one sample for GENERATOR_SOURCE_DOUBLE. The gauge instance with
  # empty labels is only created when the sink's execute() runs - so seeing
  # the value line proves at least one tuple flowed through the sink.
  local sink_value=""
  for i in $(seq 1 20); do
    local raw
    raw=$(tail -f /dev/null | docker compose exec -T worker-2 \
      sh -c "curl -sf http://localhost:4356/metrics" 2>/dev/null || true)
    sink_value=$(echo "$raw" | awk '$1 == "GENERATOR_SOURCE_DOUBLE" { print $2; exit }')
    if [ -n "$sink_value" ]; then
      echo "# sink emitted GENERATOR_SOURCE_DOUBLE=${sink_value} after ${i}s" >&3
      break
    fi
    sleep 1
  done
  [ -n "$sink_value" ]
  # Sanity: the gauge value should be a real number, not 0 (which would indicate
  # binary data is being incorrectly re-formatted - see NetworkSink path).
  [ "$sink_value" != "0" ]

  # Confirm the prometheus server's scrape target is healthy.
  local targets target_health target_url
  targets=$(prometheus_api "/api/v1/targets")
  target_url=$(echo "$targets" | jq -r '.data.activeTargets[0].scrapeUrl // ""')
  target_health=$(echo "$targets" | jq -r '.data.activeTargets[0].health // ""')
  echo "# scrape target: url=${target_url} health=${target_health}" >&3
  [ "$target_health" = "up" ]
  [[ "$target_url" == *"worker-2:4356"* ]]

  # Poll Prometheus for the gauge series until the scrape has landed. This is
  # the actual "prometheus received tuples" assertion: the series only exists
  # in prometheus's storage if the sink's gauge was Set (i.e. a tuple flowed)
  # AND prometheus successfully scraped the sink at least once.
  local prom_value=""
  for i in $(seq 1 20); do
    prom_value=$(prometheus_api "/api/v1/query?query=GENERATOR_SOURCE_DOUBLE" \
      | jq -r '.data.result[0].value[1] // ""')
    if [ -n "$prom_value" ]; then
      echo "# prometheus has GENERATOR_SOURCE_DOUBLE=${prom_value} after ${i}s" >&3
      break
    fi
    sleep 1
  done
  [ -n "$prom_value" ]
  echo "$prom_value" | grep -Eq '^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$'

  # Confirm prometheus is actively scraping (exposer_scrapes_total monotonically
  # increases per scrape) - this rules out the metric being a stale leftover.
  local scrapes_a scrapes_b
  scrapes_a=$(prometheus_api "/api/v1/query?query=exposer_scrapes_total" \
    | jq -r '.data.result[0].value[1] // "0"')
  sleep 2
  scrapes_b=$(prometheus_api "/api/v1/query?query=exposer_scrapes_total" \
    | jq -r '.data.result[0].value[1] // "0"')
  echo "# exposer_scrapes_total: ${scrapes_a} -> ${scrapes_b}" >&3
  [ "$(echo "$scrapes_b > $scrapes_a" | bc)" = "1" ]

  # Confirm the gauge value changes between scrapes - this proves the sink is
  # actively pushing fresh tuples (rather than the gauge being stuck on a single
  # value).
  local later_value
  later_value=$(prometheus_api "/api/v1/query?query=GENERATOR_SOURCE_DOUBLE" \
    | jq -r '.data.result[0].value[1] // ""')
  echo "# later value: ${later_value}" >&3
  [ -n "$later_value" ]
  echo "$later_value" | grep -Eq '^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$'
  [ "$prom_value" != "$later_value" ]
}
