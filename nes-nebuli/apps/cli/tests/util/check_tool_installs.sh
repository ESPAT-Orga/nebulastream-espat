# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env bash

set -euo pipefail

# Check if yq is installed
if ! command -v yq >/dev/null 2>&1; then
  echo "Error: yq is not installed or not in PATH." >&2
  exit 1
fi

# Check if yq can read from /tmp (to detect confined snap versions)
TMP_TEST_FILE="/tmp/yq_read_test_$$.yaml"
echo "test: ok" > "$TMP_TEST_FILE"

if ! yq '.test' "$TMP_TEST_FILE" >/dev/null 2>&1; then
  echo "Error: yq cannot read files from /tmp." >&2
  echo "This is often caused by a confined snap installation of yq." >&2
  rm -f "$TMP_TEST_FILE"
  exit 1
fi

rm -f "$TMP_TEST_FILE"

# Check if pcregrep is installed
if ! command -v pcregrep >/dev/null 2>&1; then
  echo "Error: pcregrep is not installed or not in PATH." >&2
  exit 1
fi

echo "All checks passed."
