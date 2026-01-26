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