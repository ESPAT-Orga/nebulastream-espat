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

set -euo pipefail

# Create a Python virtual environment and install numpy (used by generate_bid_data.py).
# The venv is kept across runs so numpy isn't reinstalled every invocation.
if [ ! -d myenv ]; then
    python3 -m venv myenv
    myenv/bin/pip install --quiet numpy
fi
source myenv/bin/activate

# `python -m` rejects hyphens in module paths (`adaptive-optimization` isn't a valid identifier),
# so run the script by file path. The script appends the repo root to sys.path itself.
myenv/bin/python3 scripts/benchmarking/adaptive-optimization/run_bid_value_first_benchmark.py "$@"

deactivate
