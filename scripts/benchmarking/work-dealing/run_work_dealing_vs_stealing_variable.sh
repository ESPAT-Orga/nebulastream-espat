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

# Create a Python virtual environment and install the required python libraries
python3 -m venv myenv
source myenv/bin/activate
pip install argparse requests pandas pyyaml

myenv/bin/python3 -m scripts.benchmarking.work-dealing.run_nes_work_dealing_stealing --wait-between-queries 0.001 --wait-before-stopping-queries 30 --generator-rates scripts/benchmarking/work-dealing/work-dealing-variable/ingestion_queries_variable.yaml --number-of-queries 1 --buffer-size 10240 --number-of-buffers 200000 --flush-interval 5

# Deactivate the virtual environment
deactivate
rm -rf myenv
