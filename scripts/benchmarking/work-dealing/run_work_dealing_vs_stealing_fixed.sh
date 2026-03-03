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

myenv/bin/python3 -m scripts.benchmarking.work-dealing.run_nes_work_dealing_stealing --wait-between-queries 3 --wait-before-stopping-queries 5 --generator-rates scripts/benchmarking/work-dealing/work-dealing-fixed/ingestion_queries_fixed.yaml --number-of-queries 4 --buffer-size 8192 --number-of-buffers 4000000 --flush-interval 100

# Deactivate the virtual environment
deactivate
rm -rf myenv
