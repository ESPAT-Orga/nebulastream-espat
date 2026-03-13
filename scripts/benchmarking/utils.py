#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
import subprocess
import socket

from typing import Optional, Tuple



#### General Util Methods
def create_folder_and_remove_if_exists(folder_path):
    """
    Create a folder and remove it if it already exists.
    :param folder_path: Path of the folder to create.
    """
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Remove the folder and all its contents
        shutil.rmtree(folder_path)
        print(f"Removed existing folder: {folder_path}")

    # Create the folder
    os.makedirs(folder_path)
    print(f"Created folder: {folder_path}")


def check_repository_root():
    """Check if the script is being run from the repository root."""
    expected_dirs = ["nes-sources", "nes-sql-parser", "nes-systests"]
    current_dir = os.getcwd()
    contents = os.listdir(current_dir)

    if not all(expected_dir in contents for expected_dir in expected_dirs):
        raise RuntimeError("The script is not being run from the repository root.")



#### NebulaStream specific
def get_vcpkg_dir():
    # Get the hostname
    hostname = socket.gethostname()

    print(f'Getting vcpkg directory for hostname: {hostname}')

    # Determine the vcpkg directory based on the hostname
    if hostname == "nschubert-thinkstation":
        vcpkg_dir = "/home/nschubert/remote_server/vcpkg/scripts/buildsystems/vcpkg.cmake"
    elif hostname == "hare":
        vcpkg_dir = "/data/vcpkg/scripts/buildsystems/vcpkg.cmake"
    elif hostname == "mif-ws":
        vcpkg_dir = "/home/flang/vcpkg/scripts/buildsystems/vcpkg.cmake"
    elif hostname == "docker-hostname":
        vcpkg_dir = "/vcpkg"
    elif hostname == "beaver":
        vcpkg_dir = "/home/tdietzel/vcpkg/scripts/buildsystems/vcpkg.cmake"
    else:
        raise ValueError(f"Unknown hostname: {hostname}. Cannot determine vcpkg directory.")

    return vcpkg_dir

def run_command(command, cwd=None):
    print(f"Running {command}")
    result = subprocess.run(command, cwd=cwd, shell=True, check=True, text=True, capture_output=True)
    return result.stdout

def convert_unit_prefix(value, unit_prefix):
    # Convert throughput based on the unit prefix
    if unit_prefix == 'M':
        return value * 1e6  # Convert to actual value (million)
    elif unit_prefix == 'B':
        return value * 1e9  # Convert to actual value (billion)
    elif unit_prefix == 'k':
        return value * 1e3  # Convert to actual value (thousand)
    elif unit_prefix == 'm':
        return value * 1e-3
    elif unit_prefix == 'u':
        return value * 1e-6
    elif unit_prefix == 'n':
        return value * 1e-9
    elif unit_prefix == '':
        return value  # No conversion needed
    else:
        raise ValueError(f"Could not convert {value} for {unit_prefix}")


from typing import Optional

def parse_version(version_str: str) -> Tuple[int, int, int]:
    """Parse a version string (e.g., '3.22.1') into a tuple of integers."""
    parts = version_str.split(".")
    # Pad with zeros if minor or patch is missing (e.g., '3.22' becomes '3.22.0')
    while len(parts) < 3:
        parts.append("0")
    return tuple(map(int, parts))

def find_cmake_version(cmake_path: str) -> Optional[Tuple[int, int, int]]:
    """Check the version of a cmake executable."""
    try:
        result = subprocess.run(
            [cmake_path, "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        # Extract version string (e.g., "cmake version 3.22.1")
        version_str = result.stdout.split()[2]
        return parse_version(version_str)
    except (subprocess.CalledProcessError, IndexError, ValueError):
        return None

def find_suitable_cmake() -> Optional[str]:
    """Search for cmake in PATH and return the first one with version >= 3.21.0."""
    # Get all cmake executables in PATH
    cmake_paths = []
    for path in os.environ.get("PATH", "").split(os.pathsep):
        cmake_exe = os.path.join(path, "cmake")
        if os.path.isfile(cmake_exe) and os.access(cmake_exe, os.X_OK):
            cmake_paths.append(cmake_exe)

    # Check versions
    for cmake_path in cmake_paths:
        version = find_cmake_version(cmake_path)
        if version is not None and version >= (3, 21, 0):
            return cmake_path

    return None

def compile_nebulastream(cmake_flags, build_dir):
    cmake_path = find_suitable_cmake()
    if not cmake_path:
        raise RuntimeError("No suitable cmake (version >= 3.21) found in PATH.")

    cmake_command = f"{cmake_path} {cmake_flags} -S . -B {build_dir}"
    build_command = f"{cmake_path} --build {build_dir}"

    print(f"Using cmake at: {cmake_path}")
    print("Running cmake...")
    run_command(cmake_command)
    print("Building the project...")
    run_command(build_command)
