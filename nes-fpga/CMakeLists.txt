# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Add Source Code
add_subdirectory(src)

# Add nes-fpga library
get_source(nes-fpga NES_FPGA_SOURCE_FILES)
add_library(nes-fpga ${NES_FPGA_SOURCE_FILES})
# target_link_libraries(nes-fpga PUBLIC nes-common nes-runtime)
target_include_directories(nes-fpga PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

add_executable(nes-fpga-main src/main.cpp)
target_link_libraries(nes-fpga-main PRIVATE nes-fpga)
target_include_directories(nes-fpga-main PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
