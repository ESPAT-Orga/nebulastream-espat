# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sinks and Headers
add_subdirectory(src)
get_source(nes-sinks NES_SINKS_SOURCE_FILES)

# Add Library
add_library(nes-sinks ${NES_SINKS_SOURCE_FILES})

# Configure Prometheus C++ as a FetchContent dependency
FetchContent_Declare(
        prometheus_cpp
        GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
        GIT_TAG        master 
)

# Disable Prometheus tests to avoid gmock dependency conflicts
set(ENABLE_TESTING OFF CACHE BOOL "Disable Prometheus tests" FORCE)
# Disable push to avoid dependency on curl
set(ENABLE_PUSH OFF CACHE BOOL "Disable prometheus push" FORCE)
set(GENERATE_PKGCONFIG OFF CACHE BOOL "Disable prometheus push" FORCE)

# Build static
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)

# Civetweb must ignore a warning about a missing initializer field at struct creation
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")

# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sinks
target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable prometheus-cpp::pull prometheus-cpp::core)

# Make the repository available
FetchContent_MakeAvailable(prometheus_cpp)

target_include_directories(nes-sinks PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sinks REUSE_FROM nes-common)
endif ()

create_registries_for_component(Sink SinkValidation)
