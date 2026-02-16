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

#include_directories("/home/flang/prometheus-cpp/core/include")
#include_directories("/home/flang/prometheus-cpp/pull/include")
#include_directories("/home/flang/prometheus-cpp/_build/core/include")
#include_directories("/home/flang/prometheus-cpp/_build/pull/include")
#link_directories("/home/flang/prometheus-cpp/_build/lib")
#find_package(prometheus-cpp REQUIRED)
#find_package(prometheus-cpp REQUIRED "/home/flang/prometheus-cpp/_build")
#set(CMAKE_PREFIX_PATH "/home/flang/prometheus-cpp/_build")

# Disable Prometheus tests to avoid gmock conflicts
set(ENABLE_TESTING OFF CACHE BOOL "Disable Prometheus tests" FORCE)

# Do not use push
set(ENABLE_PUSH OFF CACHE BOOL "Disable prometheus push" FORCE)
#set(ENABLE_PULL ON CACHE BOOL "Disable prometheus push" FORCE)
set(GENERATE_PKGCONFIG OFF CACHE BOOL "Disable prometheus push" FORCE)

#set(CURL_INCLUDE_DIR "" CACHE PATH "Fake CURL include dir" FORCE)
#set(CURL_LIBRARY "" CACHE FILEPATH "Fake CURL library" FORCE)

# Optionally, set build type or shared/static
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)

# Cive
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
add_subdirectory("/home/flang/prometheus-cpp" "${CMAKE_BINARY_DIR}/prometheus_build")
# Suppress strict warnings for CivetWeb files
#target_compile_options(prometheus-cpp-core PRIVATE
#        -Wno-missing-field-initializers
#)
#target_compile_options(prometheus-cpp-pull PRIVATE
#        -Wno-missing-field-initializers
#)
#find_package(prometheus-cpp REQUIRED)

# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sinks
target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable prometheus-cpp::pull prometheus-cpp::core)
#target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable prometheus-cpp-pull prometheus-cpp-core)
#target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable "/home/flang/prometheus-cpp/_build/lib/libprometheus-cpp-pull.so" "/home/flang/prometheus-cpp/_build/lib/libprometheus-cpp-pull.so")
#target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable "/home/flang/prometheus-cpp/_build/lib/libprometheus-cpp-pull.a" "/home/flang/prometheus-cpp/_build/lib/libprometheus-cpp-pull.a")

target_include_directories(nes-sinks PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sinks REUSE_FROM nes-common)
endif ()

create_registries_for_component(Sink SinkValidation)
