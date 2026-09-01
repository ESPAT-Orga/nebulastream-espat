# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_statistics_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-statistics)
endfunction()

add_nes_statistics_test(statistic-store-test StatisticStoreTest.cpp)

# These two compile and run real queries, so they need the worker, the optimizer and the plugins.
function(add_nes_statistics_query_test)
    add_nes_statistics_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_include_directories(${TARGET_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(${TARGET_NAME} nes-single-node-worker-lib nes-query-optimizer nes-distributed)
endfunction()

add_nes_statistics_query_test(statistic-round-trip-test StatisticRoundTripTest.cpp)

# Additionally needs nes-grpc for the generated StatisticService stubs the test server uses.
add_nes_statistics_query_test(statistic-transport-test StatisticTransportTest.cpp)
target_link_libraries(statistic-transport-test nes-grpc)
