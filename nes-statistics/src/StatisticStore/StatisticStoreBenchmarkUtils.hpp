/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace NES
{

struct ChunkBounds
{
    uint64_t start;
    uint64_t end;
};

/// Returns [start, end) for threadId when distributing numItems across numThreads.
/// The first (numItems % numThreads) threads get one extra item.
ChunkBounds calcChunkBounds(uint64_t threadId, uint64_t numItems, uint64_t numThreads);

/// Formats bytes as a human-readable string (e.g. "1.5 GiB").
std::string formatBytes(uint64_t bytes);

/// Right-align a numeric value in a "key=value" field of at least `width` digits.
std::string pad(std::string_view key, uint64_t value, int width);

/// Left-align a string to `width` characters, padding with spaces on the right.
std::string padLeft(std::string_view value, int width);

/// Formats a duration in seconds as HH:MM:SS.
std::string formatHMS(double totalSeconds);

/// Iterates over the cartesian product of the given ranges, calling fn with one value from each.
///
/// Example:
///   forEachParam([&](auto storeType, auto numThreads)
///   {
///       /// body runs for every (storeType, numThreads) combination
///   }, Params::storeTypes, Params::threadCounts);
template <typename Fn, typename Range>
void forEachParam(Fn&& fn, Range&& range)
{
    for (const auto& val : range)
        fn(val);
}

template <typename Fn, typename Range, typename... Rest>
void forEachParam(Fn&& fn, Range&& range, Rest&&... rest)
{
    for (const auto& val : range)
        forEachParam([&](auto&&... args) { fn(val, args...); }, std::forward<Rest>(rest)...);
}

/// Runs one timed experiment: spawns numThreads threads, each calling threadFn on its [start, end) chunk.
/// Returns elapsed wall-clock time in milliseconds as a double for sub-millisecond precision.
template <typename ThreadFn>
double runTimedExperiment(const uint64_t numThreads, const uint64_t numItems, ThreadFn&& threadFn)
{
    const auto startTime = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (uint64_t t = 0; t < numThreads; ++t)
    {
        const auto [start, end] = calcChunkBounds(t, numItems, numThreads);
        threads.emplace_back([&threadFn, start, end]() { threadFn(start, end); });
    }

    for (auto& thread : threads)
    {
        thread.join();
    }

    const auto endTime = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(endTime - startTime).count();
}

struct BenchmarkArgs
{
    std::vector<std::string> filter;
    std::vector<std::string> exclude;

    /// Returns true if the config should be skipped:
    /// - any filter token is absent from reportLine, or
    /// - any exclude token is present in reportLine.
    bool shouldSkip(const std::string& reportLine) const;

    /// Parses --filter get,stats=50000 --exclude DEFAULT
    /// Prints usage and exits with 0 for --help/-h.
    /// Prints an error and exits with 1 for unrecognized arguments or invalid values.
    BenchmarkArgs(int argc, char* argv[]);
};

}
