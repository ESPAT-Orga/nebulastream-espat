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

#include <ProgressTracker.hpp>
#include <StatisticStoreBenchmarkUtils.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>
#include <variant>

#include <StatisticStore/DefaultStatisticStore.hpp>
#include <StatisticStore/SubStoresStatisticStore.hpp>
#include <StatisticStore/WindowStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <Statistic.hpp>

namespace NES
{
namespace
{
/// Output CSV filename
constexpr std::string_view BENCHMARK_CSV = "statistic_store_benchmark.csv";

constexpr uint64_t NUM_REPS = 3;

/// Generate a random seed at startup; written to CSV so runs are reproducible
const uint32_t RNG_SEED = std::random_device{}();

/// ============================================================
/// Benchmark parameter sets
/// ============================================================

/// Maximum total data bytes for pre-created statistics before a config is skipped.
/// Accounts for the original vector + copy + store contents. Set conservatively to avoid OOM.
constexpr uint64_t MAX_DATA_BYTES = 30ULL * 1024 * 1024 * 1024;

struct InsertParams
{
    static inline const std::vector<StatisticStoreType> storeTypes{
        StatisticStoreType::DEFAULT, StatisticStoreType::WINDOW, StatisticStoreType::SUB_STORES};
    static inline const std::vector<uint64_t> numStatisticsVals{500'000};
    static inline const std::vector<uint64_t> numStatisticIdsVals{100};
    static inline const std::vector<uint64_t> statisticSizes{1024, 4 * 1024};
    static inline const std::vector<uint64_t> threadCounts{1, 4, 16};
    static constexpr uint64_t windowSize = 60'000;

    /// Insert pre-creates numStatistics stats of statisticSize bytes.
    static bool fitsInMemory(const uint64_t numStatistics, const uint64_t statisticSize)
    {
        return numStatistics * statisticSize <= MAX_DATA_BYTES;
    }

    static uint64_t validateAndCount()
    {
        PRECONDITION(numStatisticsVals >= numStatisticIdsVals, "Cannot have more IDs than Statistics");
        uint64_t count = 0;
        for (const auto numStatistics : numStatisticsVals)
        {
            for (const auto statisticSize : statisticSizes)
            {
                /// Configs that do not pass fitsInMemory() are printed at the end of all runs via ProgressTracker::printSkipped()
                if (fitsInMemory(numStatistics, statisticSize))
                {
                    count += storeTypes.size() * threadCounts.size();
                }
            }
        }
        return count;
    }
};

struct GetParams
{
    static inline const std::vector<StatisticStoreType> storeTypes{
        StatisticStoreType::DEFAULT, StatisticStoreType::WINDOW, StatisticStoreType::SUB_STORES};
    static inline const std::vector<uint64_t> windowSizes{1'000, 10'000, 60'000};
    static inline const std::vector<uint64_t> numStatisticsVals{500'000};
    static inline const std::vector<uint64_t> numStatisticIdsVals{100};
    static inline const std::vector<uint64_t> pctAccessExistingVals{10, 50, 90};
    static inline const std::vector<uint64_t> numStatisticsPerRequestVals{1, 10, 100};
    static inline const std::vector<uint64_t> statisticSizes{4 * 1024};
    static inline const std::vector<uint64_t> threadCounts{1, 4, 16};

    /// Get pre-populates numStatisticIds * numStatistics stats of statisticSize bytes.
    static bool fitsInMemory(const uint64_t numStatistics, const uint64_t statisticSize)
    {
        return numStatistics * statisticSize <= MAX_DATA_BYTES;
    }

    static uint64_t validateAndCount()
    {
        PRECONDITION(numStatisticsVals >= numStatisticIdsVals, "Cannot have more IDs than Statistics");
        uint64_t count = 0;
        for (const auto numStatistics : numStatisticsVals)
        {
            for (const auto statisticSize : statisticSizes)
            {
                /// Configs that do not pass fitsInMemory() are printed at the end of all runs via ProgressTracker::printSkipped()
                if (fitsInMemory(numStatistics, statisticSize))
                {
                    count += storeTypes.size() * windowSizes.size() * pctAccessExistingVals.size() * numStatisticsPerRequestVals.size()
                        * threadCounts.size();
                }
            }
        }
        return count;
    }
};

struct MixedParams
{
    static inline const std::vector<StatisticStoreType> storeTypes{
        StatisticStoreType::DEFAULT, StatisticStoreType::WINDOW, StatisticStoreType::SUB_STORES};
    static inline const std::vector<uint64_t> windowSizes{1'000, 10'000, 60'000};
    static inline const std::vector<uint64_t> numStatisticsVals{500'000};
    static inline const std::vector<uint64_t> numStatisticIdsVals{100};
    static inline const std::vector<uint64_t> pctInsertVals{10, 50, 90};
    static inline const std::vector<uint64_t> pctPrePopulateVals{10};
    static inline const std::vector<uint64_t> numStatisticsPerRequestVals{1, 10, 100};
    static inline const std::vector<uint64_t> statisticSizes{4 * 1024};
    static inline const std::vector<uint64_t> threadCounts{1, 4, 16};

    /// Mixed pre-populates numStatistics + prepares numStatistics inserts.
    static bool fitsInMemory(const uint64_t numStatistics, const uint64_t statisticSize)
    {
        return numStatistics * statisticSize <= MAX_DATA_BYTES;
    }

    static uint64_t validateAndCount()
    {
        PRECONDITION(numStatisticsVals >= numStatisticIdsVals, "Cannot have more IDs than Statistics");
        uint64_t count = 0;
        for (const auto numStatistics : numStatisticsVals)
        {
            for (const auto statisticSize : statisticSizes)
            {
                /// Configs that do not pass fitsInMemory() are printed at the end of all runs via ProgressTracker::printSkipped()
                if (fitsInMemory(numStatistics, statisticSize))
                {
                    count += storeTypes.size() * windowSizes.size() * pctInsertVals.size() * pctPrePopulateVals.size()
                        * numStatisticsPerRequestVals.size() * threadCounts.size();
                }
            }
        }
        return count;
    }
};

/// ============================================================

struct PreparedStatistic
{
    Statistic statistic;
    Statistic::StatisticId statisticId;
};

struct StatisticData
{
    std::shared_ptr<std::byte[]> data;
    uint64_t size;
};

Statistic createDummyStatistic(
    const Statistic::StatisticId statisticId,
    Windowing::TimeMeasure startTs,
    Windowing::TimeMeasure endTs,
    std::shared_ptr<std::byte[]> statisticData,
    const int statisticSize)
{
    /// Picking always the first statistic type, as we do not care about the type
    constexpr auto statisticTypes = magic_enum::enum_values<Statistic::StatisticType>();
    constexpr auto randomStatisticType = statisticTypes[0];

    /// We do not care about the number of seen tuples
    constexpr auto numberOfSeenTuples = 42;

    return {
        statisticId,
        randomStatisticType,
        startTs,
        endTs,
        numberOfSeenTuples,
        std::move(statisticData),
        static_cast<uint64_t>(statisticSize)};
}

using StatisticStoreVariant = std::variant<DefaultStatisticStore, WindowStatisticStore, SubStoresStatisticStore>;

/// Creates a statistic store variant based on the StatisticStoreType enum
StatisticStoreVariant createStore(const StatisticStoreType storeType, const int numThreads, const uint64_t windowSize)
{
    switch (storeType)
    {
        case StatisticStoreType::DEFAULT:
            return DefaultStatisticStore{};
        case StatisticStoreType::WINDOW:
            return WindowStatisticStore{static_cast<uint64_t>(numThreads), Windowing::TimeMeasure{windowSize}};
        case StatisticStoreType::SUB_STORES:
            return SubStoresStatisticStore{static_cast<uint64_t>(numThreads)};
    }
    std::unreachable();
}

struct ChunkedPreparedStatistics
{
    std::vector<uint64_t> starts;
    std::vector<uint64_t> ends;
    std::vector<uint64_t> chunkSizes;
    std::vector<PreparedStatistic> preparedStatistics;
};

static ChunkedPreparedStatistics createStats(
    const uint64_t numStatistics,
    const uint64_t numStatisticIds,
    const StatisticData& statisticData,
    const uint64_t windowSize,
    const uint64_t numThreads)
{
    ChunkedPreparedStatistics result;
    result.starts.resize(numThreads);
    result.ends.resize(numThreads);
    result.chunkSizes.resize(numThreads);

    /// Compute per-thread chunk boundaries
    for (uint64_t t = 0; t < numThreads; ++t)
    {
        const auto [start, end] = calcChunkBounds(t, numStatistics, numThreads);
        result.starts[t] = start;
        result.ends[t] = end;
        result.chunkSizes[t] = end - start;
    }

    /// Each thread generates its own chunk into a local vector (avoids needing a default constructor for PreparedStatistic)
    std::vector<std::vector<PreparedStatistic>> perThreadStats(numThreads);
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    for (uint64_t threadId = 0; threadId < numThreads; ++threadId)
    {
        threads.emplace_back(
            [&perThreadStats, &result, threadId, numStatisticIds, windowSize, &statisticData]()
            {
                std::mt19937 rng{RNG_SEED + static_cast<uint32_t>(threadId)};
                std::uniform_int_distribution<uint64_t> idDist{0, numStatisticIds - 1};

                const uint64_t start = result.starts[threadId];
                const uint64_t end = result.ends[threadId];
                perThreadStats[threadId].reserve(result.chunkSizes[threadId]);
                uint64_t curTimestamp = start * windowSize;
                for (uint64_t i = start; i < end; ++i)
                {
                    const Statistic::StatisticId statisticId{idDist(rng)};
                    const Windowing::TimeMeasure startTs{curTimestamp};
                    const Windowing::TimeMeasure endTs{curTimestamp + windowSize};
                    auto statistic = createDummyStatistic(statisticId, startTs, endTs, statisticData.data, statisticData.size);
                    perThreadStats[threadId].emplace_back(std::move(statistic), statisticId);
                    curTimestamp += windowSize;
                }
            });
    }

    for (auto& thread : threads)
    {
        thread.join();
    }

    /// Concatenate per-thread chunks into the final vector
    result.preparedStatistics.reserve(numStatistics);
    for (auto& chunk : perThreadStats)
    {
        for (auto& ps : chunk)
        {
            result.preparedStatistics.emplace_back(std::move(ps));
        }
    }

    return result;
}

/// ============================================================
/// InsertStatistic benchmark
/// ============================================================

void runInsertStatisticBenchmark(std::ofstream& csv, ProgressTracker& progress)
{
    using Params = InsertParams;

    for (const auto statisticSize : Params::statisticSizes)
    {
        /// Generating statistic data, we do not care about the actual contents, as we are solely benchmarking the statistic store
        StatisticData statisticData{.data = std::make_shared<std::byte[]>(statisticSize), .size = statisticSize};

        forEachParam(
            [&](const auto numStatisticIds, const auto numStatistics)
            {
                if (not Params::fitsInMemory(numStatistics, statisticSize))
                {
                    const auto required = numStatistics * statisticSize;
                    progress.skip(
                        "InsertStatistic",
                        "stats=" + std::to_string(numStatistics) + " | ids=" + std::to_string(numStatisticIds)
                            + " | size=" + std::to_string(statisticSize),
                        "requires " + formatBytes(required) + " > limit " + formatBytes(MAX_DATA_BYTES));
                    return;
                }

                /// Pre-create all statistics using the maximum thread count for parallel generation
                const auto stats
                    = createStats(numStatistics, numStatisticIds, statisticData, Params::windowSize, std::thread::hardware_concurrency());

                forEachParam(
                    [&](const auto storeType, const auto numThreads)
                    {
                        for (uint64_t rep = 0; rep < NUM_REPS; ++rep)
                        {
                            /// Fresh store for each repetition
                            auto store = createStore(storeType, static_cast<int>(numThreads), Params::windowSize);

                            /// Copy prepared statistics before timing so the copy cost is excluded
                            auto statsCopy = stats.preparedStatistics;

                            const auto durationMs = runTimedExperiment(
                                numThreads,
                                numStatistics,
                                [&store, &statsCopy](const uint64_t start, const uint64_t end)
                                {
                                    for (uint64_t i = start; i < end; ++i)
                                    {
                                        auto& [statistic, statisticId] = statsCopy[i];
                                        std::visit([&](auto& s) { s.insertStatistic(statisticId, std::move(statistic)); }, store);
                                    }
                                });

                            csv << "InsertStatistic," << magic_enum::enum_name(storeType) << "," << numThreads << "," << numStatistics
                                << "," << numStatisticIds << "," << statisticSize << "," << Params::windowSize << ",-1,-1,-1,-1,"
                                << RNG_SEED << "," << rep << "," << durationMs << "\n"
                                << std::flush;
                        }

                        progress.report(
                            "Insert | " + padLeft(magic_enum::enum_name(storeType), 10) + " | " + pad("threads", numThreads, 2) + " | "
                            + pad("stats", numStatistics, 7) + " | " + pad("ids", numStatisticIds, 7) + " | "
                            + pad("size", statisticSize, 5) + " | " + pad("ws", Params::windowSize, 5));
                    },
                    Params::storeTypes,
                    Params::threadCounts);
            },
            Params::numStatisticIdsVals,
            Params::numStatisticsVals);
    }
}

/// ============================================================
/// GetStatistics benchmark
/// ============================================================

void runGetStatisticsBenchmark(std::ofstream& csv, ProgressTracker& progress)
{
    using Params = GetParams;

    for (const auto statisticSize : Params::statisticSizes)
    {
        /// Generating statistic data, we do not care about the actual contents, as we are solely benchmarking the statistic store
        StatisticData statisticData{.data = std::make_shared<std::byte[]>(statisticSize), .size = statisticSize};

        forEachParam(
            [&](const auto numStatisticIds, const auto numStatistics)
            {
                if (not Params::fitsInMemory(numStatistics, statisticSize))
                {
                    const auto required = numStatistics * statisticSize;
                    progress.skip(
                        "GetStatistics",
                        "stats=" + std::to_string(numStatistics) + " | ids=" + std::to_string(numStatisticIds)
                            + " | size=" + std::to_string(statisticSize),
                        "requires " + formatBytes(required) + " > limit " + formatBytes(MAX_DATA_BYTES));
                    return;
                }

                for (const auto windowSize : Params::windowSizes)
                {
                    /// Pre-create all statistics using the maximum thread count for parallel generation
                    const auto stats
                        = createStats(numStatistics, numStatisticIds, statisticData, windowSize, std::thread::hardware_concurrency());
                    /// Collect inserted IDs before parallel insert
                    std::vector<Statistic::StatisticId> insertedIds;
                    insertedIds.reserve(numStatistics);
                    for (const auto& ps : stats.preparedStatistics)
                    {
                        insertedIds.emplace_back(ps.statisticId);
                    }

                    forEachParam(
                        [&](const auto storeType, const auto pctAccessExisting, const auto numThreads)
                        {
                            /// Pre-populate the store (not timed)
                            const auto hwThreads = static_cast<uint64_t>(std::thread::hardware_concurrency());

                            auto store = createStore(storeType, static_cast<int>(numThreads), windowSize);

                            /// Insert all statistics in parallel using all available cores
                            {
                                std::vector<std::thread> threads;
                                threads.reserve(hwThreads);
                                for (uint64_t threadId = 0; threadId < hwThreads; ++threadId)
                                {
                                    const auto [start, end] = calcChunkBounds(threadId, numStatistics, hwThreads);
                                    threads.emplace_back(
                                        [&store, &stats, start, end]()
                                        {
                                            for (uint64_t i = start; i < end; ++i)
                                            {
                                                const auto& [statistic, statisticId] = stats.preparedStatistics[i];
                                                std::visit([&](auto& s) { s.insertStatistic(statisticId, std::move(statistic)); }, store);
                                            }
                                        });
                                }
                                for (auto& thread : threads)
                                {
                                    thread.join();
                                }
                            }

                            std::mt19937 gen{RNG_SEED};
                            const uint64_t maxTs = numStatistics * windowSize;

                            /// Create non-existing statistic IDs (beyond inserted range)
                            std::vector<Statistic::StatisticId> nonExistingIds;
                            for (uint64_t id = numStatisticIds; id < numStatisticIds + 10; ++id)
                            {
                                nonExistingIds.emplace_back(Statistic::StatisticId{id});
                            }

                            /// Build the lookup sequence: pctAccessExisting% existing, rest non-existing
                            std::vector<Statistic::StatisticId> lookupIds;
                            lookupIds.reserve(numStatistics);
                            std::uniform_int_distribution<> existingDist{0, static_cast<int>(insertedIds.size()) - 1};
                            std::uniform_int_distribution<> nonExistingDist{0, static_cast<int>(nonExistingIds.size()) - 1};
                            std::uniform_int_distribution<uint64_t> percentDist{0, 99};
                            for (uint64_t i = 0; i < numStatistics; ++i)
                            {
                                if (percentDist(gen) < pctAccessExisting)
                                {
                                    lookupIds.emplace_back(insertedIds[existingDist(gen)]);
                                }
                                else
                                {
                                    lookupIds.emplace_back(nonExistingIds[nonExistingDist(gen)]);
                                }
                            }

                            for (const auto numStatisticsPerRequest : Params::numStatisticsPerRequestVals)
                            {
                                const uint64_t queryRangeTs = numStatisticsPerRequest * windowSize;

                                /// Pre-generate random start timestamps so the RNG cost is excluded from timing
                                const uint64_t maxStartTs = maxTs > queryRangeTs ? maxTs - queryRangeTs : 0;
                                std::vector<uint64_t> lookupStartTs(lookupIds.size());
                                std::uniform_int_distribution<uint64_t> startTsDist{0, maxStartTs};
                                for (uint64_t i = 0; i < lookupIds.size(); ++i)
                                {
                                    lookupStartTs[i] = startTsDist(gen);
                                }

                                /// The store is read-only during lookups, so we reuse it across repetitions
                                for (uint64_t rep = 0; rep < NUM_REPS; ++rep)
                                {
                                    const auto durationMs = runTimedExperiment(
                                        numThreads,
                                        numStatistics,
                                        [&store, &lookupIds, &lookupStartTs, queryRangeTs](const uint64_t start, const uint64_t end)
                                        {
                                            for (uint64_t i = start; i < end; ++i)
                                            {
                                                const auto& statisticId = lookupIds[i];
                                                const auto startTs = lookupStartTs[i];
                                                auto result = std::visit(
                                                    [&](auto& s)
                                                    {
                                                        return s.getStatistics(
                                                            statisticId,
                                                            Windowing::TimeMeasure{startTs},
                                                            Windowing::TimeMeasure{startTs + queryRangeTs});
                                                    },
                                                    store);
                                            }
                                        });

                                    csv << "GetStatistics," << magic_enum::enum_name(storeType) << "," << numThreads << "," << numStatistics
                                        << "," << numStatisticIds << "," << statisticSize << "," << windowSize << "," << pctAccessExisting
                                        << "," << numStatisticsPerRequest << ",-1,-1," << RNG_SEED << "," << rep << "," << durationMs
                                        << "\n"
                                        << std::flush;
                                }

                                progress.report(
                                    "Get    | " + padLeft(magic_enum::enum_name(storeType), 10) + " | " + pad("threads", numThreads, 2)
                                    + " | " + pad("stats", numStatistics, 7) + " | " + pad("ids", numStatisticIds, 7) + " | "
                                    + pad("size", statisticSize, 5) + " | " + pad("ws", windowSize, 5) + " | "
                                    + pad("pctExist", pctAccessExisting, 2) + " | " + pad("statsPerReq", numStatisticsPerRequest, 3));
                            }
                        },
                        Params::storeTypes,
                        Params::pctAccessExistingVals,
                        Params::threadCounts);
                }
            },
            Params::numStatisticIdsVals,
            Params::numStatisticsVals);
    }
}

/// ============================================================
/// InsertAndGetStatistics benchmark
/// ============================================================

void runInsertAndGetBenchmark(std::ofstream& csv, ProgressTracker& progress)
{
    using Params = MixedParams;

    for (const auto statisticSize : Params::statisticSizes)
    {
        /// Generating statistic data, we do not care about the actual contents, as we are solely benchmarking the statistic store
        StatisticData statisticData{.data = std::make_shared<std::byte[]>(statisticSize), .size = statisticSize};

        forEachParam(
            [&](const auto numStatisticIds, const auto numStatistics)
            {
                if (!Params::fitsInMemory(numStatistics, statisticSize))
                {
                    const auto required = numStatistics * statisticSize;
                    progress.skip(
                        "InsertAndGet",
                        "stats=" + std::to_string(numStatistics) + " | ids=" + std::to_string(numStatisticIds)
                            + " | size=" + std::to_string(statisticSize),
                        "requires " + formatBytes(required) + " > limit " + formatBytes(MAX_DATA_BYTES));
                    return;
                }

                for (const auto windowSize : Params::windowSizes)
                {
                    for (const auto pctPrePopulate : Params::pctPrePopulateVals)
                    {
                        const uint64_t numPrePopulate = numStatistics * pctPrePopulate / 100;

                        /// Pre-create statistics for pre-population using the maximum thread count for parallel generation
                        const auto prePopStats
                            = createStats(numPrePopulate, numStatisticIds, statisticData, windowSize, std::thread::hardware_concurrency());
                        const uint64_t maxTs = numPrePopulate * windowSize;

                        /// Prepare insert operations (timestamps continue beyond the pre-populated range)
                        std::mt19937 rng{RNG_SEED};
                        std::uniform_int_distribution<uint64_t> idDist{0, numStatisticIds - 1};

                        std::vector<PreparedStatistic> preparedInserts;
                        preparedInserts.reserve(numStatistics);
                        uint64_t curTs = maxTs;
                        for (uint64_t i = 0; i < numStatistics; ++i)
                        {
                            const Statistic::StatisticId statisticId{idDist(rng)};
                            const Windowing::TimeMeasure startTs{curTs};
                            const Windowing::TimeMeasure endTs{curTs + windowSize};
                            auto statistic = createDummyStatistic(
                                statisticId, startTs, endTs, statisticData.data, static_cast<int>(statisticData.size));
                            preparedInserts.emplace_back(std::move(statistic), statisticId);
                            curTs += windowSize;
                        }

                        /// Prepare lookup IDs from the range that will be pre-populated
                        std::vector<Statistic::StatisticId> lookupIds;
                        lookupIds.reserve(numStatistics);
                        for (uint64_t i = 0; i < numStatistics; ++i)
                        {
                            lookupIds.emplace_back(Statistic::StatisticId{idDist(rng)});
                        }

                        forEachParam(
                            [&](const auto storeType, const auto pctInsert, const auto numThreads, const auto numStatisticsPerRequest)
                            {
                                const uint64_t queryRangeTs = numStatisticsPerRequest * windowSize;

                                /// Pre-determine the insert-vs-get operation sequence
                                std::mt19937 opRng{RNG_SEED};
                                const uint64_t numOps = numStatistics;
                                std::vector<bool> isInsertOp;
                                isInsertOp.reserve(numOps);
                                std::uniform_int_distribution<uint64_t> pctDist{0, 99};
                                for (uint64_t i = 0; i < numOps; ++i)
                                {
                                    isInsertOp.emplace_back(pctDist(opRng) < pctInsert);
                                }

                                /// Pre-generate random start timestamps so the RNG cost is excluded from timing
                                const uint64_t maxStartTs = maxTs > queryRangeTs ? maxTs - queryRangeTs : 0;
                                std::vector<uint64_t> lookupStartTs(lookupIds.size());
                                std::uniform_int_distribution<uint64_t> startTsDist{0, maxStartTs};
                                for (uint64_t i = 0; i < lookupIds.size(); ++i)
                                {
                                    lookupStartTs[i] = startTsDist(opRng);
                                }

                                for (uint64_t rep = 0; rep < NUM_REPS; ++rep)
                                {
                                    /// Fresh store per rep since inserts modify state
                                    auto store = createStore(storeType, static_cast<int>(numThreads), windowSize);

                                    /// Pre-populate the store in parallel using all available cores
                                    {
                                        const auto hwThreads = static_cast<uint64_t>(std::thread::hardware_concurrency());
                                        std::vector<std::thread> threads;
                                        threads.reserve(hwThreads);
                                        for (uint64_t threadId = 0; threadId < hwThreads; ++threadId)
                                        {
                                            const auto [start, end] = calcChunkBounds(threadId, numPrePopulate, hwThreads);
                                            threads.emplace_back(
                                                [&store, &prePopStats, start, end]()
                                                {
                                                    for (uint64_t i = start; i < end; ++i)
                                                    {
                                                        const auto& [statistic, statisticId] = prePopStats.preparedStatistics[i];
                                                        std::visit(
                                                            [&](auto& s) { s.insertStatistic(statisticId, std::move(statistic)); }, store);
                                                    }
                                                });
                                        }
                                        for (auto& thread : threads)
                                        {
                                            thread.join();
                                        }
                                    }

                                    /// Copy insert data before timing (moves will consume it)
                                    auto insertsCopy = preparedInserts;

                                    const auto durationMs = runTimedExperiment(
                                        numThreads,
                                        numOps,
                                        [&store, &insertsCopy, &lookupIds, &lookupStartTs, &isInsertOp, queryRangeTs](
                                            const uint64_t start, const uint64_t end)
                                        {
                                            uint64_t insertIdx = start;
                                            uint64_t lookupIdx = start;
                                            const uint64_t insertCount = insertsCopy.size();
                                            const uint64_t lookupCount = lookupIds.size();

                                            for (uint64_t i = start; i < end; ++i)
                                            {
                                                if (isInsertOp[i])
                                                {
                                                    auto& [statistic, statisticId] = insertsCopy[insertIdx % insertCount];
                                                    std::visit(
                                                        [&](auto& s) { s.insertStatistic(statisticId, std::move(statistic)); }, store);
                                                    ++insertIdx;
                                                }
                                                else
                                                {
                                                    const auto& statisticId = lookupIds[lookupIdx % lookupCount];
                                                    const auto startTs = lookupStartTs[lookupIdx % lookupCount];
                                                    auto result = std::visit(
                                                        [&](auto& s)
                                                        {
                                                            return s.getStatistics(
                                                                statisticId,
                                                                Windowing::TimeMeasure{startTs},
                                                                Windowing::TimeMeasure{startTs + queryRangeTs});
                                                        },
                                                        store);
                                                    ++lookupIdx;
                                                }
                                            }
                                        });

                                    csv << "InsertAndGetStatistics," << magic_enum::enum_name(storeType) << "," << numThreads << ","
                                        << numStatistics << "," << numStatisticIds << "," << statisticSize << "," << windowSize << ",-1,"
                                        << numStatisticsPerRequest << "," << pctInsert << "," << pctPrePopulate << "," << RNG_SEED << ","
                                        << rep << "," << durationMs << "\n"
                                        << std::flush;
                                }

                                progress.report(
                                    "Mixed  | " + padLeft(magic_enum::enum_name(storeType), 10) + " | " + pad("threads", numThreads, 2)
                                    + " | " + pad("stats", numStatistics, 7) + " | " + pad("ids", numStatisticIds, 7) + " | "
                                    + pad("size", statisticSize, 5) + " | " + pad("ws", windowSize, 5) + " | "
                                    + pad("pctPrePop", pctPrePopulate, 2) + " | " + pad("pctInsert", pctInsert, 2) + " | "
                                    + pad("statsPerReq", numStatisticsPerRequest, 3));
                            },
                            Params::storeTypes,
                            Params::pctInsertVals,
                            Params::threadCounts,
                            Params::numStatisticsPerRequestVals);
                    }
                }
            },
            Params::numStatisticIdsVals,
            Params::numStatisticsVals);
    }
}

/// ============================================================

struct BenchmarkSelection
{
    bool runInsert = false;
    bool runGet = false;
    bool runMixed = false;
};

/// Parses --benchmarks insert,get,mixed.
/// Valid tokens: insert, get, mixed.  Default (no flag): run all three.
/// Prints usage and exits with 0 for --help/-h.
/// Prints an error and exits with 1 for unrecognized arguments or invalid values.
BenchmarkSelection parseBenchmarkSelection(int argc, char* argv[])
{
    auto printUsage = [&]()
    {
        std::cout << "Usage: " << argv[0] << " [--benchmarks <list>]\n"
                  << "\n"
                  << "Options:\n"
                  << "  --benchmarks <list>   Comma-separated list of benchmarks to run.\n"
                  << "                        Valid values: insert,get,mixed\n"
                  << "                        Default: all three\n"
                  << "  --help, -h            Print this help message and exit\n";
    };

    BenchmarkSelection sel;

    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg{argv[i]};

        if (arg == "--help" || arg == "-h")
        {
            printUsage();
            std::exit(0);
        }

        std::string_view value;
        if (arg == "--benchmarks")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "Error: --benchmarks requires a value\n";
                printUsage();
                std::exit(1);
            }
            value = argv[++i];
        }
        else
        {
            std::cerr << "Error: unrecognized argument '" << arg << "'\n";
            printUsage();
            std::exit(1);
        }

        std::string valueStr{value};
        std::istringstream ss{valueStr};
        std::string token;
        while (std::getline(ss, token, ','))
        {
            if (token == "insert")
            {
                sel.runInsert = true;
            }
            else if (token == "get")
            {
                sel.runGet = true;
            }
            else if (token == "mixed")
            {
                sel.runMixed = true;
            }
            else
            {
                std::cerr << "Error: unknown benchmark '" << token << "'. Valid values: insert, get, mixed\n";
                std::exit(1);
            }
        }
    }

    /// Default: run all benchmarks
    if (!sel.runInsert && !sel.runGet && !sel.runMixed)
    {
        sel.runInsert = sel.runGet = sel.runMixed = true;
    }

    return sel;
}

void runBenchmarks(int argc, char* argv[])
{
    const auto [runInsert, runGet, runMixed] = parseBenchmarkSelection(argc, argv);

    std::ofstream csv{std::string{BENCHMARK_CSV}};
    csv << "benchmark,store_type,num_threads,num_statistics,num_statistic_ids,statistic_size,window_size,"
        << "pct_access_existing,num_statistics_per_request,pct_insert,pct_pre_populate,random_seed,repetition,duration_ms\n";

    const uint64_t noInsertConfigs = runInsert ? InsertParams::validateAndCount() : 0;
    const uint64_t noGetConfigs = runGet ? GetParams::validateAndCount() : 0;
    const uint64_t noMixedConfigs = runMixed ? MixedParams::validateAndCount() : 0;

    ProgressTracker progress{noInsertConfigs + noGetConfigs + noMixedConfigs};

    std::cout << "=== StatisticStore Custom Benchmark ===\n";
    std::cout << "Start time: " << std::chrono::system_clock::now() << "\n";
    std::cout << "Total configs: " << (noInsertConfigs + noGetConfigs + noMixedConfigs) << " (" << NUM_REPS << " reps each)\n";
    std::cout << "Random seed: " << RNG_SEED << "\n\n";

    auto benchStart = std::chrono::steady_clock::now();

    if (runInsert)
    {
        std::cout << "--- InsertStatistic (" << noInsertConfigs << " configs) ---\n";
        progress.beginSection(noInsertConfigs);
        runInsertStatisticBenchmark(csv, progress);
        progress.finalizeProgressBar();
        std::cout << "--- InsertStatistic finished in "
                  << formatHMS(std::chrono::duration<double>(std::chrono::steady_clock::now() - benchStart).count()) << " ---\n";
        benchStart = std::chrono::steady_clock::now();
    }

    if (runGet)
    {
        std::cout << "\n--- GetStatistics (" << noGetConfigs << " configs) ---\n";
        progress.beginSection(noGetConfigs);
        runGetStatisticsBenchmark(csv, progress);
        progress.finalizeProgressBar();
        std::cout << "--- GetStatistics finished in "
                  << formatHMS(std::chrono::duration<double>(std::chrono::steady_clock::now() - benchStart).count()) << " ---\n";
        benchStart = std::chrono::steady_clock::now();
    }

    if (runMixed)
    {
        std::cout << "\n--- InsertAndGetStatistics (" << noMixedConfigs << " configs) ---\n";
        progress.beginSection(noMixedConfigs);
        runInsertAndGetBenchmark(csv, progress);
        progress.finalizeProgressBar();
        std::cout << "--- InsertAndGetStatistics finished in "
                  << formatHMS(std::chrono::duration<double>(std::chrono::steady_clock::now() - benchStart).count()) << " ---\n";
    }

    std::cout << "\nBenchmarks complete in " << formatHMS(progress.getElapsedSeconds()) << ". Results written to "
              << std::filesystem::absolute(BENCHMARK_CSV).string() << "\n";

    progress.printSkipped();
}
}
}

int main(int argc, char* argv[])
{
    NES::runBenchmarks(argc, argv);
    return 0;
}
