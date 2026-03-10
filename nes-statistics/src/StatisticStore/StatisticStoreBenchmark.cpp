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

#include <random>
#include <variant>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <StatisticStore/SubStoresStatisticStore.hpp>
#include <StatisticStore/WindowStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <benchmark/benchmark.h>
#include <magic_enum/magic_enum.hpp>
#include <Statistic.hpp>

namespace NES
{
namespace
{

/// Output CSV filename
constexpr std::string_view BENCHMARK_CSV = "statistic_store_benchmark.csv";

/// Seed for all RNGs to ensure reproducible benchmarks
constexpr uint32_t RNG_SEED = 42;

Statistic createDummyStatistic(
    const Statistic::StatisticId statisticId,
    Windowing::TimeMeasure startTs,
    Windowing::TimeMeasure endTs,
    std::mt19937& gen,
    int statisticSize)
{
    /// Picking a random statistic type value
    constexpr auto statisticTypes = magic_enum::enum_values<Statistic::StatisticType>();
    std::uniform_int_distribution<> enumDistribution{0, static_cast<int>(magic_enum::enum_count<Statistic::StatisticType>()) - 1};
    const auto randomStatisticType = statisticTypes[enumDistribution(gen)];

    /// Generating random statistic data
    std::vector<int8_t> statisticData(statisticSize);
    std::uniform_int_distribution<> byteDistribution{0, 255};
    for (int i = 0; i < statisticSize; ++i)
    {
        statisticData[i] = byteDistribution(gen);
    }

    /// Randomize numberOfSeenTuples independently of the statistic data size
    std::uniform_int_distribution<uint64_t> seenTuplesDist{1, 10000};
    const auto numberOfSeenTuples = seenTuplesDist(gen);

    return {
        statisticId, randomStatisticType, startTs, endTs, numberOfSeenTuples, statisticData.data(), static_cast<uint64_t>(statisticSize)};
}

using StatisticStoreVariant = std::variant<DefaultStatisticStore, WindowStatisticStore, SubStoresStatisticStore>;

/// Creates a statistic store variant based on the StatisticStoreType enum
StatisticStoreVariant createStore(StatisticStoreType storeType, int numThreads, uint64_t windowSize)
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
    return DefaultStatisticStore{};
}

/// Benchmark insertStatistic throughput
/// Args: {storeType, windowSize, numberOfStatisticIds, numThreads, numStatistics, statisticSize}
static void BM_InsertStatistic(benchmark::State& state)
{
    const auto storeType = static_cast<StatisticStoreType>(state.range(0));
    const auto windowSize = static_cast<uint64_t>(state.range(1));
    const auto numberOfStatisticIds = static_cast<int>(state.range(2));
    const auto numThreads = static_cast<int>(state.range(3));
    const auto numStatistics = static_cast<int>(state.range(4));
    const auto statisticSize = static_cast<int>(state.range(5));

    auto store = createStore(storeType, numThreads, windowSize);

    /// Pre-create all statistics before the benchmark loop
    std::mt19937 rng{RNG_SEED};
    std::uniform_int_distribution<Statistic::StatisticId> idDist{0, static_cast<uint64_t>(numberOfStatisticIds - 1)};

    struct PreparedStatistic
    {
        Statistic statistic;
        Statistic::StatisticHash hash;
    };

    std::vector<PreparedStatistic> preparedStatistics;
    preparedStatistics.reserve(numStatistics);
    uint64_t curTs = 0;
    for (int i = 0; i < numStatistics; ++i)
    {
        const Statistic::StatisticId statisticId = idDist(rng);
        const Windowing::TimeMeasure startTs{curTs};
        const Windowing::TimeMeasure endTs{curTs + windowSize};
        auto statistic = createDummyStatistic(statisticId, startTs, endTs, rng, statisticSize);
        const auto hash = statistic.getHash();
        preparedStatistics.push_back({std::move(statistic), hash});
        curTs += windowSize;
    }

    /// Benchmark loop: iterate over pre-created statistics
    int idx = 0;
    for (auto _ : state)
    {
        auto& prepared = preparedStatistics[idx % numStatistics];
        /// Copy the statistic since insertStatistic takes by move
        auto statisticCopy = prepared.statistic;
        std::visit([&](auto& s) { s.insertStatistic(prepared.hash, std::move(statisticCopy)); }, store);
        ++idx;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetLabel(std::string{magic_enum::enum_name(storeType)});
}

/// Benchmark getStatistics latency
/// Args: {storeType, windowSize, numberOfStatisticIds, pctAccessExisting, numStatistics, statisticSize}
static void BM_GetStatistics(benchmark::State& state)
{
    const auto storeType = static_cast<StatisticStoreType>(state.range(0));
    const auto windowSize = static_cast<uint64_t>(state.range(1));
    const auto numberOfStatisticIds = static_cast<int>(state.range(2));
    const auto pctAccessExisting = static_cast<int>(state.range(3));
    const auto numStatistics = static_cast<int>(state.range(4));
    const auto statisticSize = static_cast<int>(state.range(5));

    auto store = createStore(storeType, 1, windowSize);

    /// Pre-populate the store and save the hashes of inserted statistics
    std::mt19937 gen{RNG_SEED};
    std::vector<Statistic::StatisticHash> insertedHashes;
    for (int id = 0; id < numberOfStatisticIds; ++id)
    {
        uint64_t curTs = 0;
        for (int i = 0; i < numStatistics; ++i)
        {
            const Windowing::TimeMeasure startTs{curTs};
            const Windowing::TimeMeasure endTs{curTs + windowSize};
            auto statistic = createDummyStatistic(id, startTs, endTs, gen, statisticSize);
            const auto hash = statistic.getHash();
            insertedHashes.push_back(hash);
            std::visit([&](auto& s) { s.insertStatistic(hash, std::move(statistic)); }, store);
            curTs += windowSize;
        }
    }

    /// Create hashes for non-existing statistics (use IDs beyond the inserted range)
    std::vector<Statistic::StatisticHash> nonExistingHashes;
    for (int id = numberOfStatisticIds; id < numberOfStatisticIds + 10; ++id)
    {
        auto dummyStatistic = createDummyStatistic(id, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{windowSize}, gen, statisticSize);
        nonExistingHashes.push_back(dummyStatistic.getHash());
    }

    /// Benchmark retrieving statistics, mixing existing and non-existing lookups
    const uint64_t maxTs = numStatistics * windowSize;
    std::uniform_int_distribution<> existingDist{0, static_cast<int>(insertedHashes.size()) - 1};
    std::uniform_int_distribution<> nonExistingDist{0, static_cast<int>(nonExistingHashes.size()) - 1};
    std::uniform_int_distribution<> pctDist{0, 99};
    for (auto _ : state)
    {
        Statistic::StatisticHash hash;
        if (pctDist(gen) < pctAccessExisting)
        {
            hash = insertedHashes[existingDist(gen)];
        }
        else
        {
            hash = nonExistingHashes[nonExistingDist(gen)];
        }

        try
        {
            auto result = std::visit(
                [&](auto& s) { return s.getStatistics(hash, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{maxTs}); }, store);
            benchmark::DoNotOptimize(result);
        }
        catch (const std::out_of_range&)
        {
            /// Expected for non-existing statistic lookups
        }
    }

    state.SetLabel(std::string{magic_enum::enum_name(storeType)});
}

/// Helper to get the integer value for a StatisticStoreType for use in benchmark Args
constexpr auto DEFAULT = static_cast<int>(StatisticStoreType::DEFAULT);
constexpr auto WINDOW = static_cast<int>(StatisticStoreType::WINDOW);
constexpr auto SUB_STORES = static_cast<int>(StatisticStoreType::SUB_STORES);

/// Register benchmarks for all three store types with varying parameters
/// Args: {storeType, windowSize, numberOfStatisticIds, numThreads, numStatistics, statisticSize}
/// Run with: --benchmark_out=insert_statistic_benchmark.csv --benchmark_out_format=csv
BENCHMARK(BM_InsertStatistic)
    ->Args({DEFAULT, 10, 1, 1, 100, 1024})
    ->Args({DEFAULT, 10, 10, 1, 100, 1024})
    ->Args({DEFAULT, 10, 10, 1, 1000, 1024})
    ->Args({DEFAULT, 10, 10, 1, 100, 100 * 1024})
    ->Args({DEFAULT, 100, 1, 1, 100, 1024})
    ->Args({DEFAULT, 100, 10, 1, 100, 1024})
    ->Args({DEFAULT, 1000, 1, 1, 100, 1024})
    ->Args({DEFAULT, 1000, 10, 1, 100, 1024})
    ->Args({WINDOW, 10, 1, 1, 100, 1024})
    ->Args({WINDOW, 10, 10, 1, 100, 1024})
    ->Args({WINDOW, 10, 10, 1, 1000, 1024})
    ->Args({WINDOW, 10, 10, 1, 100, 100 * 1024})
    ->Args({WINDOW, 100, 1, 1, 100, 1024})
    ->Args({WINDOW, 100, 10, 1, 100, 1024})
    ->Args({WINDOW, 1000, 1, 1, 100, 1024})
    ->Args({WINDOW, 1000, 10, 1, 100, 1024})
    ->Args({SUB_STORES, 10, 1, 1, 100, 1024})
    ->Args({SUB_STORES, 10, 10, 1, 100, 1024})
    ->Args({SUB_STORES, 10, 10, 1, 1000, 1024})
    ->Args({SUB_STORES, 10, 10, 1, 100, 100 * 1024})
    ->Args({SUB_STORES, 100, 1, 1, 100, 1024})
    ->Args({SUB_STORES, 100, 10, 1, 100, 1024})
    ->Args({SUB_STORES, 1000, 1, 1, 100, 1024})
    ->Args({SUB_STORES, 1000, 10, 1, 100, 1024});

/// Args: {storeType, windowSize, numberOfStatisticIds, pctAccessExisting, numStatistics, statisticSize}
/// Run with: --benchmark_out=get_statistics_benchmark.csv --benchmark_out_format=csv
BENCHMARK(BM_GetStatistics)
    ->Args({DEFAULT, 10, 1, 100, 100, 1024})
    ->Args({DEFAULT, 10, 10, 100, 100, 1024})
    ->Args({DEFAULT, 10, 10, 50, 100, 1024})
    ->Args({DEFAULT, 100, 1, 100, 100, 1024})
    ->Args({DEFAULT, 100, 10, 100, 100, 1024})
    ->Args({DEFAULT, 1000, 1, 100, 100, 1024})
    ->Args({DEFAULT, 1000, 10, 100, 100, 1024})
    ->Args({WINDOW, 10, 1, 100, 100, 1024})
    ->Args({WINDOW, 10, 10, 100, 100, 1024})
    ->Args({WINDOW, 10, 10, 50, 100, 1024})
    ->Args({WINDOW, 100, 1, 100, 100, 1024})
    ->Args({WINDOW, 100, 10, 100, 100, 1024})
    ->Args({WINDOW, 1000, 1, 100, 100, 1024})
    ->Args({WINDOW, 1000, 10, 100, 100, 1024})
    ->Args({SUB_STORES, 10, 1, 100, 100, 1024})
    ->Args({SUB_STORES, 10, 10, 100, 100, 1024})
    ->Args({SUB_STORES, 10, 10, 50, 100, 1024})
    ->Args({SUB_STORES, 100, 1, 100, 100, 1024})
    ->Args({SUB_STORES, 100, 10, 100, 100, 1024})
    ->Args({SUB_STORES, 1000, 1, 100, 100, 1024})
    ->Args({SUB_STORES, 1000, 10, 100, 100, 1024});

}
}

int main(int argc, char** argv)
{
    std::vector<char*> args(argv, argv + argc);
    auto outArg = std::string{"--benchmark_out="}.append(NES::BENCHMARK_CSV);
    std::string fmtArg = "--benchmark_out_format=csv";
    args.push_back(outArg.data());
    args.push_back(fmtArg.data());
    argc = static_cast<int>(args.size());
    argv = args.data();

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
