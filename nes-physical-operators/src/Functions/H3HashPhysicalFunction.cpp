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

#include <Functions/H3HashPhysicalFunction.hpp>

#include <cstdint>
#include <random>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

namespace
{

/// Recompute the column index that CountMinSketch's build pipeline would have
/// targeted for *value* in row *rowIndex*.
///
/// Mirrors CountMinSketchPhysicalFunction::reset (mt19937(seed) -> bytes) +
/// H3HashFunction::calculate (XOR-fold seeds for each set bit). Constants
/// match HashFunction::HashValue::raw_type = uint64_t (sizeOfSingleSeed = 8,
/// numberOfBitsInKey = 64).
uint64_t
computeH3Column(uint64_t value, uint64_t rowIndex, [[maybe_unused]] uint64_t numberOfRows, uint64_t numberOfCols, uint64_t seed)
{
    constexpr uint64_t sizeOfSingleSeed = sizeof(HashFunction::HashValue::raw_type);
    constexpr uint64_t numberOfBitsInKey = sizeof(HashFunction::HashValue::raw_type) * 8;

    PRECONDITION(rowIndex < numberOfRows, "rowIndex {} out of range for numberOfRows {}", rowIndex, numberOfRows);
    PRECONDITION(numberOfCols > 0, "numberOfCols must be > 0");

    /// CountMinSketchPhysicalFunction lays out the seeds with a per-row stride
    /// of *sizeOfSingleSeed* bytes (not numberOfBitsInKey * sizeOfSingleSeed),
    /// so consecutive rows share most of their seed bytes.
    /// Row R reads bytes [R*8, R*8 + 8*64).
    const uint64_t startByte = rowIndex * sizeOfSingleSeed;

    std::mt19937 gen(seed);
    std::uniform_int_distribution<> dis(0, 255);

    /// Skip bytes before this row's window.
    for (uint64_t i = 0; i < startByte; ++i)
    {
        (void)dis(gen);
    }

    /// Read this row's seeds: numberOfBitsInKey seeds, each sizeOfSingleSeed
    /// bytes, packed little-endian (matching how the build pipeline reads them
    /// via readValueFromMemRef<uint64_t>).
    uint64_t rowSeeds[numberOfBitsInKey];
    for (uint64_t bitIdx = 0; bitIdx < numberOfBitsInKey; ++bitIdx)
    {
        uint64_t seedVal = 0;
        for (uint64_t b = 0; b < sizeOfSingleSeed; ++b)
        {
            const auto byte = static_cast<uint64_t>(static_cast<unsigned char>(dis(gen)));
            seedVal |= byte << (b * 8);
        }
        rowSeeds[bitIdx] = seedVal;
    }

    /// H3 hash: XOR seeds for each set bit of value (LSB first, matching
    /// H3HashFunction::calculate which iterates keyBit 0..numberOfBitsInKey).
    uint64_t hash = 0;
    uint64_t op = value;
    for (uint64_t bitIdx = 0; bitIdx < numberOfBitsInKey; ++bitIdx)
    {
        if ((op & 1ULL) == 1)
        {
            hash ^= rowSeeds[bitIdx];
        }
        op >>= 1;
    }
    return hash % numberOfCols;
}

}

H3HashPhysicalFunction::H3HashPhysicalFunction(
    PhysicalFunction valueFunction,
    PhysicalFunction rowIndexFunction,
    PhysicalFunction numberOfRowsFunction,
    PhysicalFunction numberOfColsFunction,
    PhysicalFunction seedFunction)
    : valueFunction(std::move(valueFunction))
    , rowIndexFunction(std::move(rowIndexFunction))
    , numberOfRowsFunction(std::move(numberOfRowsFunction))
    , numberOfColsFunction(std::move(numberOfColsFunction))
    , seedFunction(std::move(seedFunction))
{
}

VarVal H3HashPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto valueRaw = valueFunction.execute(record, arena).getRawValueAs<nautilus::val<uint64_t>>();
    const auto rowIndexRaw = rowIndexFunction.execute(record, arena).getRawValueAs<nautilus::val<uint64_t>>();
    const auto numberOfRowsRaw = numberOfRowsFunction.execute(record, arena).getRawValueAs<nautilus::val<uint64_t>>();
    const auto numberOfColsRaw = numberOfColsFunction.execute(record, arena).getRawValueAs<nautilus::val<uint64_t>>();
    const auto seedRaw = seedFunction.execute(record, arena).getRawValueAs<nautilus::val<uint64_t>>();

    const auto column = nautilus::invoke(computeH3Column, valueRaw, rowIndexRaw, numberOfRowsRaw, numberOfColsRaw, seedRaw);
    return VarVal{column};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterH3_HASHPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 5, "H3_HASH requires exactly 5 child functions, got {}", args.childFunctions.size());
    return H3HashPhysicalFunction(
        args.childFunctions[0], args.childFunctions[1], args.childFunctions[2], args.childFunctions[3], args.childFunctions[4]);
}

}
