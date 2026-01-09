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
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>

namespace NES
{

/// Creates a count-min sketch via our aggregation function. It stores the count-min sketch as a 2-D array.
/// While computing, it also stores an array of seed values for each row that is derived from the seed.
class CountMinSketchPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    CountMinSketchPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::string_view numberOfSeenTuplesFieldName,
        DataType counterType,
        uint64_t numberOfCols,
        uint64_t numberOfRows,
        uint64_t seed);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~CountMinSketchPhysicalFunction() override = default;

private:
    std::string numberOfSeenTuplesFieldName;
    DataType counterType;
    uint64_t numberOfCols;
    uint64_t numberOfRows;
    /// Just the sketch, does not include seeds size!
    uint64_t totalSizeOfSketchInBytes;
    uint64_t numberOfBitsInKey;
    uint64_t sizeOfSingleSeed;
    uint64_t totalSizeOfSeeds;
    uint64_t seed;
    uint32_t loweredMetaDataSize{sizeof(numberOfCols) + sizeof(numberOfRows)};
};

}
