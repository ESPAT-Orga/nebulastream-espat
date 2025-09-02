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

#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Statistic/StatisticProvider.hpp>

namespace NES
{

struct ReservoirSampleProviderArguments final : StatisticProviderArguments
{
    std::shared_ptr<MemoryLayout> memoryLayout;

    explicit ReservoirSampleProviderArguments(std::shared_ptr<MemoryLayout> memoryLayout) : memoryLayout(std::move(memoryLayout)) { }

    ~ReservoirSampleProviderArguments() override = default;

    std::unique_ptr<StatisticProviderArguments> clone() override
    {
        /// This is copied from LowerToPhysicalReservoirProbe. We need the clone method as we need StatisticProvider to be copyable.
        switch (memoryLayout->getSchema().memoryLayoutType)
        {
            case Schema::MemoryLayoutType::ROW_LAYOUT: {
                /// For a row layout, we do not care about the overall memory area size
                std::unique_ptr<MemoryLayout> newMemoryLayout = std::make_unique<RowLayout>(0, memoryLayout->getSchema());
                return std::make_unique<ReservoirSampleProviderArguments>(std::move(newMemoryLayout));
                break;
            }
            case Schema::MemoryLayoutType::COLUMNAR_LAYOUT:
                throw NotImplemented(
                    "Not possible currently to use a columnar layout, as we do not know the size of the sample at this moment.");
        }
        std::unreachable();
    }
};

/// | ------ Meta-Data ------  |    --- Statistics Area ---     |
/// | Number of Tuples (64bit) |  --- Tuples in the sample ---  |
class ReservoirSampleIteratorImpl final : public StatisticProviderIteratorImpl
{
public:
    explicit ReservoirSampleIteratorImpl(
        const nautilus::val<int8_t*>& statisticMemArea, const ReservoirSampleProviderArguments& reservoirSampleArguments);
    ~ReservoirSampleIteratorImpl() override = default;
    Record operator*() override;
    StatisticProviderIteratorImpl& operator++() override;
    nautilus::val<bool> operator==(const StatisticProviderIteratorImpl& other) const override;

protected:
    void advanceToBegin() override;
    void advanceToEnd() override;

private:
    /// Provided via the constructor. Needs to be a shared_ptr as we call StatisticProvider::begin() multiple times and StatisticProvider::begin()
    std::shared_ptr<MemoryLayout> memoryLayout;

    /// Set by each statistic
    nautilus::val<uint64_t> sampleSize;
    nautilus::val<uint64_t> recordPos;
    nautilus::val<int8_t*> nextTupleMem;
};

/// A nautilus wrapper for the header of a reservoir sample
class ReservoirSampleHeaderRef
{
    nautilus::val<uint32_t> metaDataSize;
    nautilus::val<uint32_t> sizeOfMetaDataSize;
    nautilus::val<uint32_t> sizeOfTotalSize;
    nautilus::val<int8_t*> statisticMemArea;
    nautilus::val<uint64_t> sampleSize;
    nautilus::val<uint64_t> totalSize;

public:
    ReservoirSampleHeaderRef()
        : metaDataSize(8), sizeOfMetaDataSize(4), sizeOfTotalSize(4), statisticMemArea(nullptr), sampleSize(0), totalSize(0)
    {
    }

    void setMemArea(const nautilus::val<int8_t*>& statisticMemArea);
    void setSampleSize(const nautilus::val<uint64_t>& sampleSize);

    /// Writes the current header information to the header at statisticMemArea
    void writeMetaData();

    nautilus::val<uint64_t> getTotalSize(const nautilus::val<uint64_t>& sampleDataSize);
    nautilus::val<uint32_t> readMetaDataSize();
    nautilus::val<uint64_t> readSampleSize();
    nautilus::val<int8_t*> getSampleMemArea();
    nautilus::val<uint64_t> readTotalSize();
};

}
