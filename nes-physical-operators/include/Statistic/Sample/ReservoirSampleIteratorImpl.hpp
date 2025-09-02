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

#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Statistic/StatisticProvider.hpp>

namespace NES
{

struct ReservoirSampleProviderArguments final : StatisticProviderArguments
{
    std::unique_ptr<MemoryLayout> memoryLayout;

    explicit ReservoirSampleProviderArguments(std::unique_ptr<MemoryLayout> memoryLayout) : memoryLayout(std::move(memoryLayout)) { }

    ~ReservoirSampleProviderArguments() override = default;
};

/// | ------ Meta Data ------  |    --- Statistics Area ---     |
/// | Number of Tuples (64bit) |  --- Tuples in the sample ---  |
class ReservoirSampleIteratorImpl final : public StatisticProviderIteratorImpl
{
public:
    explicit ReservoirSampleIteratorImpl(
        const nautilus::val<int8_t*>& statisticMemArea, ReservoirSampleProviderArguments& reservoirSampleArguments);
    ~ReservoirSampleIteratorImpl() override = default;
    Nautilus::Record operator*() const override;
    StatisticProviderIteratorImpl& operator++() override;
    nautilus::val<bool> operator==(const StatisticProviderIteratorImpl& other) const override;

protected:
    void advanceToBegin() override;
    void advanceToEnd() override;

private:
    /// Provided via the constructor
    std::unique_ptr<MemoryLayout> memoryLayout;

    /// Set by each statistic
    nautilus::val<uint64_t> sampleSize;
    nautilus::val<uint64_t> recordPos;
    nautilus::val<int8_t*> sampleData;
};

}
