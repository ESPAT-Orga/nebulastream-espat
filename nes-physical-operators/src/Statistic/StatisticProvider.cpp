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
#include <Statistic/StatisticProvider.hpp>

#include <Statistic/Histogram/EquiWidthHistogramIteratorImpl.hpp>
#include <Statistic/Sample/ReservoirSampleIteratorImpl.hpp>
#include <Statistic/Sketch/CountMinIteratorImpl.hpp>

namespace NES
{
StatisticProvider::StatisticProviderIterator::StatisticProviderIterator(StatisticProviderIterator&& statisticProviderIterator) noexcept
    : iteratorImpl(std::move(statisticProviderIterator.iteratorImpl))
{
}

StatisticProvider::StatisticProviderIterator::StatisticProviderIterator(std::unique_ptr<StatisticProviderIteratorImpl> iteratorImpl)
    : iteratorImpl(std::move(iteratorImpl))
{
}

Nautilus::Record StatisticProvider::StatisticProviderIterator::operator*() const
{
    return iteratorImpl->operator*();
}

StatisticProvider::StatisticProviderIterator& StatisticProvider::StatisticProviderIterator::operator++()
{
    iteratorImpl->operator++();
    return *this;
}

nautilus::val<bool> StatisticProvider::StatisticProviderIterator::operator==(const StatisticProviderIterator& other) const
{
    return iteratorImpl->operator==(*other.iteratorImpl);
}

nautilus::val<bool> StatisticProvider::StatisticProviderIterator::operator!=(const StatisticProviderIterator& other) const
{
    return not(*this == other);
}

void StatisticProvider::StatisticProviderIterator::advanceToBegin() const
{
    return iteratorImpl->advanceToBegin();
}

void StatisticProvider::StatisticProviderIterator::advanceToEnd() const
{
    return iteratorImpl->advanceToEnd();
}

StatisticProvider::StatisticProvider(
    const Statistic::StatisticType statisticType, std::unique_ptr<StatisticProviderArguments> statisticProviderArguments)
    : statisticType(statisticType), statisticProviderArguments(std::move(statisticProviderArguments))
{
}

StatisticProvider::StatisticProvider(StatisticProvider&& other) noexcept
    : statisticType(other.statisticType), statisticProviderArguments(std::move(other.statisticProviderArguments))
{
}

StatisticProvider::StatisticProvider(const StatisticProvider& other) noexcept
    : statisticType(other.statisticType), statisticProviderArguments(other.statisticProviderArguments->clone())
{
}

StatisticProvider& StatisticProvider::operator=(StatisticProvider&& other) noexcept
{
    statisticType = other.statisticType;
    statisticProviderArguments = std::move(other.statisticProviderArguments);
    return *this;
}

StatisticProvider& StatisticProvider::operator=(const StatisticProvider& other) noexcept
{
    statisticType = other.statisticType;
    statisticProviderArguments = other.statisticProviderArguments->clone();
    return *this;
}

StatisticProvider::StatisticProviderIterator StatisticProvider::begin(const nautilus::val<int8_t*>& statisticMemArea) const
{
    switch (statisticType)
    {
        case Statistic::StatisticType::Reservoir_Sample: {
            const auto reservoirSampleArguments = dynamic_cast<ReservoirSampleProviderArguments*>(statisticProviderArguments.get());
            INVARIANT(reservoirSampleArguments != nullptr, "ReservoirSampleProviderArguments is expected!");
            StatisticProviderIterator iterator{std::make_unique<ReservoirSampleIteratorImpl>(statisticMemArea, *reservoirSampleArguments)};
            iterator.advanceToBegin();
            return iterator;
        }
        case Statistic::StatisticType::Equi_Width_Histogram: {
            const auto equiWidthHistogramArguments = dynamic_cast<EquiWidthHistogramProviderArguments*>(statisticProviderArguments.get());
            INVARIANT(equiWidthHistogramArguments != nullptr, "EquiWidthHistogramProviderArguments is expected!");
            StatisticProviderIterator iterator{
                std::make_unique<EquiWidthHistogramIteratorImpl>(statisticMemArea, *equiWidthHistogramArguments)};
            iterator.advanceToBegin();
            return iterator;
        }
        case Statistic::StatisticType::Count_Min_Sketch: {
            const auto countMinArguments = dynamic_cast<CountMinProviderArguments*>(statisticProviderArguments.get());
            INVARIANT(countMinArguments != nullptr, "CountMinProviderArguments is expected!");
            StatisticProviderIterator iterator{std::make_unique<CountMinIteratorImpl>(statisticMemArea, *countMinArguments)};
            iterator.advanceToBegin();
            return iterator;
        }
    }

    std::unreachable();
}

StatisticProvider::StatisticProviderIterator StatisticProvider::end(const nautilus::val<int8_t*>& statisticMemArea) const
{
    switch (statisticType)
    {
        case Statistic::StatisticType::Reservoir_Sample: {
            const auto reservoirSampleArguments = dynamic_cast<ReservoirSampleProviderArguments*>(statisticProviderArguments.get());
            INVARIANT(reservoirSampleArguments != nullptr, "ReservoirSampleProviderArguments is expected!");
            StatisticProviderIterator iterator{std::make_unique<ReservoirSampleIteratorImpl>(statisticMemArea, *reservoirSampleArguments)};
            iterator.advanceToEnd();
            return iterator;
        }
        case Statistic::StatisticType::Equi_Width_Histogram: {
            const auto equiWidthHistogramArguments = dynamic_cast<EquiWidthHistogramProviderArguments*>(statisticProviderArguments.get());
            INVARIANT(equiWidthHistogramArguments != nullptr, "EquiWidthHistogramProviderArguments is expected!");
            StatisticProviderIterator iterator{
                std::make_unique<EquiWidthHistogramIteratorImpl>(statisticMemArea, *equiWidthHistogramArguments)};
            iterator.advanceToEnd();
            return iterator;
        }
        case Statistic::StatisticType::Count_Min_Sketch: {
            const auto countMinArguments = dynamic_cast<CountMinProviderArguments*>(statisticProviderArguments.get());
            INVARIANT(countMinArguments != nullptr, "CountMinProviderArguments is expected!");
            StatisticProviderIterator iterator{std::make_unique<CountMinIteratorImpl>(statisticMemArea, *countMinArguments)};
            iterator.advanceToEnd();
            return iterator;
        }
    }

    std::unreachable();
}

StatisticProviderIteratorImpl::StatisticProviderIteratorImpl(nautilus::val<int8_t*> statisticMemArea)
    : statisticMemArea(std::move(statisticMemArea))
{
}

}
