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
#include <Statistic/StatisticProvider.hpp>

namespace NES
{


struct EquiWidthHistogramProviderArguments : StatisticProviderArguments
{
    std::string binStartFieldName;
    std::string binEndFieldName;
    std::string binCounterFieldName;
    DataType dataTypeCounter;
    DataType dataTypeStartEnd;
    uint64_t sizeOfDataType;

    EquiWidthHistogramProviderArguments(
        std::string binStartFieldName,
        std::string binEndFieldName,
        std::string binCounterFieldName,
        DataType dataTypeCounter,
        DataType dataTypeStartEnd)
        : binStartFieldName(std::move(binStartFieldName))
        , binEndFieldName(std::move(binEndFieldName))
        , binCounterFieldName(std::move(binCounterFieldName))
        , dataTypeCounter(std::move(dataTypeCounter))
        , dataTypeStartEnd(std::move(dataTypeStartEnd))
        , sizeOfDataType(dataTypeCounter.getSizeInBytes() + dataTypeStartEnd.getSizeInBytes() * 2)
    {
    }

    friend bool operator==(const EquiWidthHistogramProviderArguments& lhs, const EquiWidthHistogramProviderArguments& rhs)
    {
        return lhs == static_cast<const StatisticProviderArguments&>(rhs) && lhs.binStartFieldName == rhs.binStartFieldName
            && lhs.binEndFieldName == rhs.binEndFieldName && lhs.binCounterFieldName == rhs.binCounterFieldName
            && lhs.dataTypeCounter == rhs.dataTypeCounter && lhs.dataTypeStartEnd == rhs.dataTypeStartEnd
            && lhs.sizeOfDataType == rhs.sizeOfDataType;
    }

    friend bool operator!=(const EquiWidthHistogramProviderArguments& lhs, const EquiWidthHistogramProviderArguments& rhs)
    {
        return !(lhs == rhs);
    }

    ~EquiWidthHistogramProviderArguments() override = default;

    std::unique_ptr<StatisticProviderArguments> clone() override { return std::make_unique<EquiWidthHistogramProviderArguments>(*this); }
};

/// | ------ Meta-Data ------  |    --- Statistics Area ---     |
/// | Number of Bins (64bit)  |  --- Bins in the sample, c.f. Bin ---  |
class EquiWidthHistogramIteratorImpl final : public StatisticProviderIteratorImpl
{
public:
    explicit EquiWidthHistogramIteratorImpl(
        const nautilus::val<int8_t*>& statisticMemArea, EquiWidthHistogramProviderArguments equiWidthHistogramArguments);
    ~EquiWidthHistogramIteratorImpl() override = default;
    Record operator*() override;
    StatisticProviderIteratorImpl& operator++() override;
    nautilus::val<bool> operator==(const StatisticProviderIteratorImpl& other) const override;

protected:
    void advanceToBegin() override;
    void advanceToEnd() override;

private:
    /// Provided via the constructor
    EquiWidthHistogramProviderArguments equiWidthHistogramArgs;

    /// Set by each statistic
    nautilus::val<int8_t*> binMemRef;
    nautilus::val<uint64_t> numberOfBins;
    nautilus::val<int8_t*> histogramData;
};
}
