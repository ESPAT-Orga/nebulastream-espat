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
    {
    }

    ~EquiWidthHistogramProviderArguments() override = default;
};

/// | ------ Meta Data ------  |    --- Statistics Area ---     |
/// | Number of Bins (64bit)  |  --- Bins in the sample, c.f. Bin ---  |
class EquiWidthHistogramIteratorImpl : public StatisticProviderIteratorImpl
{
public:
    explicit EquiWidthHistogramIteratorImpl(
        const nautilus::val<int8_t*>& statisticMemArea, EquiWidthHistogramProviderArguments& equiWidthHistogramArguments);
    ~EquiWidthHistogramIteratorImpl() override = default;
    Nautilus::Record operator*() const override;
    StatisticProviderIteratorImpl& operator++() override;
    nautilus::val<bool> operator==(const StatisticProviderIteratorImpl& other) const override;

protected:
    void advanceToBegin() override;
    void advanceToEnd() override;

private:
    /// Provided via the constructor
    std::string binStartFieldName;
    std::string binEndFieldName;
    std::string binCounterFieldName;
    DataType dataTypeCounter;
    DataType dataTypeStartEnd;
    uint64_t sizeOfDataType;

    /// Set by each statistic
    nautilus::val<int8_t*> binMemRef;
    nautilus::val<uint64_t> numberOfBins;
    nautilus::val<int8_t*> histogramData;
};
}
