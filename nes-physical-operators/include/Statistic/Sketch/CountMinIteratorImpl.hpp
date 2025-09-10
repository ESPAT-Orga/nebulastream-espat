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

struct CountMinProviderArguments final : StatisticProviderArguments
{
    DataType counterDataType;

    explicit CountMinProviderArguments(DataType counterDataType) : counterDataType(std::move(counterDataType)) { }
};

/// |       ------ Meta Data ------        |       --- Statistics Area ---       |
/// | No. Rows (64bit) No. Columns (64it)  |    Count Min 2-D Array[rows][col]   |
class CountMinIteratorImpl final : public StatisticProviderIteratorImpl
{
public:
    explicit CountMinIteratorImpl(const nautilus::val<int8_t*>& statisticMemArea, CountMinProviderArguments& countMinProviderArguments);
    ~CountMinIteratorImpl() override = default;
    Nautilus::Record operator*() const override;
    StatisticProviderIteratorImpl& operator++() override;
    nautilus::val<bool> operator==(const StatisticProviderIteratorImpl& other) const override;

protected:
    void advanceToBegin() override;
    void advanceToEnd() override;

private:
    /// Provided via the constructor
    DataType counterDataType;
    std::string columnFieldName;
    std::string rowFieldName;
    std::string counterFieldName;

    /// Set by each statistic
    nautilus::val<uint64_t> numberOfRows;
    nautilus::val<uint64_t> numberOfColumns;
    nautilus::val<uint64_t> counterRow;
    nautilus::val<uint64_t> counterCol;
    nautilus::val<int8_t*> curCounter;
};

}
