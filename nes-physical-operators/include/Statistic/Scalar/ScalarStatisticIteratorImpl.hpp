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

struct ScalarStatisticProviderArguments final : StatisticProviderArguments
{
    DataType valueDataType;
    Record::RecordFieldIdentifier valueFieldName;

    explicit ScalarStatisticProviderArguments(DataType valueDataType, Record::RecordFieldIdentifier valueFieldName)
        : valueDataType(std::move(valueDataType)), valueFieldName(std::move(valueFieldName))
    {
    }

    ~ScalarStatisticProviderArguments() override = default;

    std::unique_ptr<StatisticProviderArguments> clone() override { return std::make_unique<ScalarStatisticProviderArguments>(*this); }
};

/// |  --- Statistics Area ---  |
/// |   Value (valueDataType)   |
/// The scalar statistics (Count / Sum / Avg) persist a single value and carry no metadata, so unlike the synopsis
/// statistics they write no header: the value sits at offset 0 and its type is pinned by the probe. Serves all three
/// ops, which differ only in that type.
class ScalarStatisticIteratorImpl final : public StatisticProviderIteratorImpl
{
public:
    explicit ScalarStatisticIteratorImpl(
        const nautilus::val<int8_t*>& statisticMemArea, ScalarStatisticProviderArguments scalarProviderArguments);
    ~ScalarStatisticIteratorImpl() override = default;
    Record operator*() override;
    StatisticProviderIteratorImpl& operator++() override;
    nautilus::val<bool> operator==(const StatisticProviderIteratorImpl& other) const override;

protected:
    void advanceToBegin() override;
    void advanceToEnd() override;

private:
    /// Provided via the constructor
    ScalarStatisticProviderArguments scalarProviderArgs;

    /// A scalar statistic holds exactly one value, so the cursor is 0 at begin and 1 at end
    nautilus::val<uint64_t> index;
};

}
