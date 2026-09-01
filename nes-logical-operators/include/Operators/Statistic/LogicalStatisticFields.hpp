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

#include <string>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <fmt/format.h>

namespace NES
{

/// Per-statistic payload field name, derived purely from the statisticId so that the aggregation function
/// producing the synopsis and the StatisticStoreWriter consuming it can never drift apart -- even after plan
/// (de)serialization, where only the id round-trips.
inline Identifier statisticDataFieldName(const StatisticId statisticId)
{
    return Identifier::parse(fmt::format("STATISTICDATA_{}", statisticId.getRawValue()));
}

/// The field names every statistic build chain agrees on. Held as unbound fields: these are *declarations*,
/// written down before any operator exists to produce them, which is exactly what UnqualifiedUnboundField is
/// for. Schema inference binds them to their producing operator on the way out of getOutputSchema().
///
/// Note there is deliberately no qualifier API here. Disambiguation used to be a "SOURCE$" string prefix
/// mutated onto the field name; fields now carry their producing operator, so two same-named fields from
/// different operators are already distinct and nothing needs to be renamed.
///
/// DataType::Type overloads resolve to NOT_NULLABLE (DataTypeProvider.cpp:55-58), which the
/// StatisticStoreWriter requires of every field it reads.
class LogicalStatisticFields
{
public:
    UnqualifiedUnboundField statisticNumberOfSeenTuplesField{
        Identifier::parse(std::string{StatisticFieldNames::NUMBER_OF_SEEN_TUPLES}), DataType::Type::UINT64};
    UnqualifiedUnboundField statisticIdField{Identifier::parse(std::string{StatisticFieldNames::STATISTIC_ID}), DataType::Type::UINT64};
    UnqualifiedUnboundField statisticStartTsField{Identifier::parse(std::string{StatisticFieldNames::START_TS}), DataType::Type::UINT64};
    UnqualifiedUnboundField statisticEndTsField{Identifier::parse(std::string{StatisticFieldNames::END_TS}), DataType::Type::UINT64};
    UnqualifiedUnboundField statisticDataField{Identifier::parse(std::string{StatisticFieldNames::DATA}), DataType::Type::VARSIZED};
    UnqualifiedUnboundField statisticTypeField{Identifier::parse(std::string{StatisticFieldNames::TYPE}), DataType::Type::UINT64};

    LogicalStatisticFields() = default;
    bool operator==(const LogicalStatisticFields&) const = default;
};

}
