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
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <fmt/format.h>
#include <StatisticTuple.hpp>

namespace NES
{

/// A statistic to persist: its store id and its type. The per-synopsis VARSIZED data-field name is derived
/// purely from the id (see statisticDataFieldName), so the StatisticBuild operator and the StatisticStoreWriter
/// always agree on it -- even after plan (de)serialization, where only the id round-trips.
struct StatisticTarget
{
    StatisticTuple::StatisticId statisticId;
    StatisticTuple::StatisticType statisticType;
    bool operator==(const StatisticTarget&) const = default;
};

/// Per-synopsis VARSIZED data-field name, derived purely from the statisticId so producers and consumers
/// never drift. Upper case, since the SLT sink field-name parsing requires it.
inline std::string statisticDataFieldName(const StatisticTuple::StatisticId statisticId)
{
    return fmt::format("STATISTICDATA_{}", statisticId.getRawValue());
}

/// Simple (name, type) pair used across all statistic operator headers.
/// Intentionally independent of the NES Schema template hierarchy so that operator
/// headers can include it without pulling in heavy schema machinery.
struct StatisticField
{
    std::string name;
    DataType dataType;
    bool operator==(const StatisticField&) const = default;
};

/// Acts as an abstract class that every statistic build logical function should inherit from.
/// It stores field names necessary across all statistic functions.
class LogicalStatisticFields
{
public:
    /// The fields need to be in upper case. Otherwise, the parsing of the field names in the SLT of the sink does not work
    StatisticField statisticNumberOfSeenTuplesField
        = {"STATISTICNUMBEROFSEENTUPLES", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticIdField
        = {"STATISTICID", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticStartTsField
        = {"STATISTICSTART", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticEndTsField
        = {"STATISTICEND", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticDataField
        = {"STATISTICDATA", DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticTypeField
        = {"STATISTICTYPE", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};

    LogicalStatisticFields() = default;
    bool operator==(const LogicalStatisticFields&) const = default;

    LogicalStatisticFields(
        StatisticField statisticNumberOfSeenTuplesField,
        StatisticField statisticIdField,
        StatisticField statisticStartTsField,
        StatisticField statisticEndTsField)
        : statisticNumberOfSeenTuplesField(std::move(statisticNumberOfSeenTuplesField))
        , statisticIdField(std::move(statisticIdField))
        , statisticStartTsField(std::move(statisticStartTsField))
        , statisticEndTsField(std::move(statisticEndTsField))
    {
    }

    /// Prepend qualifierName (e.g. "default$") to every field name that does not already start with it.
    LogicalStatisticFields& addQualifierName(const std::string_view qualifierName)
    {
        auto qualify = [qualifierName](StatisticField& f)
        {
            if (!f.name.starts_with(qualifierName))
            {
                f.name = std::string(qualifierName) + f.name;
            }
        };
        qualify(statisticNumberOfSeenTuplesField);
        qualify(statisticIdField);
        qualify(statisticStartTsField);
        qualify(statisticEndTsField);
        qualify(statisticDataField);
        qualify(statisticTypeField);
        return *this;
    }
};
}
