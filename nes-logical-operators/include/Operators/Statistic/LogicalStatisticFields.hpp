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
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>

namespace NES
{
/// Acts as an abstract class that every statistic build logical function should inherit from.
/// It stores field names necessary across all statistic functions.
class LogicalStatisticFields
{
public:
    /// The fields need to be in upper case. Otherwise, the parsing of the field names in the SLT of the sink does not work
    Schema::Field statisticNumberOfSeenTuplesField
        = {"STATISTICNUMBEROFSEENTUPLES", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    Schema::Field statisticHashField
        = {"STATISTICHASH", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    Schema::Field statisticStartTsField
        = {"STATISTICSTART", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    Schema::Field statisticEndTsField
        = {"STATISTICEND", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    Schema::Field statisticDataField
        = {"STATISTICDATA", DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE)};
    Schema::Field statisticTypeField
        = {"STATISTICTYPE", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};

    LogicalStatisticFields() = default;
    bool operator==(const LogicalStatisticFields&) const = default;

    LogicalStatisticFields(
        Schema::Field statisticNumberOfSeenTuplesField,
        Schema::Field statisticHashField,
        Schema::Field statisticStartTsField,
        Schema::Field statisticEndTsField)
        : statisticNumberOfSeenTuplesField(std::move(statisticNumberOfSeenTuplesField))
        , statisticHashField(std::move(statisticHashField))
        , statisticStartTsField(std::move(statisticStartTsField))
        , statisticEndTsField(std::move(statisticEndTsField))
    {
    }

    LogicalStatisticFields& addQualifierName(const std::string_view qualifierName)
    {
        statisticNumberOfSeenTuplesField.addQualifierIfNotExists(qualifierName);
        statisticHashField.addQualifierIfNotExists(qualifierName);
        statisticStartTsField.addQualifierIfNotExists(qualifierName);
        statisticEndTsField.addQualifierIfNotExists(qualifierName);
        statisticDataField.addQualifierIfNotExists(qualifierName);
        statisticTypeField.addQualifierIfNotExists(qualifierName);
        return *this;
    }
};
}
