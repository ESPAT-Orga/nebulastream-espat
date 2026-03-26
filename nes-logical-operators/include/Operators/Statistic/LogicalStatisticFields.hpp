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
    Schema::Field statisticIdField
        = {"STATISTICID", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
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
        Schema::Field statisticIdField,
        Schema::Field statisticStartTsField,
        Schema::Field statisticEndTsField)
        : statisticNumberOfSeenTuplesField(std::move(statisticNumberOfSeenTuplesField))
        , statisticIdField(std::move(statisticIdField))
        , statisticStartTsField(std::move(statisticStartTsField))
        , statisticEndTsField(std::move(statisticEndTsField))
    {
    }

    LogicalStatisticFields& addQualifierName(const std::string_view qualifierName)
    {
        statisticNumberOfSeenTuplesField.addQualifierIfNotExists(qualifierName);
        statisticIdField.addQualifierIfNotExists(qualifierName);
        statisticStartTsField.addQualifierIfNotExists(qualifierName);
        statisticEndTsField.addQualifierIfNotExists(qualifierName);
        statisticDataField.addQualifierIfNotExists(qualifierName);
        statisticTypeField.addQualifierIfNotExists(qualifierName);
        return *this;
    }

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_START_TS{
            "statisticStartTs",
            {},
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(STATISTIC_START_TS, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_END_TS{
            "statisticEndTs",
            {},
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(STATISTIC_END_TS, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_NUMBER_OF_SEEN_TUPLES{
            "statisticNumberOfSeenTuples",
            {},
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(STATISTIC_NUMBER_OF_SEEN_TUPLES, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_ID_FIELD{
            "statisticIdFieldName",
            {},
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(STATISTIC_ID_FIELD, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_DATA{
            "statisticData",
            {},
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(STATISTIC_DATA, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_TYPE{
            "statisticType",
            {},
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(STATISTIC_TYPE, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(
                STATISTIC_START_TS, STATISTIC_END_TS, STATISTIC_NUMBER_OF_SEEN_TUPLES, STATISTIC_ID_FIELD, STATISTIC_DATA, STATISTIC_TYPE);
    };
};
}
