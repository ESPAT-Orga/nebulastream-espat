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
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
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

/// One statistic field: its identifier and its data type.
///
/// statistic-renaming declared these as `Schema::Field`. Upstream's Schema is templated on the field type,
/// and its bound `Field` requires the LogicalOperator that produced it, which is not available where these
/// constants are declared. We therefore keep the (name, type) pair here and materialise an
/// UnqualifiedUnboundField on demand for schema construction. `name` is an Identifier, which converts
/// implicitly to Record::RecordFieldIdentifier, so the physical operators read it directly.
struct StatisticField
{
    Identifier name;
    DataType dataType;

    StatisticField(Identifier name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType)) { }

    [[nodiscard]] UnqualifiedUnboundField unbound() const { return UnqualifiedUnboundField{name, dataType}; }

    bool operator==(const StatisticField&) const = default;
};

/// Acts as an abstract class that every statistic build logical function should inherit from.
/// It stores field names necessary across all statistic functions.
class LogicalStatisticFields
{
public:
    /// The fields need to be in upper case. Otherwise, the parsing of the field names in the SLT of the sink does not work
    StatisticField statisticNumberOfSeenTuplesField{
        Identifier::parse("STATISTICNUMBEROFSEENTUPLES"),
        DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticIdField{
        Identifier::parse("STATISTICID"), DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticStartTsField{
        Identifier::parse("STATISTICSTART"), DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticEndTsField{
        Identifier::parse("STATISTICEND"), DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticDataField{
        Identifier::parse("STATISTICDATA"), DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE)};
    StatisticField statisticTypeField{
        Identifier::parse("STATISTICTYPE"), DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)};

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

    /// TODO: statistic-renaming has addQualifierName(), which calls Schema::Field::addQualifierIfNotExists on
    /// each field so the StatisticBuild/StoreWriter output schema carries the source qualifier. Upstream models
    /// qualification through QualifiedIdentifier / IdList instead, and the fields above are deliberately
    /// unqualified (extent 1). Reintroduce the qualifying step when the statistic logical operators land, at
    /// which point the right upstream spelling will be clear from how those operators build their schemas.
};

}
