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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Statistic.hpp>

namespace NES
{
class SerializableOperator;

/// Builds a new stream by reading back the single value of a scalar statistic (Count / Sum / Avg). One operator serves
/// all three ops: their payload is the same bare scalar and they differ only in its data type, which the probe must be
/// told because nothing links it back to the build's on-field (see the SQL surface in AntlrSQLQueryPlanCreator).
/// The statisticType is carried because StatisticProvider dispatches its iterator on it.
class ScalarStatisticProbeLogicalOperator final : public LogicalStatisticFields, public OriginIdAssigner
{
public:
    explicit ScalarStatisticProbeLogicalOperator(Statistic::StatisticId statisticId, Statistic::StatisticType op, DataType valueType);
    explicit ScalarStatisticProbeLogicalOperator(
        Statistic::StatisticId statisticId,
        Statistic::StatisticType op,
        DataType valueType,
        std::string valueFieldName,
        LogicalStatisticFields logicalStatisticFields);

    [[nodiscard]] bool operator==(const ScalarStatisticProbeLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withTraitSet(TraitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withInferredSchema(const std::vector<Schema>& inputSchemas) const;

    Statistic::StatisticId statisticId;
    Statistic::StatisticType op;
    DataType valueType;

    std::string valueFieldName = VALUE_FIELD_NAME_DEFAULT;

private:
    static constexpr std::string_view NAME = "ScalarStatisticProbe";

    static constexpr std::string VALUE_FIELD_NAME_DEFAULT = "STATISTICVALUE";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

template <>
struct Reflector<ScalarStatisticProbeLogicalOperator>
{
    Reflected operator()(const ScalarStatisticProbeLogicalOperator&) const;
};

template <>
struct Unreflector<ScalarStatisticProbeLogicalOperator>
{
    ScalarStatisticProbeLogicalOperator operator()(const Reflected&) const;
};

static_assert(LogicalOperatorConcept<ScalarStatisticProbeLogicalOperator>);

namespace detail
{
struct ReflectedScalarStatisticProbeLogicalOperator
{
    Statistic::StatisticId::Underlying statisticId;
    uint64_t op;
    DataType valueType;
    std::string valueFieldName;
};
}

}
