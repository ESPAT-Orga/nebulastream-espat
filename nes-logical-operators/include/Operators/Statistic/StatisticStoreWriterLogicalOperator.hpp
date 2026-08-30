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

#include <optional>
#include <utility>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Reflection.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

/// Logical operator that writes a statistic to the statistic store on the node.
/// This operator decides what node stores the built statistic emitted from the statistic query.
class StatisticStoreWriterLogicalOperator final : public ManagedByOperator
{
public:
    /// Persists ONE statistic built by a StatisticBuild. Multiple synopses in one query are handled by chaining
    /// N of these (one per target). The VARSIZED data field this writer reads is derived from the id
    /// (statisticDataFieldName); the operator passes its input through and only adds the STATISTICID field, so
    /// downstream writers in the chain still see every data field.
    StatisticStoreWriterLogicalOperator(
        WeakLogicalOperator self,
        std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType statisticType);

    StatisticStoreWriterLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType statisticType);

    static TypedLogicalOperator<StatisticStoreWriterLogicalOperator> create(
        std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType statisticType);

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] bool operator==(const StatisticStoreWriterLogicalOperator& rhs) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withInferredSchema() const;
    [[nodiscard]] StatisticTuple::StatisticId getStatisticId() const;
    [[nodiscard]] StatisticTuple::StatisticType getStatisticType() const;
    [[nodiscard]] static LogicalStatisticFields getOutputStatisticFields(std::string_view qualifierName);

    /// Needs to be shared so that the window aggregation build can set the field names
    std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields;

private:
    static constexpr std::string_view NAME = "StatisticStoreWriter";

    StatisticTuple::StatisticId statisticId;
    StatisticTuple::StatisticType statisticType;
    std::optional<LogicalOperator> child;
    TraitSet traitSet;

    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

template <>
struct Reflector<StatisticStoreWriterLogicalOperator>
{
    Reflected operator()(const StatisticStoreWriterLogicalOperator& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<StatisticStoreWriterLogicalOperator>
{
    StatisticStoreWriterLogicalOperator operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<StatisticStoreWriterLogicalOperator>);

namespace detail
{
struct ReflectedStatisticStoreWriterLogicalOperator
{
    /// The data field name is NOT serialized -- it is re-derived from the id (statisticDataFieldName), so it
    /// cannot drift across (de)serialization.
    StatisticTuple::StatisticId::Underlying statisticId;
    StatisticTuple::StatisticType statisticType;
};
}

}
