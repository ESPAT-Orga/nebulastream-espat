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

/// Reproduction tests for "Issue 1: Optimization rules corrupt multi-root logical plans"
/// (see MULTI_ROOT_PLAN_ISSUES.md). The tests assert the EXPECTED behavior for plans with multiple sink
/// roots that share operator instances (DAGs) and currently FAIL on main:
/// - rules rebuild the plan once per root (withInferredSchema/withChildren regenerate OperatorIds), so a
///   shared operator is silently duplicated into copies with fresh ids (nothing downstream can re-merge
///   them),
/// - rules with per-visit state (OriginIdInference) make diverging decisions for the copies,
/// - DecideMemoryLayoutRule outright rejects multi-root plans via a precondition (as do several other
///   rules; it stands in for that whole class here).

#include <cstdlib>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/OriginIdInferenceRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{

QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}

class MultiRootPlanRulesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("MultiRootPlanRulesTest.log", LogLevel::LOG_DEBUG); }

    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    TypedLogicalOperator<SourceDescriptorLogicalOperator> makeSource()
    {
        auto descriptor = sourceCatalog.getInlineSource(
            Identifier::parse("File"),
            createSchema(),
            Host("localhost"),
            {{Identifier::parse(InputFormatterDescriptor::getTypeString()), "CSV"}},
            {{Identifier::parse("file_path"), "/dev/null"}});
        EXPECT_TRUE(descriptor.has_value());
        return SourceDescriptorLogicalOperator::create(std::move(descriptor.value()));
    }

    LogicalOperator makeSink(const LogicalOperator& child)
    {
        auto descriptor = sinkCatalog.getInlineSink(
            createSchema(), Identifier::parse("Print"), Host("localhost"), {{Identifier::parse("output_format"), "CSV"}}, {});
        EXPECT_TRUE(descriptor.has_value());
        return SinkLogicalOperator::create(child, descriptor.value());
    }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};

/// Two sink roots share ONE selection instance. Rebuilding the plan (type inference) must not split the
/// shared operator into independent copies: the operator reachable via both roots must stay the same
/// operator (same id) afterwards.
TEST_F(MultiRootPlanRulesTest, TypeInferenceKeepsSharedOperatorsShared)
{
    const auto source = makeSource();
    const auto predicate = GreaterEqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("id")].value()},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}};
    const auto selection = SelectionLogicalOperator::create(source, predicate);
    const auto plan = LogicalPlan(randomQueryId(), {makeSink(selection), makeSink(selection)});

    /// Control: before the rule, both roots reference the same instance.
    ASSERT_EQ(plan.getRootOperators()[0].getChildren()[0].getId(), plan.getRootOperators()[1].getChildren()[0].getId());

    const auto inferred = TypeInferenceRule{}.apply(plan);

    const auto sharedViaRoot0 = inferred.getRootOperators()[0].getChildren()[0];
    const auto sharedViaRoot1 = inferred.getRootOperators()[1].getChildren()[0];
    EXPECT_EQ(sharedViaRoot0.getId(), sharedViaRoot1.getId())
        << "the shared selection was rebuilt once per root and duplicated into unrelated operators";
    /// The duplication cascades: the shared source below is duplicated as well.
    EXPECT_EQ(sharedViaRoot0.getChildren()[0].getId(), sharedViaRoot1.getChildren()[0].getId())
        << "the shared source was duplicated as well";
}

/// Two sink roots share ONE source. Origin inference assigns origin ids with a running counter per
/// visit, so the two paths to the same source must not end up with different origin ids: a shared source
/// is one origin, not one origin per consumer.
TEST_F(MultiRootPlanRulesTest, OriginIdInferenceAssignsOneOriginToASharedSource)
{
    const auto source = makeSource();
    const auto plan = LogicalPlan(randomQueryId(), {makeSink(source), makeSink(source)});

    const auto inferred = OriginIdInferenceRule{}.apply(plan);

    const auto originsViaRoot0 = getTrait<OutputOriginIdsTrait>(inferred.getRootOperators()[0].getChildren()[0].getTraitSet());
    const auto originsViaRoot1 = getTrait<OutputOriginIdsTrait>(inferred.getRootOperators()[1].getChildren()[0].getTraitSet());
    ASSERT_TRUE(originsViaRoot0.has_value());
    ASSERT_TRUE(originsViaRoot1.has_value());
    ASSERT_EQ(originsViaRoot0.value().get().size(), 1U);
    ASSERT_EQ(originsViaRoot1.value().get().size(), 1U);
    EXPECT_EQ(originsViaRoot0.value().get()[0], originsViaRoot1.value().get()[0])
        << "the SAME source was assigned a different origin id per consuming root (semantic divergence: "
           "origins drive watermarking)";
}

/// Multi-root plans must pass through the static rules; DecideMemoryLayoutRule currently rejects them
/// with PRECONDITION(rootOperators.size() == 1), which calls std::terminate — hence a death-test child
/// process that must exit cleanly once the precondition is lifted.
TEST_F(MultiRootPlanRulesTest, DecideMemoryLayoutAcceptsMultiRootPlans)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto source = makeSource();
    const auto plan = LogicalPlan(randomQueryId(), {makeSink(source), makeSink(source)});

    EXPECT_EXIT(
        {
            std::ignore = DecideMemoryLayoutRule{}.apply(plan);
            std::exit(0);
        },
        ::testing::ExitedWithCode(0),
        "")
        << "single-root precondition terminates on multi-root plans";
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
