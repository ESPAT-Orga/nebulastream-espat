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

#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>

#include <AntlrSQLBaseListener.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <ParserRuleContext.h>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Identifiers/SketchDimensions.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <CommonParserFunctions.hpp>
#include <ErrorHandling.hpp>
#include <ParserUtil.hpp>

namespace NES::Parsers
{
LogicalPlan AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    if (sinks.empty())
    {
        throw InvalidQuerySyntax("Query does not contain sink");
    }
    if (queryPlans.empty())
    {
        throw InvalidQuerySyntax("Query could not be parsed");
    }
    /// TODO #421: support multiple sinks
    INVARIANT(!sinks.empty(), "Need at least one sink!");
    return std::visit(
        Overloaded{
            [&](const std::string& sinkName) { return LogicalPlanBuilder::addSink(sinkName, queryPlans.top()); },
            [&](const std::pair<std::string, ConfigMap>& inlineSink)
            {
                const auto& [type, configOptions] = inlineSink;
                const auto sinkConfig = getSinkConfig(configOptions);
                const auto schemaOpt = getSinkSchema(configOptions);
                const Schema schema = (schemaOpt.has_value() ? schemaOpt.value() : Schema{});
                return LogicalPlanBuilder::addInlineSink(type, schema, sinkConfig, queryPlans.top());
            }},
        sinks.front());
}

Windowing::TimeMeasure buildTimeMeasure(const int size, const uint64_t timebase)
{
    switch (timebase)
    {
        case AntlrSQLLexer::MS:
            return API::Milliseconds(size);
        case AntlrSQLLexer::SEC:
            return API::Seconds(size);
        case AntlrSQLLexer::MINUTE:
            return API::Minutes(size);
        case AntlrSQLLexer::HOUR:
            return API::Hours(size);
        case AntlrSQLLexer::DAY:
            return API::Days(size);
        default:
            const AntlrSQLLexer lexer(nullptr);
            const std::string tokenName = std::string(lexer.getVocabulary().getSymbolicName(timebase));
            throw InvalidQuerySyntax("Unknown time unit: {}", tokenName);
    }
}

static LogicalFunction createFunctionFromOpBoolean(LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType)
    {
        case AntlrSQLLexer::EQ:
            return EqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::NEQJ:
            return NegateLogicalFunction(EqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction)));
        case AntlrSQLLexer::LT:
            return LessLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GT:
            return GreaterLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GTE:
            return GreaterEqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::LTE:
            return LessEqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown Comparison Operator: {} of type: {}", lexer.getVocabulary().getSymbolicName(tokenType), tokenType);
    }
}

static LogicalFunction createLogicalBinaryFunction(LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType)
    {
        case AntlrSQLLexer::AND:
            return AndLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::OR:
            return OrLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown binary function in SQL query for op {} with type: {} and left {} and right {}",
                lexer.getVocabulary().getSymbolicName(tokenType),
                tokenType,
                std::move(leftFunction),
                std::move(rightFunction));
    }
}

void AntlrSQLQueryPlanCreator::enterSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    helpers.top().isSelect = true;
    AntlrSQLBaseListener::enterSelectClause(context);
}

void AntlrSQLQueryPlanCreator::enterFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helpers.top().isFrom = true;
    AntlrSQLBaseListener::enterFromClause(context);
}

void AntlrSQLQueryPlanCreator::enterSinkClause(AntlrSQLParser::SinkClauseContext* context)
{
    if (context->sink().empty())
    {
        throw InvalidQuerySyntax("INTO must be followed by at least one sink-identifier.");
    }
    /// Store all specified sinks.
    for (const auto& sink : context->sink())
    {
        if (sink->identifier() != nullptr)
        {
            sinks.emplace_back(bindIdentifier(sink->identifier()));
        }
        else if (sink->inlineSink() != nullptr)
        {
            const auto& sinkInlineSink = sink->inlineSink();

            const auto type = bindIdentifier(sinkInlineSink->type);
            const auto configOptions = bindConfigOptions(sinkInlineSink->parameters->namedConfigExpression());

            sinks.emplace_back(std::make_pair(type, configOptions));
        }
    }
}

void AntlrSQLQueryPlanCreator::exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext* context)
{
    /// If we are exiting a logical binary operator in a join relation, we need to build the binary function for the joinKey and
    /// not for the general function
    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Expected two operands for binary op, got {}: {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto leftFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helpers.top().joinKeyRelationHelper.push_back(function);
    }
    else
    {
        if (helpers.top().functionBuilder.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Expected two operands for binary op, got {}: {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto leftFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helpers.top().functionBuilder.push_back(function);
    }
}

void AntlrSQLQueryPlanCreator::exitSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    helpers.top().functionBuilder.clear();
    helpers.top().isSelect = false;
    AntlrSQLBaseListener::exitSelectClause(context);
}

void AntlrSQLQueryPlanCreator::exitFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helpers.top().isFrom = false;
    AntlrSQLBaseListener::exitFromClause(context);
}

void AntlrSQLQueryPlanCreator::enterWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helpers.top().isWhereOrHaving = true;
    AntlrSQLBaseListener::enterWhereClause(context);
}

void AntlrSQLQueryPlanCreator::exitWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    helpers.top().addWhereClause(helpers.top().functionBuilder.back());
    helpers.top().functionBuilder.clear();
    AntlrSQLBaseListener::exitWhereClause(context);
}

void AntlrSQLQueryPlanCreator::enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext* context)
{
    auto opTokenType = context->getStart()->getType();
    helpers.top().opBoolean = opTokenType;
    AntlrSQLBaseListener::enterComparisonOperator(context);
}

void AntlrSQLQueryPlanCreator::exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }
    LogicalFunction function;

    if (helpers.top().functionBuilder.size() < 2)
    {
        if (helpers.top().functionBuilder.size() + helpers.top().constantBuilder.size() == 2)
        {
            throw InvalidQuerySyntax(
                "Attempted to use a raw constant in a binary expression. {} in `{}`.",
                fmt::join(helpers.top().constantBuilder, ", "),
                context->getText());
        }
        throw InvalidQuerySyntax(
            "There were less than 2 functions in the functionBuilder in exitArithmeticBinary. `{}`.", context->getText());
    }
    const auto rightFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    const auto leftFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType)
    {
        case AntlrSQLLexer::ASTERISK:
            function = MulLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::SLASH:
            function = DivLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::PLUS:
            function = AddLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::MINUS:
            function = SubLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::PERCENT:
            function = ModuloLogicalFunction(leftFunction, rightFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helpers.top().functionBuilder.push_back(function);
}

void AntlrSQLQueryPlanCreator::exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }
    LogicalFunction function;

    if (helpers.top().functionBuilder.empty())
    {
        throw InvalidQuerySyntax("Expected unary operator, got nothing: {}", context->getText());
    }
    const auto innerFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType)
    {
        case AntlrSQLLexer::PLUS:
            function = innerFunction;
            break;
        case AntlrSQLLexer::MINUS:
            function = MulLogicalFunction(
                ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UINT64), "-1"), innerFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helpers.top().functionBuilder.push_back(function);
}

void AntlrSQLQueryPlanCreator::enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent);
    const bool isParentRuleTableAlias = (parentContext != nullptr) && parentContext->getRuleIndex() == AntlrSQLParser::RuleTableAlias;
    if (helpers.top().isFrom && !helpers.top().isJoinRelation)
    {
        helpers.top().newSourceName = context->getText();
    }
    else if (helpers.top().isJoinRelation && isParentRuleTableAlias)
    {
        helpers.top().joinSourceRenames.emplace_back(context->getText());
    }
    AntlrSQLBaseListener::enterUnquotedIdentifier(context);
}

void AntlrSQLQueryPlanCreator::enterIdentifier(AntlrSQLParser::IdentifierContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    std::optional<size_t> parentRuleIndex;
    if (const auto* const parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }
    if (helpers.top().isGroupBy)
    {
        helpers.top().groupByFields.emplace_back(bindIdentifier(context));
    }
    else if (
        (helpers.top().isWhereOrHaving || helpers.top().isSelect || helpers.top().isWindow)
        && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().functionBuilder.emplace_back(FieldAccessLogicalFunction(bindIdentifier(context)));
    }
    else if (helpers.top().isFrom and not helpers.top().isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        /// get main source name
        helpers.top().setSource(bindIdentifier(context));
    }
    else if (
        AntlrSQLParser::RuleNamedExpression == parentRuleIndex and helpers.top().isInFunctionCall() and not helpers.top().isJoinRelation
        and not helpers.top().isInAggFunction())
    {
        /// handle renames of identifiers
        if (helpers.top().isArithmeticBinary)
        {
            throw InvalidQuerySyntax("There must not be a binary arithmetic token at this point: {}", context->getText());
        }
        if ((helpers.top().isWhereOrHaving || helpers.top().isSelect))
        {
            /// The user specified named expression (field access or function) with 'AS THE_NAME'
            /// (we handle cases where the user did not specify a name via 'AS' in 'exitNamedExpression')
            const auto attribute = std::move(helpers.top().functionBuilder.back());
            helpers.top().functionBuilder.pop_back();
            helpers.top().addProjection(FieldIdentifier(bindIdentifier(context)), attribute);
        }
    }
    else if (helpers.top().isInAggFunction() and AntlrSQLParser::RuleNamedExpression == parentRuleIndex)
    {
        const auto expression = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        if (expression.tryGetAs<FieldAccessLogicalFunction>())
        {
            /// Simple case: MEDIAN(i8) AS out — use the alias as the aggregation output name.
            const auto aggFunc = helpers.top().windowAggs.back();
            helpers.top().windowAggs.pop_back();
            const auto newAggFunc = std::make_shared<WindowAggregationLogicalFunction>(
                aggFunc->withAsField(FieldAccessLogicalFunction(bindIdentifier(context))));
            helpers.top().windowAggs.push_back(newAggFunc);
            helpers.top().addProjection(std::nullopt, newAggFunc->getAsField());
        }
        else
        {
            /// Expression case: MEDIAN(i8) + UINT64(1) AS out — project expression under alias.
            helpers.top().addProjection(FieldIdentifier(bindIdentifier(context)), expression);
        }
        helpers.top().hasUnnamedAggregation = false;
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().joinKeyRelationHelper.emplace_back(FieldAccessLogicalFunction(bindIdentifier(context)));
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        helpers.top().joinSources.push_back(bindIdentifier(context));
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RuleTableAlias == parentRuleIndex)
    {
        helpers.top().joinSourceRenames.push_back(bindIdentifier(context));
    }
}

void AntlrSQLQueryPlanCreator::enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    if (not helpers.empty() and not helpers.top().isFrom and not helpers.top().isSetOperation)
    {
        throw InvalidQuerySyntax("Subqueries are only supported in FROM clauses, but got {}", context->getText());
    }

    const AntlrSQLHelper helper;
    helpers.push(helper);
    AntlrSQLBaseListener::enterPrimaryQuery(context);
}

void AntlrSQLQueryPlanCreator::exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    LogicalPlan queryPlan = [&]
    {
        if (not helpers.top().queryPlans.empty())
        {
            return std::move(helpers.top().queryPlans[0]);
        }
        if (helpers.top().getSource().empty())
        {
            const auto [type, configOptions] = helpers.top().getInlineSourceConfig();
            const auto parserConfig = getParserConfig(configOptions);
            const auto sourceConfig = getSourceConfig(configOptions);
            const auto schema = getSourceSchema(configOptions);
            if (!schema.has_value())
            {
                throw InvalidConfigParameter("Inline Source is missing schema definition");
            }

            return LogicalPlanBuilder::createLogicalPlan(type, schema.value(), sourceConfig, parserConfig);
        }
        return LogicalPlanBuilder::createLogicalPlan(helpers.top().getSource());
    }();

    for (auto whereExpr = helpers.top().getWhereClauses().rbegin(); whereExpr != helpers.top().getWhereClauses().rend(); ++whereExpr)
    {
        queryPlan = LogicalPlanBuilder::addSelection(std::move(*whereExpr), queryPlan);
    }

    /// Insert pre-aggregation projections for desugared expression arguments (e.g., AVG(i + UINT64(1))).
    if (!helpers.top().preAggregationProjections.empty())
    {
        queryPlan = LogicalPlanBuilder::addProjection(helpers.top().preAggregationProjections, /*asterisk=*/true, queryPlan);
    }

    auto statisticAggs = std::initializer_list<std::string>{"ReservoirSample", "EquiWidthHistogram", "CountMinSketch"};
    if (helpers.top().windowType != nullptr && helpers.top().joinKeyRelationHelper.empty() and not helpers.top().windowAggs.empty()
        and std::ranges::find(statisticAggs, helpers.top().windowAggs.front().get()->getName()) != std::end(statisticAggs))
    {
        /// This is actually a statistic operation, not an aggregation
        auto logicalStatisticFields = std::make_shared<LogicalStatisticFields>();
        queryPlan = LogicalPlanBuilder::addStatisticBuild(
            queryPlan, helpers.top().windowType, helpers.top().windowAggs, helpers.top().groupByFields, logicalStatisticFields);
        const auto windowAggName = helpers.top().windowAggs.front().get()->getName();

        if (not helpers.top().statProbe.has_value())
        {
            const auto hash = FieldAccessLogicalFunction{
                LogicalStatisticFields().statisticHashField.dataType, LogicalStatisticFields().statisticHashField.name};
            helpers.top().addProjection(std::nullopt, hash);
            const auto start = FieldAccessLogicalFunction{
                LogicalStatisticFields().statisticStartTsField.dataType, LogicalStatisticFields().statisticStartTsField.name};
            helpers.top().addProjection(std::nullopt, start);
            const auto end = FieldAccessLogicalFunction{
                LogicalStatisticFields().statisticEndTsField.dataType, LogicalStatisticFields().statisticEndTsField.name};
            helpers.top().addProjection(std::nullopt, end);
            const auto numTuples = FieldAccessLogicalFunction{
                LogicalStatisticFields().statisticNumberOfSeenTuplesField.dataType,
                LogicalStatisticFields().statisticNumberOfSeenTuplesField.name};
            helpers.top().addProjection(std::nullopt, numTuples);
        }

        if (windowAggName == "ReservoirSample")
        {
            queryPlan = LogicalPlanBuilder::addStatisticStoreWriter(
                queryPlan, logicalStatisticFields, helpers.top().statisticHash.value(), Statistic::StatisticType::Reservoir_Sample);
        }
        else if (windowAggName == "EquiWidthHistogram")
        {
            queryPlan = LogicalPlanBuilder::addStatisticStoreWriter(
                queryPlan, logicalStatisticFields, helpers.top().statisticHash.value(), Statistic::StatisticType::Equi_Width_Histogram);
        }
        if (windowAggName == "CountMinSketch")
        {
            queryPlan = LogicalPlanBuilder::addStatisticStoreWriter(
                queryPlan, logicalStatisticFields, helpers.top().statisticHash.value(), Statistic::StatisticType::Count_Min_Sketch);
        }
    }
    else if (helpers.top().isInAggFunction())
    {
        /// Plain old windowed aggregation
        queryPlan = LogicalPlanBuilder::addWindowAggregation(
            queryPlan, helpers.top().windowType, helpers.top().windowAggs, helpers.top().groupByFields);
    }
    else if (helpers.top().windowType != nullptr && helpers.top().joinKeyRelationHelper.empty())
    {
        /// Window without aggregation function
        queryPlan = LogicalPlanBuilder::addWindowAggregation(
            queryPlan, helpers.top().windowType, helpers.top().windowAggs, helpers.top().groupByFields);
    }
    if (helpers.top().statProbe.has_value())
    {
        auto op = helpers.top().statProbe.value();
        queryPlan = LogicalPlanBuilder::addStatProbeOp(op, queryPlan);

        const auto windowStart = FieldAccessLogicalFunction{
            LogicalStatisticFields().statisticStartTsField.dataType, LogicalStatisticFields().statisticStartTsField.name};
        const auto windowEnd = FieldAccessLogicalFunction{
            LogicalStatisticFields().statisticEndTsField.dataType, LogicalStatisticFields().statisticEndTsField.name};
        helpers.top().addProjection(std::nullopt, windowStart);
        helpers.top().addProjection(std::nullopt, windowEnd);
        if (auto probeOpt = op.tryGetAs<ReservoirProbeLogicalOperator>())
        {
            const auto probe = probeOpt.value().get();
            for (auto schemaField : probe.sampleSchema.getFields())
            {
                const auto field = FieldAccessLogicalFunction{schemaField.dataType, schemaField.name};
                helpers.top().addProjection(std::nullopt, field);
            }
        }
        else if (auto probeOpt = op.tryGetAs<EquiWidthHistogramProbeLogicalOperator>())
        {
            const auto probe = probeOpt.value().get();
            const auto start = FieldAccessLogicalFunction{probe.startEndType, probe.binStartFieldName};
            const auto end = FieldAccessLogicalFunction{probe.startEndType, probe.binEndFieldName};
            const auto counter = FieldAccessLogicalFunction{probe.counterType, probe.binCounterFieldName};

            helpers.top().addProjection(std::nullopt, start);
            helpers.top().addProjection(std::nullopt, end);
            helpers.top().addProjection(std::nullopt, counter);
        }
        else if (auto probeOpt = op.tryGetAs<CountMinSketchProbeLogicalOperator>())
        {
            const auto probe = probeOpt.value().get();
            const auto rowIndex = FieldAccessLogicalFunction{probe.indexType, probe.rowIndexFieldName};
            const auto columnIndex = FieldAccessLogicalFunction{probe.indexType, probe.columnIndexFieldName};
            const auto counter = FieldAccessLogicalFunction{probe.counterType, probe.counterFieldName};

            helpers.top().addProjection(std::nullopt, rowIndex);
            helpers.top().addProjection(std::nullopt, columnIndex);
            helpers.top().addProjection(std::nullopt, counter);
        }
    }

    queryPlan = LogicalPlanBuilder::addProjection(helpers.top().getProjections(), helpers.top().asterisk, queryPlan);

    if (helpers.top().windowType != nullptr)
    {
        for (auto havingExpr = helpers.top().getHavingClauses().rbegin(); havingExpr != helpers.top().getHavingClauses().rend();
             ++havingExpr)
        {
            queryPlan = LogicalPlanBuilder::addSelection(*havingExpr, queryPlan);
        }
    }
    helpers.pop();
    if (helpers.empty())
    {
        queryPlans.push(queryPlan);
    }
    else
    {
        auto& subQueryHelper = helpers.top();
        subQueryHelper.queryPlans.push_back(queryPlan);
    }
    AntlrSQLBaseListener::exitPrimaryQuery(context);
}

void AntlrSQLQueryPlanCreator::enterWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helpers.top().isWindow = true;
    AntlrSQLBaseListener::enterWindowClause(context);
}

void AntlrSQLQueryPlanCreator::exitWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helpers.top().isWindow = false;
    AntlrSQLBaseListener::exitWindowClause(context);
}

void AntlrSQLQueryPlanCreator::enterTimeUnit(AntlrSQLParser::TimeUnitContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    std::optional<size_t> parentRuleIndex;
    if (const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }

    auto* token = context->getStop();
    auto timeunit = token->getType();
    if (parentRuleIndex == AntlrSQLParser::RuleAdvancebyParameter)
    {
        helpers.top().timeUnitAdvanceBy = timeunit;
    }
    else
    {
        helpers.top().timeUnit = timeunit;
    }
}

void AntlrSQLQueryPlanCreator::exitSizeParameter(AntlrSQLParser::SizeParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("SizeParameter must have 'size', a number, and a time unit.");
    }
    helpers.top().size = std::stoi(context->children.at(1)->getText());
    AntlrSQLBaseListener::exitSizeParameter(context);
}

void AntlrSQLQueryPlanCreator::exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("AdvancebyParameter must have 'ADVANCE BY', a number, and a time unit.");
    }
    helpers.top().advanceBy = std::stoi(context->children.at(2)->getText());
    AntlrSQLBaseListener::exitAdvancebyParameter(context);
}

void AntlrSQLQueryPlanCreator::exitTimestampParameter(AntlrSQLParser::TimestampParameterContext* context)
{
    helpers.top().timestamp = bindIdentifier(context->name);
}

/// WINDOWS
void AntlrSQLQueryPlanCreator::exitTumblingWindow(AntlrSQLParser::TumblingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helpers.top().timestamp.empty())
    {
        helpers.top().windowType = std::make_shared<Windowing::TumblingWindow>(API::IngestionTime(), timeMeasure);
    }
    else
    {
        helpers.top().windowType = std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helpers.top().timestamp)), timeMeasure);
    }
    AntlrSQLBaseListener::exitTumblingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    const auto slidingLength = buildTimeMeasure(helpers.top().advanceBy, helpers.top().timeUnitAdvanceBy);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helpers.top().timestamp.empty())
    {
        helpers.top().windowType = Windowing::SlidingWindow::of(API::IngestionTime(), timeMeasure, slidingLength);
    }
    else
    {
        helpers.top().windowType = Windowing::SlidingWindow::of(
            Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helpers.top().timestamp)),
            timeMeasure,
            slidingLength);
    }
    AntlrSQLBaseListener::exitSlidingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper& helper = helpers.top();
    if (context->name == nullptr and helper.functionBuilder.size() == 1
        and helper.functionBuilder.back().tryGetAs<FieldAccessLogicalFunction>() and not helpers.top().hasUnnamedAggregation)
    {
        /// Project onto the specified field and remove the field access from the active functions.
        helpers.top().addProjection(std::nullopt, std::move(helpers.top().functionBuilder.back()));
        helpers.top().functionBuilder.pop_back();
    }
    else if (helper.isSelect && context->getText() == "*" && helper.functionBuilder.empty())
    {
        helper.asterisk = true;
    }
    /// The user did not specify a new name (... AS THE_NAME) for the aggregation function.
    /// The aggregation's asField is already set in exitFunctionCall. Project the expression directly,
    /// which may be a plain field access (MEDIAN(i8)) or an arithmetic expression (MEDIAN(i8) + UINT64(1)).
    else if (context->name == nullptr and not helpers.top().functionBuilder.empty() and helpers.top().hasUnnamedAggregation)
    {
        const auto expression = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        helpers.top().addProjection(std::nullopt, expression);
        helpers.top().hasUnnamedAggregation = false;
    }
    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLBaseListener::enterFunctionCall(context);
}

void AntlrSQLQueryPlanCreator::enterHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = true;
    AntlrSQLBaseListener::enterHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    if (helpers.top().functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There was more than one function in the functionBuilder in exitHavingClause.");
    }
    helpers.top().addHavingClause(helpers.top().functionBuilder.back());
    helpers.top().functionBuilder.clear();
    AntlrSQLBaseListener::exitHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitComparison(AntlrSQLParser::ComparisonContext* context)
{
    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Requires two functions but got {} at {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto leftFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helpers.top().opBoolean);
        helpers.top().joinKeyRelationHelper.push_back(function);
    }
    else
    {
        if (helpers.top().functionBuilder.size() < 2)
        {
            throw InvalidQuerySyntax("Comparison requires two parameters, got {}", context->getText());
        }
        const auto rightFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto leftFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();

        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helpers.top().opBoolean);
        helpers.top().functionBuilder.push_back(function);
    }
    AntlrSQLBaseListener::exitComparison(context);
}

void AntlrSQLQueryPlanCreator::enterJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helpers.top().joinKeyRelationHelper.clear();
    helpers.top().isJoinRelation = true;
    AntlrSQLBaseListener::enterJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext* context)
{
    INVARIANT(helpers.top().isJoinRelation, "Join criteria must be inside a join relation.");
    AntlrSQLBaseListener::enterJoinCriteria(context);
}

void AntlrSQLQueryPlanCreator::enterJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    if (not helpers.top().isJoinRelation)
    {
        throw InvalidQuerySyntax("Join type must be inside a join relation.");
    }
    AntlrSQLBaseListener::enterJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    const auto joinType = context->getText();
    auto tokenType = context->getStop()->getType();

    if (joinType.empty() || tokenType == AntlrSQLLexer::INNER)
    {
        helpers.top().joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown join type: {}, resolved to token type: {}", joinType, tokenType);
    }
    AntlrSQLBaseListener::exitJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helpers.top().isJoinRelation = false;
    if (helpers.top().joinSources.size() == helpers.top().joinSourceRenames.size() + 1)
    {
        helpers.top().joinSourceRenames.emplace_back("");
    }

    /// we assume that the left query plan is the first element in the queryPlans vector and the right query plan is the second element
    if (helpers.top().queryPlans.size() != 2)
    {
        throw InvalidQuerySyntax(
            "Join relation requires two subqueries, but got {} at {}", helpers.top().queryPlans.size(), context->getText());
    }
    const auto leftQueryPlan = helpers.top().queryPlans[0];
    const auto rightQueryPlan = helpers.top().queryPlans[1];
    helpers.top().queryPlans.clear();

    if (helpers.top().joinKeyRelationHelper.size() != 1)
    {
        throw InvalidQuerySyntax("joinFunction is required but empty at {}", context->getText());
    }
    if (!helpers.top().windowType)
    {
        throw InvalidQuerySyntax("windowType is required but empty at {}", context->getText());
    }
    const auto queryPlan = LogicalPlanBuilder::addJoin(
        leftQueryPlan, rightQueryPlan, helpers.top().joinKeyRelationHelper.at(0), helpers.top().windowType, helpers.top().joinType);
    if (not helpers.empty())
    {
        /// we are in a subquery
        helpers.top().queryPlans.push_back(queryPlan);
    }
    else
    {
        /// for now, we will never enter this branch, because we always have a subquery
        /// as we require the join relations to always be a sub-query
        queryPlans.push(queryPlan);
    }
    AntlrSQLBaseListener::exitJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::exitLogicalNot(AntlrSQLParser::LogicalNotContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }

    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", context->getText());
        }
        const auto innerFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        auto negatedFunction = NegateLogicalFunction(innerFunction);
        helpers.top().joinKeyRelationHelper.emplace_back(negatedFunction);
    }
    else
    {
        if (helpers.top().functionBuilder.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", context->getText());
        }
        const auto innerFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        helpers.top().functionBuilder.emplace_back(NegateLogicalFunction(innerFunction));
    }
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    if (context->children.size() != 1)
    {
        throw InvalidQuerySyntax("When exiting a constant, there must be exactly one children in the context {}", context->getText());
    }
    if (const auto stringLiteralContext = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(context->children.at(0)))
    {
        if (!(stringLiteralContext->getText().size() > 2))
        {
            throw InvalidQuerySyntax(
                "A constant string literal must contain at least two quotes and must not be empty at {}", context->getText());
        }
        helpers.top().constantBuilder.push_back(context->getText().substr(1, stringLiteralContext->getText().size() - 2));
    }
    else
    {
        helpers.top().constantBuilder.push_back(context->getText());
    }
}

static uint64_t parseConstant(std::string constant, const char* fieldName)
{
    uint64_t result;
    auto parseResult = std::from_chars(constant.data(), constant.data() + constant.size(), result);
    if (parseResult.ec != std::errc())
    {
        throw InvalidQuerySyntax("Failed to parse field `{}` content: {}", fieldName, constant);
    }
    return result;
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    const auto funcName = toUpperCase(context->children[0]->getText());
    const auto tokenType = context->getStart()->getType();

    /// Ensures the current aggregation argument is a direct field access.
    /// If the argument is an expression (e.g. i + UINT64(1)), desugars it into a pre-aggregation
    /// projection so that the aggregation operates on a simple field reference.
    const auto ensureFieldAccessArgument = [&]()
    {
        if (helpers.top().functionBuilder.empty())
        {
            throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
        }
        if (!helpers.top().functionBuilder.back().tryGetAs<FieldAccessLogicalFunction>())
        {
            /// Desugar: pop the expression, create a temp field, store a pre-aggregation projection,
            /// and push a FieldAccess to the temp field back onto functionBuilder.
            auto expression = std::move(helpers.top().functionBuilder.back());
            helpers.top().functionBuilder.pop_back();
            const auto tempName = toUpperCase(fmt::format("_agg_input_{}", helpers.top().aggExprCounter++));
            helpers.top().preAggregationProjections.emplace_back(FieldIdentifier(tempName), std::move(expression));
            helpers.top().functionBuilder.emplace_back(FieldAccessLogicalFunction(tempName));
        }
    };

    auto isAggregation = false;
    switch (tokenType)
    {
        case AntlrSQLLexer::COUNT:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(
                CountAggregationLogicalFunction(helpers.top().functionBuilder.back().getAs<FieldAccessLogicalFunction>().get())));
            isAggregation = true;
            break;
        case AntlrSQLLexer::AVG:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(
                AvgAggregationLogicalFunction(helpers.top().functionBuilder.back().getAs<FieldAccessLogicalFunction>().get())));
            isAggregation = true;
            break;
        case AntlrSQLLexer::MAX:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(
                MaxAggregationLogicalFunction(helpers.top().functionBuilder.back().getAs<FieldAccessLogicalFunction>().get())));
            isAggregation = true;
            break;
        case AntlrSQLLexer::MIN:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(
                MinAggregationLogicalFunction(helpers.top().functionBuilder.back().getAs<FieldAccessLogicalFunction>().get())));
            isAggregation = true;
            break;
        case AntlrSQLLexer::SUM:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(
                SumAggregationLogicalFunction(helpers.top().functionBuilder.back().getAs<FieldAccessLogicalFunction>().get())));
            isAggregation = true;
            break;
        case AntlrSQLLexer::MEDIAN:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(
                MedianAggregationLogicalFunction(helpers.top().functionBuilder.back().getAs<FieldAccessLogicalFunction>().get())));
            isAggregation = true;
            break;
        default:
            helpers.top().hasUnnamedAggregation = false;
            /// Check if the function is a constructor for a datatype
            if (const auto dataType = DataTypeProvider::tryProvideDataType(funcName); dataType.has_value())
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax("Expected constant, got nothing at {}", context->getText());
                }
                helpers.top().hasUnnamedAggregation = false;
                auto value = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                auto constFunctionItem = ConstantValueLogicalFunction(*dataType, std::move(value));
                helpers.top().functionBuilder.emplace_back(constFunctionItem);
            }
            /// These special functions mix constants and field references, so they must be handled
            /// before the general argument count check (which only counts functionBuilder entries).
            else if (funcName == "RESERVOIR")
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected constant (sample hash) as first argument of Reservoir_Probe function call, got nothing at {}",
                        context->getText());
                }
                const uint64_t statisticHash = parseConstant(helpers.top().constantBuilder.front(), "statisticHash");
                helpers.top().statisticHash = statisticHash;
                if (helpers.top().functionBuilder.empty())
                {
                    throw InvalidQuerySyntax("Sample requires sample fields as arguments at {}", context->getText());
                }
                std::vector<FieldAccessLogicalFunction> sampleFields;
                for (auto& field : helpers.top().functionBuilder)
                {
                    PRECONDITION(
                        field.tryGetAs<FieldAccessLogicalFunction>().has_value(), "sample field was not a FieldAccessLogicalFunction");
                    sampleFields.emplace_back(field.getAs<FieldAccessLogicalFunction>().get());
                }
                helpers.top().functionBuilder.clear();

                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected constant at the end of ReservoirSample function call, got nothing at {}", context->getText());
                }
                uint64_t reservoirSize = parseConstant(helpers.top().constantBuilder.back(), "reservoirSize");
                helpers.top().constantBuilder.pop_back();
                /// We need to pass a field that exists in the schema. But we do not care of the actual value, as the sample gets built
                /// across all fields for now.
                const auto uselessOnField = FieldAccessLogicalFunction(sampleFields[0]);
                const auto asFieldIfNotOverwritten = FieldAccessLogicalFunction{
                    LogicalStatisticFields().statisticDataField.dataType, LogicalStatisticFields().statisticDataField.name};
                helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(
                    ReservoirSampleLogicalFunction(uselessOnField, asFieldIfNotOverwritten, sampleFields, reservoirSize, statisticHash)));
                break;
            }
            else if (funcName == "RESERVOIR_PROBE")
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected constant (sample hash) as first argument of Reservoir_Probe function call, got nothing at {}",
                        context->getText());
                }
                const uint64_t statisticHash = parseConstant(helpers.top().constantBuilder.back(), "statisticHash");
                helpers.top().constantBuilder.pop_back();
                if (helpers.top().functionBuilder.empty())
                {
                    throw InvalidQuerySyntax("Sample probe requires sample fields and data types as arguments at {}", context->getText());
                }
                Schema sampleSchema;
                std::ranges::reverse_view functionBuilderReversed{helpers.top().functionBuilder};
                for (size_t i = 0; i < functionBuilderReversed.size(); i += 2)
                {
                    auto nameFn = helpers.top().functionBuilder.at(i);
                    auto datatypeFn = helpers.top().functionBuilder.at(i + 1);
                    INVARIANT(
                        nameFn.tryGetAs<FieldAccessLogicalFunction>().has_value()
                            and datatypeFn.tryGetAs<FieldAccessLogicalFunction>().has_value(),
                        "sample field/datatype was not a FieldAccessLogicalFunction");
                    auto nameFieldAccFn = nameFn.getAs<FieldAccessLogicalFunction>().get();
                    auto datatypeFieldAccFn = datatypeFn.getAs<FieldAccessLogicalFunction>().get();
                    auto datatype = DataTypeProvider::tryProvideDataType(datatypeFieldAccFn.getFieldName());
                    INVARIANT(datatype.has_value(), "Provided datatype {} was not a valid datatype!", datatypeFieldAccFn.getFieldName());
                    sampleSchema.addField(nameFieldAccFn.getFieldName(), datatype.value());
                }
                helpers.top().functionBuilder.clear();
                helpers.top().statProbe = ReservoirProbeLogicalOperator(statisticHash, sampleSchema);
                break;
            }
            else if (funcName == "EQUIWIDTHHISTOGRAM")
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected constant (statistic hash) as first argument of EQUIWIDTHHISTOGRAM function call, got nothing at {}",
                        context->getText());
                }
                const uint64_t statisticHash = parseConstant(helpers.top().constantBuilder.front(), "statisticHash");
                helpers.top().statisticHash = statisticHash;
                if (helpers.top().functionBuilder.size() != 2
                    && helpers.top().functionBuilder.back().tryGetAs<FieldAccessLogicalFunction>())
                {
                    throw InvalidQuerySyntax(
                        "EQUIWIDTHHISTOGRAM requires the second argument to be a fieldname and the last to be a datatype in lower case");
                }
                auto counterDatatypeOption = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                INVARIANT(
                    counterDatatypeOption.tryGetAs<FieldAccessLogicalFunction>().has_value(),
                    "counter datatype was not a FieldAccessLogicalFunction");
                auto counterDatatypeFn = counterDatatypeOption.getAs<FieldAccessLogicalFunction>();
                auto counterDatatype = DataTypeProvider::tryProvideDataType(counterDatatypeFn.get().getFieldName());
                INVARIANT(counterDatatype.has_value(), "counter datatype was not a datatype");
                const auto fieldName = helpers.top().functionBuilder.back().tryGetAs<FieldAccessLogicalFunction>().value().get();
                helpers.top().functionBuilder.pop_back();
                if (helpers.top().constantBuilder.size() != 4)
                {
                    throw InvalidQuerySyntax("EQUIWIDTHHISTOGRAM requires the arguments numBuckets, minValue, maxValue to be constants");
                }
                const auto maxValue = parseConstant(helpers.top().constantBuilder.back(), "maxValue");
                helpers.top().constantBuilder.pop_back();
                const auto minValue = parseConstant(helpers.top().constantBuilder.back(), "minValue");
                helpers.top().constantBuilder.pop_back();
                const auto numBuckets = parseConstant(helpers.top().constantBuilder.back(), "numBuckets");
                helpers.top().constantBuilder.pop_back();
                const auto asFieldIfNotOverwritten = FieldAccessLogicalFunction{
                    LogicalStatisticFields().statisticDataField.dataType, LogicalStatisticFields().statisticDataField.name};
                helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(EquiWidthHistogramLogicalFunction{
                    fieldName, asFieldIfNotOverwritten, numBuckets, minValue, maxValue, statisticHash, counterDatatype.value()}));
                break;
            }
            else if (funcName == "EQUIWIDTHHISTOGRAM_PROBE")
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected constant (sample hash) as first argument of EQUIWIDTH_PROBE function call, got nothing at {}",
                        context->getText());
                }
                const uint64_t statisticHash = parseConstant(helpers.top().constantBuilder.back(), "statisticHash");
                helpers.top().constantBuilder.pop_back();
                if (helpers.top().functionBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected counter datatype and start/end datatype as lowercase arguments, got {}", context->getText());
                }
                INVARIANT(
                    helpers.top().functionBuilder.size() >= 2, "Expected at least two elements: counter datatype and start/end datatype");
                auto startEndDatatypeOption = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto counterDatatypeOption = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                INVARIANT(
                    counterDatatypeOption.tryGetAs<FieldAccessLogicalFunction>().has_value()
                        and startEndDatatypeOption.tryGetAs<FieldAccessLogicalFunction>().has_value(),
                    "counter datatype or start/end datatype was not a FieldAccessLogicalFunction");
                auto counterDatatypeFn = counterDatatypeOption.getAs<FieldAccessLogicalFunction>();
                auto startEndDatatypeFn = startEndDatatypeOption.getAs<FieldAccessLogicalFunction>();
                auto counterDatatype = DataTypeProvider::tryProvideDataType(counterDatatypeFn.get().getFieldName());
                auto startEndDatatype = DataTypeProvider::tryProvideDataType(startEndDatatypeFn.get().getFieldName());
                INVARIANT(
                    counterDatatype.has_value() and startEndDatatype.has_value(),
                    "counter datatype or start/end datatype was not a datatype");
                helpers.top().statProbe
                    = EquiWidthHistogramProbeLogicalOperator(statisticHash, counterDatatype.value(), startEndDatatype.value());
                break;
            }
            else if (funcName == "COUNTMINSKETCH")
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected constant (statistic hash) as first argument of COUNTMINSKETCH function call, got nothing at {}",
                        context->getText());
                }
                if (helpers.top().functionBuilder.size() != 2
                    && helpers.top().functionBuilder.back().tryGetAs<FieldAccessLogicalFunction>())
                {
                    throw InvalidQuerySyntax("COUNTMINSKETCH requires the second argument to be a fieldname, and a datatype for the counter"
                                             "");
                }
                const auto counterTypeFn = helpers.top().functionBuilder.back().tryGetAs<FieldAccessLogicalFunction>().value().get();
                auto counterDatatype = DataTypeProvider::tryProvideDataType(counterTypeFn.getFieldName()).value();
                helpers.top().functionBuilder.pop_back();
                const auto fieldName = helpers.top().functionBuilder.back().tryGetAs<FieldAccessLogicalFunction>().value().get();
                helpers.top().functionBuilder.pop_back();
                const auto seed = parseConstant(helpers.top().constantBuilder.back(), "seed");
                helpers.top().constantBuilder.pop_back();
                const auto rows = parseConstant(helpers.top().constantBuilder.back(), "rows");
                helpers.top().constantBuilder.pop_back();
                const auto columns = parseConstant(helpers.top().constantBuilder.back(), "columns");
                helpers.top().constantBuilder.pop_back();
                const Statistic::StatisticHash statisticHash = parseConstant(helpers.top().constantBuilder.back(), "statisticHash");
                helpers.top().constantBuilder.pop_back();
                helpers.top().statisticHash = statisticHash;
                const auto asFieldIfNotOverwritten = FieldAccessLogicalFunction{
                    LogicalStatisticFields().statisticDataField.dataType, LogicalStatisticFields().statisticDataField.name};
                helpers.top().windowAggs.push_back(std::make_shared<WindowAggregationLogicalFunction>(CountMinSketchLogicalFunction{
                    fieldName, asFieldIfNotOverwritten, NumberOfCols{columns}, NumberOfRows{rows}, seed, counterDatatype, statisticHash}));
                break;
            }
            else if (funcName == "COUNTMIN_PROBE")
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "Expected constant (sample hash) as first argument of COUNTMIN_PROBE function call, got nothing at {}",
                        context->getText());
                }
                const uint64_t statisticHash = parseConstant(helpers.top().constantBuilder.back(), "statisticHash");
                helpers.top().constantBuilder.pop_back();
                if (helpers.top().functionBuilder.empty())
                {
                    throw InvalidQuerySyntax("Expected counter datatype as lowercase argument, got {}", context->getText());
                }
                auto counterDatatypeOption = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                INVARIANT(
                    counterDatatypeOption.tryGetAs<FieldAccessLogicalFunction>().has_value(),
                    "counter datatype was not a FieldAccessLogicalFunction");
                auto counterDatatypeFn = counterDatatypeOption.getAs<FieldAccessLogicalFunction>().get();
                auto counterDatatype = DataTypeProvider::tryProvideDataType(counterDatatypeFn.getFieldName());
                INVARIANT(counterDatatype.has_value(), "counter datatype was not a datatype");
                helpers.top().statProbe = CountMinSketchProbeLogicalOperator(statisticHash, counterDatatype.value());
                break;
            }
            else
            {
                const auto numArgs = context->argument.size();
                if (numArgs > helpers.top().functionBuilder.size())
                {
                    throw InvalidQuerySyntax(
                        "Function '{}' expects {} arguments but only {} are available",
                        funcName,
                        numArgs,
                        helpers.top().functionBuilder.size());
                }
                auto argsBegin = helpers.top().functionBuilder.end() - static_cast<std::ptrdiff_t>(numArgs);
                std::vector<LogicalFunction> funcArgs(argsBegin, helpers.top().functionBuilder.end());
                if (auto logicalFunction = LogicalFunctionProvider::tryProvide(funcName, std::move(funcArgs)))
                {
                    helpers.top().functionBuilder.resize(helpers.top().functionBuilder.size() - numArgs);
                    helpers.top().functionBuilder.push_back(*logicalFunction);
                }
                else
                {
                    throw InvalidQuerySyntax("Unknown (aggregation) function: {}, resolved to token type: {}", funcName, tokenType);
                }
            }
    }

    /// For aggregation functions, generate an auto-name for the result field and replace the raw
    /// field access in functionBuilder with a reference to the aggregation output. This enables
    /// post-aggregation arithmetic like MEDIAN(i8) + UINT64(1).
    if (isAggregation)
    {
        helpers.top().hasUnnamedAggregation = true;
        const auto& onField = helpers.top().functionBuilder.back().getAs<FieldAccessLogicalFunction>().get();
        const auto autoName = fmt::format("{}_{}", onField.getFieldName(), funcName);
        const auto asField = FieldAccessLogicalFunction(autoName);
        const auto aggFunc = helpers.top().windowAggs.back();
        helpers.top().windowAggs.pop_back();
        const auto newAggFunc = std::make_shared<WindowAggregationLogicalFunction>(aggFunc->withAsField(asField));
        helpers.top().windowAggs.push_back(newAggFunc);
        helpers.top().functionBuilder.back() = LogicalFunction(asField);
    }
}

void AntlrSQLQueryPlanCreator::exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext* context)
{
    helpers.top().minimumCount = std::stoi(context->getText());
}

void AntlrSQLQueryPlanCreator::enterInlineSource(AntlrSQLParser::InlineSourceContext* context)
{
    const auto type = bindIdentifier(context->type);

    const auto parameters = bindConfigOptions(context->parameters->namedConfigExpression());

    helpers.top().setInlineSource(type, parameters);
}

void AntlrSQLQueryPlanCreator::enterSetOperation(AntlrSQLParser::SetOperationContext*)
{
    AntlrSQLHelper helper;
    helper.isSetOperation = true;
    helpers.push(helper);
}

void AntlrSQLQueryPlanCreator::exitSetOperation(AntlrSQLParser::SetOperationContext* context)
{
    INVARIANT(!helpers.empty(), "the set operation helper should not disappear before this function call");

    auto& helperPlans = helpers.top().queryPlans;
    if (helperPlans.size() < 2)
    {
        throw InvalidQuerySyntax("Union does not have sufficient amount of QueryPlans for unifying.");
    }

    auto rightQuery = std::move(helperPlans.back());
    helperPlans.pop_back();
    auto leftQuery = std::move(helperPlans.back());
    helperPlans.pop_back();
    helpers.pop();

    auto queryPlan = LogicalPlanBuilder::addUnion(std::move(leftQuery), std::move(rightQuery));
    if (!helpers.empty())
    {
        /// we are in a subquery
        helpers.top().queryPlans.push_back(std::move(queryPlan));
    }
    else
    {
        queryPlans.push(std::move(queryPlan));
    }
    AntlrSQLBaseListener::exitSetOperation(context);
}

void AntlrSQLQueryPlanCreator::enterGroupByClause(AntlrSQLParser::GroupByClauseContext* context)
{
    helpers.top().isGroupBy = true;
    AntlrSQLBaseListener::enterGroupByClause(context);
}

void AntlrSQLQueryPlanCreator::exitGroupByClause(AntlrSQLParser::GroupByClauseContext* context)
{
    helpers.top().isGroupBy = false;
    AntlrSQLBaseListener::exitGroupByClause(context);
}
}
