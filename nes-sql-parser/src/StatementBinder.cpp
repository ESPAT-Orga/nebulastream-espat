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

#include <SQLQueryParser/StatementBinder.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <Util/Strings.hpp>

#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <CollectionDomain.hpp>
#include <Metric.hpp>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <BailErrorStrategy.h>
#include <CommonTokenStream.h>
#include <Exceptions.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/URI.hpp>
#include <ErrorHandling.hpp>

#include <CommonParserFunctions.hpp>
#include <RequestStatisticStatement.hpp>

namespace NES
{


/// NOLINTBEGIN(readability-convert-member-functions-to-static)
class StatementBinder::Impl
{
    std::shared_ptr<const SourceCatalog> sourceCatalog;
    std::function<LogicalPlan(AntlrSQLParser::QueryContext*)> queryBinder;

public:
    using Literal = std::variant<std::string, int64_t, uint64_t, double, bool>;

    Impl(
        const std::shared_ptr<const SourceCatalog>& sourceCatalog,
        const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryBinder)
        : sourceCatalog(sourceCatalog), queryBinder(queryBinder)
    {
    }

    ~Impl() = default;

    /// TODO #897 replace with normal comparison binding
    std::pair<std::string, Literal> bindShowFilter(const AntlrSQLParser::ShowFilterContext* showFilterAST) const
    {
        return {bindIdentifier(showFilterAST->attr), bindLiteral(showFilterAST->value)};
    }

    std::pair<std::string, Literal> bindDropFilter(const AntlrSQLParser::DropFilterContext* dropFilterAST) const
    {
        return {bindIdentifier(dropFilterAST->attr), bindLiteral(dropFilterAST->value)};
    }

    StatementOutputFormat bindFormat(AntlrSQLParser::ShowFormatContext* formatAST) const
    {
        if (formatAST->TEXT() != nullptr)
        {
            return StatementOutputFormat::TEXT;
        }
        if (formatAST->JSON() != nullptr)
        {
            return StatementOutputFormat::JSON;
        }
        INVARIANT(false, "Invalid format type, is the binder out of sync or was a nullptr passed?");
        std::unreachable();
    }

    CreateLogicalSourceStatement
    bindCreateLogicalSourceStatement(AntlrSQLParser::CreateLogicalSourceDefinitionContext* logicalSourceDefAST) const
    {
        const auto sourceName = bindIdentifier(logicalSourceDefAST->sourceName->strictIdentifier());
        const auto schema = bindSchema(logicalSourceDefAST->schemaDefinition());
        return CreateLogicalSourceStatement{.name = sourceName, .schema = schema};
    }

    CreatePhysicalSourceStatement
    bindCreatePhysicalSourceStatement(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* physicalSourceDefAST) const
    {
        const auto logicalSourceName = LogicalSourceName(bindIdentifier(physicalSourceDefAST->logicalSource->strictIdentifier()));
        /// TODO #764 use normal identifiers for types
        const std::string type = physicalSourceDefAST->type->getText();
        auto configOptions = [&]()
        {
            if (physicalSourceDefAST->optionsClause() != nullptr)
            {
                return bindConfigOptions(physicalSourceDefAST->optionsClause()->options->namedConfigExpression());
            }
            return ConfigMap{};
        }();

        const auto parserConfig = getParserConfig(configOptions);
        auto sourceConfig = getSourceConfig(configOptions);

        /// "host" determines worker placement, not source behavior — extract it from the config map into a dedicated field.
        std::optional<Host> host;
        if (auto it = sourceConfig.find("host"); it != sourceConfig.end())
        {
            host = Host(it->second);
            sourceConfig.erase(it);
        }

        return CreatePhysicalSourceStatement{
            .attachedTo = logicalSourceName, .sourceType = type, .host = host, .sourceConfig = sourceConfig, .parserConfig = parserConfig};
    }

    CreateWorkerStatement bindCreateWorkerStatement(AntlrSQLParser::CreateWorkerDefinitionContext* workerDefAST) const
    {
        auto configs = (workerDefAST->optionsClause() != nullptr)
            ? bindConfigOptionsWithDuplicates(workerDefAST->optionsClause()->options->namedConfigExpression())
            : ConfigMultiMap{};

        auto capacity = [&] -> std::optional<size_t>
        {
            auto it = std::ranges::find_if(configs, [](const auto& key) { return key.first.size() == 1 && key.first[0] == "CAPACITY"; });
            if (it != configs.end())
            {
                auto* literalOpt = std::get_if<Literal>(&it->second);
                if (literalOpt && std::holds_alternative<uint64_t>(*literalOpt))
                {
                    return static_cast<size_t>(std::get<uint64_t>(*literalOpt));
                }
                throw InvalidQuerySyntax("Capacity must be an unsigned integer literal");
            }
            return std::nullopt;
        }();

        auto dataAddress = [&] -> std::string
        {
            auto it = std::ranges::find_if(configs, [](const auto& key) { return key.first.size() == 1 && key.first[0] == "DATA"; });
            if (it != configs.end())
            {
                const Literal* literalOpt = std::get_if<Literal>(&it->second);
                if (literalOpt && std::holds_alternative<std::string>(*literalOpt))
                {
                    return URI(std::get<std::string>(*literalOpt)).toString();
                }
                throw InvalidQuerySyntax("DATA must be a string literal");
            }
            return {};
        }();

        auto downStreams = [&] -> std::vector<std::string>
        {
            return configs
                | std::views::filter([](const auto& option) { return option.first.size() == 1 && option.first[0] == "DOWNSTREAM"; })
                | std::views::values
                | std::views::transform(
                       [](const auto& value)
                       {
                           const Literal* literalOpt = std::get_if<Literal>(&value);
                           if (literalOpt && std::holds_alternative<std::string>(*literalOpt))
                           {
                               return URI(std::get<std::string>(*literalOpt)).toString();
                           }
                           throw InvalidQuerySyntax("DOWNSTREAM must be a string literal");
                       })
                | std::ranges::to<std::vector<std::string>>();
        }();


        return CreateWorkerStatement{
            .host = URI(bindStringLiteral(workerDefAST->hostaddr)).toString(),
            .dataAddress = std::move(dataAddress),
            .capacity = capacity,
            .downstream = downStreams,
            .config = {}};
    }

    CreateSinkStatement bindCreateSinkStatement(AntlrSQLParser::CreateSinkDefinitionContext* sinkDefAST) const
    {
        const auto sinkName = bindIdentifier(sinkDefAST->sinkName->strictIdentifier());
        const auto sinkType = sinkDefAST->type->getText();
        const auto configOptions = [&]()
        {
            if (sinkDefAST->optionsClause() != nullptr)
            {
                return bindConfigOptions(sinkDefAST->optionsClause()->options->namedConfigExpression());
            }
            return ConfigMap{};
        }();
        std::unordered_map<std::string, std::string> sinkOptions{};
        if (const auto sinkConfigIter = configOptions.find("SINK"); sinkConfigIter != configOptions.end())
        {
            sinkOptions
                = sinkConfigIter->second | std::views::filter([](auto& pair) { return std::holds_alternative<Literal>(pair.second); })
                | std::views::transform(
                      [](auto& pair) { return std::make_pair(toLowerCase(pair.first), literalToString(std::get<Literal>(pair.second))); })
                | std::ranges::to<std::unordered_map<std::string, std::string>>();
        }
        std::unordered_map<std::string, std::string> formatOptions{};
        if (const auto formatConfigIter = configOptions.find("PARSER"); formatConfigIter != configOptions.end())
        {
            formatOptions
                = formatConfigIter->second | std::views::filter([](auto& pair) { return std::holds_alternative<Literal>(pair.second); })
                | std::views::transform(
                      [](auto& pair) { return std::make_pair(toLowerCase(pair.first), literalToString(std::get<Literal>(pair.second))); })
                | std::ranges::to<std::unordered_map<std::string, std::string>>();
        }
        /// "host" determines worker placement, not sink behavior — extract it from the config map into a dedicated field.
        std::optional<Host> host;
        if (auto it = sinkOptions.find("host"); it != sinkOptions.end())
        {
            host = Host(it->second);
            sinkOptions.erase(it);
        }

        const auto schema = bindSchema(sinkDefAST->schemaDefinition());
        return CreateSinkStatement{
            .name = sinkName,
            .sinkType = sinkType,
            .schema = schema,
            .host = host,
            .sinkConfig = sinkOptions,
            .formatConfig = formatOptions};
    }

    Statement bindCreateStatement(AntlrSQLParser::CreateStatementContext* createAST) const
    {
        if (auto* const logicalSourceDefAST = createAST->createDefinition()->createLogicalSourceDefinition();
            logicalSourceDefAST != nullptr)
        {
            return bindCreateLogicalSourceStatement(logicalSourceDefAST);
        }
        if (auto* const physicalSourceDefAST = createAST->createDefinition()->createPhysicalSourceDefinition();
            physicalSourceDefAST != nullptr)
        {
            return bindCreatePhysicalSourceStatement(physicalSourceDefAST);
        }
        if (auto* const sinkDefAST = createAST->createDefinition()->createSinkDefinition(); sinkDefAST != nullptr)
        {
            return bindCreateSinkStatement(sinkDefAST);
        }
        if (auto* const workerDefAST = createAST->createDefinition()->createWorkerDefinition(); workerDefAST != nullptr)
        {
            return bindCreateWorkerStatement(workerDefAST);
        }
        throw InvalidStatement("Unrecognized CREATE statement");
    }

    ShowLogicalSourcesStatement bindShowLogicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::optional<StatementOutputFormat>{};
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "NAME")
            {
                throw InvalidQuerySyntax("Filter for SHOW LOGICAL SOURCES must be on name attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW LOGICAL SOURCES must be a string");
            }
            return ShowLogicalSourcesStatement{.name = std::get<std::string>(value), .format = format};
        }
        return ShowLogicalSourcesStatement{.name = {}, .format = format};
    }

    ShowPhysicalSourcesStatement bindShowPhysicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter,
        const AntlrSQLParser::ShowPhysicalSourcesSubjectContext* physicalSourcesSubject,
        AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        std::optional<LogicalSourceName> logicalSourceName{};
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::optional<StatementOutputFormat>{};
        if (physicalSourcesSubject->logicalSourceName != nullptr)
        {
            logicalSourceName = LogicalSourceName(bindIdentifier(physicalSourcesSubject->logicalSourceName));
        }
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "ID")
            {
                throw InvalidQuerySyntax("Filter for SHOW PHYSICAL SOURCES must be on id attribute");
            }
            if (not std::holds_alternative<uint64_t>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW PHYSICAL SOURCES must be an unsigned integer");
            }
            return ShowPhysicalSourcesStatement{.logicalSource = logicalSourceName, .id = std::get<uint64_t>(value), .format = format};
        }
        return ShowPhysicalSourcesStatement{.logicalSource = logicalSourceName, .id = {}, .format = format};
    }

    ShowSinksStatement
    bindShowSinksStatement(const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::optional<StatementOutputFormat>{};
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "NAME")
            {
                throw InvalidQuerySyntax("Filter for SHOW SINKS must be on name attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW SINKS must be a string");
            }
            return ShowSinksStatement{.name = std::get<std::string>(value), .format = format};
        }
        return ShowSinksStatement{.name = {}, .format = format};
    }

    ShowQueriesStatement
    bindShowQueriesStatement(const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::optional<StatementOutputFormat>{};
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "ID")
            {
                throw InvalidQuerySyntax("Filter for SHOW QUERIES must be on id attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW QUERIES must be a string");
            }
            return ShowQueriesStatement{.id = DistributedQueryId{std::get<std::string>(value)}, .format = format};
        }
        return ShowQueriesStatement{.id = {}, .format = format};
    }

    Statement bindShowStatement(AntlrSQLParser::ShowStatementContext* showAST) const
    {
        auto* showFilter = showAST->showFilter();

        if (const auto* logicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowLogicalSourcesSubjectContext*>(showAST->showSubject());
            logicalSourcesSubject != nullptr)
        {
            return bindShowLogicalSourcesStatement(showFilter, showAST->showFormat());
        }
        if (auto* physicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowPhysicalSourcesSubjectContext*>(showAST->showSubject());
            physicalSourcesSubject != nullptr)
        {
            return bindShowPhysicalSourcesStatement(showFilter, physicalSourcesSubject, showAST->showFormat());
        }
        if (const auto* queriesSubject = dynamic_cast<AntlrSQLParser::ShowQueriesSubjectContext*>(showAST->showSubject());
            queriesSubject != nullptr)
        {
            return bindShowQueriesStatement(showFilter, showAST->showFormat());
        }
        if (const auto* sinksSubject = dynamic_cast<AntlrSQLParser::ShowSinksSubjectContext*>(showAST->showSubject());
            sinksSubject != nullptr)
        {
            return bindShowSinksStatement(showFilter, showAST->showFormat());
        }
        throw InvalidStatement("Unrecognized SHOW statement");
    }

    Statement bindDropStatement(AntlrSQLParser::DropStatementContext* dropAst) const
    {
        const auto* const dropFilter = dropAst->dropFilter();
        PRECONDITION(dropFilter != nullptr, "Drop statement must have a WHERE filter");
        const auto [attr, value] = bindDropFilter(dropFilter);

        if (AntlrSQLParser::DropSourceContext* dropSourceAst = dropAst->dropSubject()->dropSource(); dropSourceAst != nullptr)
        {
            if (const auto* const logicalSourceSubject = dropSourceAst->dropLogicalSourceSubject(); logicalSourceSubject != nullptr)
            {
                if (attr != "NAME")
                {
                    throw InvalidQuerySyntax("Filter for DROP LOGICAL SOURCE must be on NAME attribute");
                }
                if (not std::holds_alternative<std::string>(value))
                {
                    throw InvalidQuerySyntax("Filter value for DROP LOGICAL SOURCE must be a string");
                }
                const auto logicalSourceName = LogicalSourceName(std::get<std::string>(value));
                return DropLogicalSourceStatement{logicalSourceName};
            }
            if (const auto* const physicalSourceSubject = dropSourceAst->dropPhysicalSourceSubject(); physicalSourceSubject != nullptr)
            {
                if (attr != "ID")
                {
                    throw InvalidQuerySyntax("Filter for DROP PHYSICAL SOURCE must be on ID attribute");
                }
                if (not std::holds_alternative<uint64_t>(value))
                {
                    throw InvalidQuerySyntax("Filter value for DROP PHYSICAL SOURCE must be an unsigned integer");
                }
                if (const auto physicalSource = sourceCatalog->getPhysicalSource(PhysicalSourceId{std::get<uint64_t>(value)});
                    physicalSource.has_value())
                {
                    return DropPhysicalSourceStatement{*physicalSource};
                }
                throw UnknownSourceName("There is no physical source with id {}", std::get<uint64_t>(value));
            }
        }
        else if (const auto* const dropQueryAst = dropAst->dropSubject()->dropQuery(); dropQueryAst != nullptr)
        {
            if (attr != "ID")
            {
                throw InvalidQuerySyntax("Filter for DROP QUERY must be on ID attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for DROP QUERY must be a string");
            }
            return DropQueryStatement{.id = DistributedQueryId(std::get<std::string>(value))};
        }
        else if (const auto* const dropSinkAst = dropAst->dropSubject()->dropSink(); dropSinkAst != nullptr)
        {
            if (attr != "NAME")
            {
                throw InvalidQuerySyntax("Filter for DROP SINK must be on NAME attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for DROP SINK must be a string");
            }
            const auto sinkName = std::get<std::string>(value);
            return DropSinkStatement{sinkName};
        }
        throw InvalidStatement("Unrecognized DROP statement");
    }

    /// Converts a timeUnit token type to a millisecond multiplier.
    static uint64_t timeUnitToMs(size_t tokenType)
    {
        switch (tokenType)
        {
            case AntlrSQLLexer::MS:
                return 1;
            case AntlrSQLLexer::SEC:
                return 1000;
            case AntlrSQLLexer::MINUTE:
                return 60000;
            case AntlrSQLLexer::HOUR:
                return 3600000;
            case AntlrSQLLexer::DAY:
                return 86400000;
            default:
                throw InvalidQuerySyntax("Unknown time unit in REQUEST STATISTIC window clause");
        }
    }

    /// Parses the metricType rule into a Metric enum.
    static Metric bindMetricType(AntlrSQLParser::MetricTypeContext* metricTypeAST)
    {
        if (metricTypeAST->CARDINALITY() != nullptr)
        {
            return Metric::Cardinality;
        }
        if (metricTypeAST->MINVAL() != nullptr)
        {
            return Metric::MinVal;
        }
        if (metricTypeAST->MAXVAL() != nullptr)
        {
            return Metric::MaxVal;
        }
        if (metricTypeAST->RATE() != nullptr)
        {
            return Metric::Rate;
        }
        if (metricTypeAST->AVERAGE() != nullptr)
        {
            return Metric::Average;
        }
        throw InvalidQuerySyntax("Unknown metric type in REQUEST STATISTIC");
    }

    /// Extracts window size (and optional advance) in milliseconds from a windowClause AST node.
    static std::pair<uint64_t, std::optional<uint64_t>> bindWindowClause(AntlrSQLParser::WindowClauseContext* windowClauseAST)
    {
        PRECONDITION(windowClauseAST != nullptr, "REQUEST STATISTIC requires a WINDOW clause");
        auto* windowSpec = windowClauseAST->windowSpec();
        PRECONDITION(windowSpec != nullptr, "Window spec must not be null");

        auto* timeBasedWindow = dynamic_cast<AntlrSQLParser::TimeBasedWindowContext*>(windowSpec);
        if (timeBasedWindow == nullptr)
        {
            throw InvalidQuerySyntax("REQUEST STATISTIC only supports time-based windows (TUMBLING or SLIDING)");
        }

        auto* timeWindow = timeBasedWindow->timeWindow();
        if (auto* tumbling = dynamic_cast<AntlrSQLParser::TumblingWindowContext*>(timeWindow); tumbling != nullptr)
        {
            auto* sizeParam = tumbling->sizeParameter();
            const auto sizeValue = std::stoull(sizeParam->INTEGER_VALUE()->getText());
            const auto timeUnitToken = sizeParam->timeUnit()->getStart()->getType();
            const auto sizeMs = sizeValue * timeUnitToMs(timeUnitToken);
            return {sizeMs, {}};
        }
        if (auto* sliding = dynamic_cast<AntlrSQLParser::SlidingWindowContext*>(timeWindow); sliding != nullptr)
        {
            auto* sizeParam = sliding->sizeParameter();
            const auto sizeValue = std::stoull(sizeParam->INTEGER_VALUE()->getText());
            const auto sizeTimeUnitToken = sizeParam->timeUnit()->getStart()->getType();
            const auto sizeMs = sizeValue * timeUnitToMs(sizeTimeUnitToken);

            auto* advanceParam = sliding->advancebyParameter();
            const auto advanceValue = std::stoull(advanceParam->INTEGER_VALUE()->getText());
            const auto advanceTimeUnitToken = advanceParam->timeUnit()->getStart()->getType();
            const auto advanceMs = advanceValue * timeUnitToMs(advanceTimeUnitToken);
            return {sizeMs, advanceMs};
        }
        throw InvalidQuerySyntax("REQUEST STATISTIC window must be TUMBLING or SLIDING");
    }

    /// Binds a REQUEST STATISTIC statement from the AST.
    RequestStatisticBuildStatement
    bindRequestStatisticBuildStatement(AntlrSQLParser::RequestStatisticStatementContext* requestStatAST) const
    {
        auto* characteristic = requestStatAST->requestCharacteristic();
        PRECONDITION(characteristic != nullptr, "requestCharacteristic must not be null");

        if (auto* dataChar = dynamic_cast<AntlrSQLParser::DataCharacteristicContext*>(characteristic); dataChar != nullptr)
        {
            const auto metric = bindMetricType(dataChar->metricType());
            const auto sourceName = bindIdentifier(dataChar->sourceName);
            const auto fieldName = bindIdentifier(dataChar->fieldName);
            const auto [windowSizeMs, windowAdvanceMs] = bindWindowClause(dataChar->windowClause());

            std::unordered_map<std::string, std::string> options;
            if (dataChar->optionsClause() != nullptr)
            {
                /// REQUEST STATISTIC options are flat or dotted keys (e.g. `"localhost:8080" AS host`,
                /// `256 AS sketch.columns`). Use the multi-map binder and join path segments with '.'
                /// so single-part keys like `host` are stored as-is.
                auto configOptions = bindConfigOptionsWithDuplicates(dataChar->optionsClause()->options->namedConfigExpression());
                for (const auto& [path, value] : configOptions)
                {
                    if (std::holds_alternative<Literal>(value) && !path.empty())
                    {
                        const auto key = fmt::format("{}", fmt::join(path | std::views::transform(toLowerCase), "."));
                        options[key] = literalToString(std::get<Literal>(value));
                    }
                }
            }

            std::optional<std::string> eventTimeFieldName;
            if (dataChar->eventTimeField != nullptr)
            {
                eventTimeFieldName = bindIdentifier(dataChar->eventTimeField);
            }

            DataDomain domain{.logicalSourceName = sourceName, .fieldName = fieldName};
            return RequestStatisticBuildStatement{
                .domain = domain,
                .metric = metric,
                .windowSizeMs = windowSizeMs,
                .windowAdvanceMs = windowAdvanceMs,
                .eventTimeFieldName = std::move(eventTimeFieldName),
                .conditionTrigger = {},
                .options = std::move(options)};
        }
        if (auto* workloadChar = dynamic_cast<AntlrSQLParser::WorkloadCharacteristicContext*>(characteristic); workloadChar != nullptr)
        {
            const auto queryId = workloadChar->queryId->getText();
            const auto operatorId = workloadChar->operatorId->getText();
            throw NotImplemented(
                "REQUEST STATISTIC WORKLOAD is not yet implemented. "
                "Requires extracting subplans from running queries (query {}, operator {}).",
                queryId,
                operatorId);
        }
        if (auto* infraChar = dynamic_cast<AntlrSQLParser::InfrastructureCharacteristicContext*>(characteristic); infraChar != nullptr)
        {
            const auto workerId = bindIdentifier(infraChar->workerId);
            throw NotImplemented(
                "REQUEST STATISTIC INFRASTRUCTURE is not yet implemented. "
                "Requires infrastructure metric sources for worker {}.",
                workerId);
        }
        throw InvalidStatement("Unrecognized REQUEST STATISTIC characteristic");
    }

    std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const
    {
        try
        {
            if (auto* const createAST = statementAST->createStatement(); createAST != nullptr)
            {
                return bindCreateStatement(createAST);
            }
            if (auto* showAST = statementAST->showStatement(); showAST != nullptr)
            {
                return bindShowStatement(showAST);
            }
            if (auto* dropAst = statementAST->dropStatement(); dropAst != nullptr)
            {
                return bindDropStatement(dropAst);
            }
            if (auto* const requestStatAST = statementAST->requestStatisticStatement(); requestStatAST != nullptr)
            {
                return bindRequestStatisticBuildStatement(requestStatAST);
            }
            if (auto* const explainStatementAST = statementAST->explainStatement())
            {
                INVARIANT(explainStatementAST->query() != nullptr, "Should be enforced by antlr");
                return ExplainQueryStatement{queryBinder(explainStatementAST->query())};
            }
            if (auto* const queryAst = statementAST->queryWithOptions(); queryAst != nullptr)
            {
                std::optional<DistributedQueryId> queryId;
                if (queryAst->optionsClause() != nullptr)
                {
                    auto options = bindConfigOptions(queryAst->optionsClause()->options->namedConfigExpression());
                    if (auto optionsIter = options.find("QUERY"); optionsIter != options.end())
                    {
                        if (auto idIter = optionsIter->second.find("ID"); idIter != optionsIter->second.end())
                        {
                            auto* literal = std::get_if<Literal>(&idIter->second);
                            if ((literal == nullptr) || !std::holds_alternative<std::string>(*literal))
                            {
                                throw InvalidQuerySyntax("Query id must be a string");
                            }
                            queryId = DistributedQueryId(std::get<std::string>(*literal));
                        }
                    }
                }
                return QueryStatement{.plan = queryBinder(queryAst->query()), .id = queryId};
            }

            throw InvalidStatement(statementAST->toString());
        }
        catch (Exception& e)
        {
            return std::unexpected{e};
        }
        catch (const std::exception& e)
        {
            return std::unexpected{InvalidStatement(e.what())};
        }
    }
};

StatementBinder::StatementBinder(
    const std::shared_ptr<const SourceCatalog>& sourceCatalog,
    const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryPlanBinder)
    : impl(std::make_unique<Impl>(sourceCatalog, queryPlanBinder))
{
}

StatementBinder::StatementBinder(StatementBinder&& other) noexcept : impl(std::move(other.impl))
{
}

StatementBinder& StatementBinder::operator=(StatementBinder&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    impl = std::move(other.impl);
    return *this;
}

StatementBinder::~StatementBinder() = default;

std::expected<Statement, Exception> StatementBinder::bind(AntlrSQLParser::StatementContext* statementAST) const
{
    return impl->bind(statementAST);
}

std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
StatementBinder::parseAndBind(const std::string_view statementString) const
{
    try
    {
        antlr4::ANTLRInputStream input(statementString.data(), statementString.length());
        AntlrSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        AntlrSQLParser parser(&tokens);
        /// Remove default error listeners that print to stdout/stderr
        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        /// Enable that antlr throws exceptions on parsing errors
        parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
        AntlrSQLParser::MultipleStatementsContext* tree = parser.multipleStatements();
        if (tree == nullptr)
        {
            return std::unexpected{InvalidQuerySyntax("{}", statementString)};
        }

        return std::expected<std::vector<std::expected<Statement, Exception>>, Exception>{
            tree->statement() | std::views::transform([this](auto* statementAST) { return impl->bind(statementAST); })
            | std::ranges::to<std::vector>()};
    }
    catch (antlr4::ParseCancellationException& e)
    {
        return std::unexpected{InvalidQuerySyntax("{}", e)};
    }
}

std::expected<Statement, Exception> StatementBinder::parseAndBindSingle(std::string_view statementStrings) const
{
    auto allParsed = parseAndBind(statementStrings);
    if (allParsed.has_value())
    {
        if (allParsed->size() > 1)
        {
            return std::unexpected{InvalidQuerySyntax("Expected a single statement, but got multiple")};
        }
        if (allParsed->empty())
        {
            return std::unexpected{InvalidQuerySyntax("Expected a single statement, but got none")};
        }
        return allParsed->at(0);
    }
    return std::unexpected{allParsed.error()};
}

std::ostream& operator<<(std::ostream& os, const CreatePhysicalSourceStatement& obj)
{
    return os << fmt::format(
               "CreatePhysicalSourceStatement: attachedTo: {} sourceType: {} host: {} sourceConfig: {} parserConfig: {}",
               obj.attachedTo,
               obj.sourceType,
               obj.host ? obj.host->getRawValue() : "<none>",
               obj.sourceConfig,
               obj.parserConfig);
}

/// NOLINTEND(readability-convert-member-functions-to-static)
}
