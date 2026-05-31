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

#include <Repl.hpp>

#include <algorithm>
#include <array>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>

#include <CollectionDomain.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RequestStatisticStatement.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Statements/JsonOutputFormatter.hpp> /// NOLINT(misc-include-cleaner)
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementOutputAssembler.hpp>
#include <Statements/TextOutputFormatter.hpp> /// NOLINT(misc-include-cleaner)
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <replxx.hxx>

namespace NES
{

namespace
{

/// Tags a sink operator's formatConfig with workload-switch gate metadata. The compiler
/// reads "gate_switch_name"/"gate_expected_value" in LowerToCompiledQueryPlanPhase::processSink
/// and binds the predecessor pipeline stage's gateAtomic via SwitchRegistry.
///
/// SinkLogicalOperator from the parser usually carries only a name; the descriptor is bound
/// by SinkBindingRule downstream. We resolve the descriptor up-front via the sink catalog so
/// the tagged operator has the full descriptor and SinkBindingRule treats it as already bound.
LogicalOperator tagSinkWithGate(
    const LogicalOperator& sinkOp,
    const std::string& switchName,
    int64_t expectedValue,
    const SinkCatalog& sinkCatalog)
{
    const auto typed = sinkOp.getAs<SinkLogicalOperator>();
    auto descriptor = typed->getSinkDescriptor();
    if (not descriptor.has_value())
    {
        descriptor = sinkCatalog.getSinkDescriptor(typed->getSinkName());
    }
    if (not descriptor.has_value())
    {
        throw UnknownSinkName(
            "Cannot tag sink '{}' with workload-switch gate: descriptor not found in catalog", typed->getSinkName());
    }
    const auto newDescriptor = descriptor->withMetadataEntries(
        {{"gate_switch_name", switchName}, {"gate_expected_value", std::to_string(expectedValue)}});
    return LogicalOperator{typed->withSinkDescriptor(newDescriptor).withChildren(sinkOp.getChildren())};
}

/// Builds a workload-switch merged plan from two LogicalPlans (`dataPlan` and `pairedPlan`),
/// each expected to be a single-source, single-sink filter chain over the SAME logical source.
///
/// The result has:
///   - The data plan's source operator shared between both chains (one source thread).
///   - Both sinks tagged with switch metadata: data sink → expected=0, paired sink → expected=1.
///   - Two root operators (one per chain) — multi-root plans are supported by the optimizer
///     after the LogicalPlan/SourceInferenceRule/DecideJoinTypesRule/DecideMemoryLayoutRule
///     and replaceOperator multi-root fixes.
///
/// Throws if either plan does not have exactly one source-name operator.
LogicalPlan buildWorkloadSwitchPlan(
    const LogicalPlan& dataPlan, const LogicalPlan& pairedPlan, const std::string& switchName, const SinkCatalog& sinkCatalog)
{
    const auto dataSources = getOperatorByType<SourceNameLogicalOperator>(dataPlan);
    const auto pairedSources = getOperatorByType<SourceNameLogicalOperator>(pairedPlan);
    if (dataSources.size() != 1)
    {
        throw NotImplemented("Workload-switch merger requires the data query to have exactly one source (got {})", dataSources.size());
    }
    if (pairedSources.size() != 1)
    {
        throw NotImplemented("Workload-switch merger requires the paired query to have exactly one source (got {})", pairedSources.size());
    }

    const LogicalOperator dataSource{dataSources.front()};
    const LogicalOperator pairedSourceOp{pairedSources.front()};

    /// Tag both sinks. Data plan's roots are sinks (single root) — same for pairedPlan.
    auto dataPlanWithGate = dataPlan;
    {
        const auto& roots = dataPlanWithGate.getRootOperators();
        INVARIANT(roots.size() == 1, "Data plan must have exactly one root (sink), got {}", roots.size());
        const auto taggedSink = tagSinkWithGate(roots.front(), switchName, 0, sinkCatalog);
        auto result = replaceOperator(dataPlanWithGate, roots.front().getId(), taggedSink);
        INVARIANT(result.has_value(), "Failed to tag data plan's sink with gate metadata");
        dataPlanWithGate = std::move(*result);
    }

    auto pairedPlanWithGate = pairedPlan;
    {
        const auto& roots = pairedPlanWithGate.getRootOperators();
        INVARIANT(roots.size() == 1, "Paired plan must have exactly one root (sink), got {}", roots.size());
        const auto taggedSink = tagSinkWithGate(roots.front(), switchName, 1, sinkCatalog);
        auto result = replaceOperator(pairedPlanWithGate, roots.front().getId(), taggedSink);
        INVARIANT(result.has_value(), "Failed to tag paired plan's sink with gate metadata");
        pairedPlanWithGate = std::move(*result);
    }

    /// Replace the paired plan's source with the data plan's source operator (sharing OperatorId)
    /// so the optimizer recognizes them as the same source after the merge.
    auto sharedPairedResult = replaceSubtree(pairedPlanWithGate, pairedSourceOp.getId(), dataSource);
    INVARIANT(sharedPairedResult.has_value(), "Failed to splice shared source into paired plan");
    const auto sharedPairedPlan = std::move(*sharedPairedResult);

    return addRootOperators(dataPlanWithGate, sharedPairedPlan.getRootOperators());
}

}

struct Repl::Impl
{
    SourceStatementHandler sourceStatementHandler;
    SinkStatementHandler sinkStatementHandler;
    TopologyStatementHandler topologyStatementHandler;
    std::shared_ptr<QueryStatementHandler> queryStatementHandler;
    StatisticRequestHandler statisticRequestHandler;
    StatementBinder binder;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::stop_token stopToken;
    std::optional<RequestStatisticBuildStatement> companionStatisticRequest;
    std::optional<std::function<void(DistributedQueryId, const std::string&, Statistic::StatisticId)>> onCompanionAssociatedWithQuery;

    std::unique_ptr<replxx::Replxx> rx;
    std::vector<std::string> history;
    bool interactiveMode = true;
    ErrorBehaviour errorBehaviour;
    StatementOutputFormat defaultOutputFormat;
    unsigned int exitCode = 0;

    /// Commands
    static constexpr const char* HELP_CMD = "help";
    static constexpr const char* QUIT_CMD = "quit";
    static constexpr const char* EXIT_CMD = "exit";
    static constexpr const char* CLEAR_CMD = "clear";

    /// NOLINTBEGIN(readability-convert-member-functions-to-static)

    Impl(
        SourceStatementHandler sourceStatementHandler,
        SinkStatementHandler sinkStatementHandler,
        TopologyStatementHandler topologyStatementHandler,
        std::shared_ptr<QueryStatementHandler> queryStatementHandler,
        StatisticRequestHandler statisticRequestHandler,
        StatementBinder binder,
        std::shared_ptr<SinkCatalog> sinkCatalog,
        const ErrorBehaviour errorBehaviour,
        const StatementOutputFormat defaultOutputFormat,
        const bool interactiveMode,
        std::stop_token stopToken,
        std::optional<RequestStatisticBuildStatement> companionStatisticRequest,
        std::optional<std::function<void(DistributedQueryId, const std::string&, Statistic::StatisticId)>> onCompanionAssociatedWithQuery)
        : sourceStatementHandler(std::move(sourceStatementHandler))
        , sinkStatementHandler(std::move(sinkStatementHandler))
        , topologyStatementHandler(std::move(topologyStatementHandler))
        , queryStatementHandler(std::move(queryStatementHandler))
        , statisticRequestHandler(std::move(statisticRequestHandler))
        , binder(std::move(binder))
        , sinkCatalog(std::move(sinkCatalog))
        , stopToken(std::move(stopToken))
        , companionStatisticRequest(std::move(companionStatisticRequest))
        , onCompanionAssociatedWithQuery(std::move(onCompanionAssociatedWithQuery))
        , interactiveMode(interactiveMode)
        , errorBehaviour(errorBehaviour)
        , defaultOutputFormat(defaultOutputFormat)
    {
        if (interactiveMode)
        {
            setupReplxx();
        }
        else
        {
            NES_INFO("Non-interactive mode detected (not a TTY). Using basic input mode.\n");
        }
    }

    void setupReplxx()
    {
        rx = std::make_unique<replxx::Replxx>();

        rx->set_word_break_characters(" \t\n\r");

        /// Set up hints
        rx->set_hint_callback(
            [](const std::string& input, int&, replxx::Replxx::Color& color) -> std::vector<std::string>
            {
                if (input.empty())
                {
                    return {};
                }

                const std::vector<std::string> commands = {"help", "quit", "exit", "clear"};
                for (const auto& cmd : commands)
                {
                    if (input.starts_with(cmd))
                    {
                        color = replxx::Replxx::Color::BLUE;
                        return {" (command)"};
                    }
                }

                return {};
            });

        rx->history_load(".nebuli_history");
    }

    void printWelcome()
    {
        const bool useColour = isatty(STDOUT_FILENO) != 0;
        auto color = [&](const char* esc) { return useColour ? esc : ""; };
        const char* bold = color("\033[1m");
        const char* accent = color("\033[34m");
        const char* reset = color("\033[0m");

        constexpr std::string_view title = "NebulaStream Interactive Query Shell";
        constexpr std::size_t width = 60;
        const std::size_t pad = (width - title.size()) / 2;

        std::cout << '\n' << accent << std::string(width, '=') << '\n';
        std::cout << std::string(pad, ' ') << bold << title << reset << '\n';
        std::cout << accent << std::string(width, '=') << reset << '\n';

        struct Cmd
        {
            const char* name;
            const char* desc;
        };

        constexpr std::array<Cmd, 4> cmds{
            {{.name = "help", .desc = "Show this help message"},
             {.name = "clear", .desc = "Clear the screen"},
             {.name = "quit", .desc = "Exit the shell"},
             {.name = "exit", .desc = "Alias for quit"}}};

        std::cout << bold << "Commands" << reset << ":\n";
        for (auto [name, desc] : cmds)
        {
            std::cout << "  • " << bold << name << reset << std::string(8 - std::strlen(name), ' ') << "─ " << desc << '\n';
        }
        std::cout << '\n'
                  << "Enter SQL to execute it; multi‑line statements are supported and\n"
                  << "run automatically once the final line ends with a semicolon.\n\n";
    }

    void printHelp()
    {
        const bool useColour = isatty(STDOUT_FILENO) != 0;
        auto color = [&](const char* esc) { return useColour ? esc : ""; };

        const char* bold = color("\033[1m");
        const char* reset = color("\033[0m");
        const char* accent = color("\033[34m");

        struct Cmd
        {
            const char* name;
            const char* desc;
        };

        constexpr std::array<Cmd, 4> cmds{
            {{.name = "help", .desc = "Show this help message"},
             {.name = "clear", .desc = "Clear the screen"},
             {.name = "quit", .desc = "Exit the shell"},
             {.name = "exit", .desc = "Alias for quit"}}};

        std::size_t padWidth = 0;
        for (const auto& cmd : cmds)
        {
            padWidth = std::max(padWidth, std::strlen(cmd.name));
        }
        padWidth += 2;

        std::cout << '\n' << bold << "Commands" << reset << ":\n";
        for (const auto& cmd : cmds)
        {
            std::cout << "  " << bold << cmd.name << reset << std::string(padWidth - std::strlen(cmd.name), ' ') << "─ " << cmd.desc
                      << '\n';
        }

        std::cout << '\n'
                  << "Enter SQL to execute it; multi‑line statements are supported and\n"
                  << "run automatically once the final line ends with a semicolon.\n\n"
                  << "Docs: " << accent << "https://docs.nebula.stream/" << reset << "\n\n";
    }

    /// This method should handle "regular" errors, such as from parsing user input or being unable to execute statements.
    /// The try-catch in the main-loop should only catch unexpected errors.
    void handleError(auto error)
    {
        if (errorBehaviour == ErrorBehaviour::FAIL_FAST)
        {
            throw error;
        }
        if (errorBehaviour == ErrorBehaviour::CONTINUE_AND_FAIL)
        {
            exitCode = 1;
        }
        NES_ERROR("Error encountered: {}", error.what());
        std::cout << fmt::format("Error encountered: {}", error.what());
    }

    void clearScreen() const
    {
        constexpr const char* ansiClear = "\033[2J\033[H";
        if (interactiveMode)
        {
            rx->clear_screen();
        }
        else
        {
            std::cout << ansiClear << std::flush;
        }
    }

    [[nodiscard]] std::string getPrompt() const { return "NES 🌌 > "; }

    [[nodiscard]] bool isCommand(const std::string& input)
    {
        std::istringstream iss(input);
        std::string cmd;
        iss >> cmd;

        return cmd == HELP_CMD || cmd == QUIT_CMD || cmd == EXIT_CMD || cmd == CLEAR_CMD;
    }

    bool handleCommand(const std::string& input)
    {
        std::istringstream iss(input);
        std::string cmd;
        iss >> cmd;

        if (cmd == HELP_CMD)
        {
            printHelp();
            return false;
        }

        if (cmd == QUIT_CMD || cmd == EXIT_CMD)
        {
            if (interactiveMode)
            {
                std::cout << "Goodbye!\n";
            }
            return true;
        }

        if (cmd == CLEAR_CMD)
        {
            clearScreen();
            return false;
        }
        return false;
    }

    [[nodiscard]] std::string readMultiLineQuery(const std::string& firstLine) const
    {
        PRECONDITION(!firstLine.empty(), "first line may not be empty.");

        std::string query;
        std::string line;
        ssize_t parenCount = 0;
        bool inString = false;
        char stringChar = 0;

        while (true)
        {
            if (query.empty())
            {
                line = firstLine;
            }
            else if (!interactiveMode)
            {
                /// Use std::getline for non-interactive mode
                std::getline(std::cin, line);
                if (std::cin.eof())
                {
                    break;
                }
            }
            else
            {
                /// Use Replxx for interactive mode
                line = rx->input(getPrompt());
            }

            if (line.empty())
            {
                continue;
            }

            if (interactiveMode && !query.empty())
            {
                rx->history_add(line);
            }

            for (const char charInLine : line)
            {
                if (inString)
                {
                    if (charInLine == stringChar)
                    {
                        inString = false;
                        stringChar = 0;
                    }
                }
                else
                {
                    if (charInLine == '\'' || charInLine == '"')
                    {
                        inString = true;
                        stringChar = charInLine;
                    }
                    else if (charInLine == '(')
                    {
                        parenCount++;
                    }
                    else if (charInLine == ')')
                    {
                        parenCount--;
                    }
                }
            }

            query += line + "\n";

            if (parenCount > 0 || inString)
            {
                continue;
            }

            if (parenCount < 0)
            {
                throw QueryInvalid("too many closing parantesis");
            }

            /// Check if the line ends with a semicolon
            if (!line.empty() && line.back() == ';')
            {
                break;
            }
        }
        return query;
    }

    bool executeQuery(const std::string& query)
    {
        auto managedParser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(query);
        auto parseResult = managedParser->parseMultiple();
        if (not parseResult.has_value())
        {
            handleError(std::move(parseResult.error()));
            return false;
        }
        auto toHandle = parseResult.value() | std::views::transform([this](const auto& stmt) { return binder.bind(stmt.get()); })
            | std::ranges::to<std::vector>();

        for (auto& bindingResult : toHandle)
        {
            if (not bindingResult.has_value())
            {
                handleError(std::move(bindingResult.error()));
                continue;
            }
            /// NOLINTNEXTLINE(fuchsia-trailing-return)
            auto visitor = [&](const auto& stmt) -> std::expected<NES::StatementResult, NES::Exception>
            {
                if constexpr (requires { sourceStatementHandler.apply(stmt); })
                {
                    return sourceStatementHandler.apply(stmt);
                }
                else if constexpr (requires { sinkStatementHandler.apply(stmt); })
                {
                    return sinkStatementHandler.apply(stmt);
                }
                else if constexpr (requires { topologyStatementHandler.apply(stmt); })
                {
                    return topologyStatementHandler.apply(stmt);
                }
                else if constexpr (requires { statisticRequestHandler.apply(stmt); })
                {
                    return statisticRequestHandler.apply(stmt);
                }
                else if constexpr (requires { queryStatementHandler->apply(stmt); })
                {
                    if constexpr (std::is_same_v<std::remove_cvref_t<decltype(stmt)>, QueryStatement>)
                    {
                        /// Workload-domain companion: splice the build branch into the data query's
                        /// LogicalPlan and submit the merged plan as the data query. Only one source
                        /// thread runs; the engine fans buffers out to both subtrees via the multi-
                        /// successor pipeline emit path (QueryEngine.cpp:512). The companion is not
                        /// deployed as a separate query.
                        if (companionStatisticRequest.has_value()
                            && std::holds_alternative<WorkloadDomain>(companionStatisticRequest->domain))
                        {
                            try
                            {
                                /// Workload-switch merger: if a paired SQL is supplied (via
                                /// --companion-switch-to-sql, stashed by ReplStarter in the request's
                                /// options as "paired_sql"), parse it and merge both filter chains into
                                /// a single shared-source plan with gate-tagged sinks. The swap callback
                                /// flips the named switch instead of redeploying.
                                LogicalPlan dataQueryPlan = stmt.plan;
                                if (const auto pairedIt = companionStatisticRequest->options.find("paired_sql");
                                    pairedIt != companionStatisticRequest->options.end() && not pairedIt->second.empty())
                                {
                                    const auto switchIt = companionStatisticRequest->options.find("switch_name");
                                    const std::string switchName
                                        = (switchIt != companionStatisticRequest->options.end()) ? switchIt->second : "filter_order";
                                    auto pairedBind = binder.parseAndBindSingle(pairedIt->second);
                                    if (not pairedBind.has_value())
                                    {
                                        return std::unexpected<Exception>(std::move(pairedBind.error()));
                                    }
                                    auto* pairedQuery = std::get_if<QueryStatement>(&pairedBind.value());
                                    if (pairedQuery == nullptr)
                                    {
                                        return std::unexpected<Exception>(Exception(
                                            "Workload-switch paired SQL must be a SELECT statement", ErrorCode::UnknownException));
                                    }
                                    dataQueryPlan = buildWorkloadSwitchPlan(stmt.plan, pairedQuery->plan, switchName, *sinkCatalog);
                                    std::cout << "[Statistic] Workload-switch merged plan: 2 filter chains gated by '" << switchName
                                              << "' (data=0, paired=1), merged roots=" << dataQueryPlan.getRootOperators().size() << "\n";
                                    std::stringstream ss;
                                    ss << dataQueryPlan;
                                    std::cout << "[Statistic DEBUG] merged plan:\n" << ss.str() << "\n";
                                    std::flush(std::cout);
                                }

                                auto submitMerged = [&](LogicalPlan mergedPlan) -> std::expected<QueryId, Exception>
                                {
                                    QueryStatement mergedStmt = stmt;
                                    mergedStmt.plan = std::move(mergedPlan);
                                    auto submitResult = queryStatementHandler->apply(mergedStmt);
                                    if (not submitResult.has_value())
                                    {
                                        return std::unexpected(submitResult.error());
                                    }
                                    return QueryId::createDistributed(submitResult->id);
                                };
                                /// Read the probe heartbeat interval from the request's options map
                                /// (stashed there by ReplStarter from --companion-probe-interval-ms).
                                /// Falls back to 10000ms if absent or unparseable.
                                uint64_t probeIntervalMs = 10000;
                                if (const auto it = companionStatisticRequest->options.find("probe_interval_ms");
                                    it != companionStatisticRequest->options.end())
                                {
                                    try { probeIntervalMs = std::stoull(it->second); }
                                    catch (...) { /* keep default */ }
                                }
                                auto statResult = statisticRequestHandler.collectWorkloadStatistic(
                                    *companionStatisticRequest, dataQueryPlan, submitMerged, probeIntervalMs);
                                if (not statResult.has_value())
                                {
                                    std::cout << "[Statistic] Failed to deploy workload companion: " << statResult.error().what()
                                              << "\n";
                                    std::flush(std::cout);
                                    return std::unexpected<Exception>(std::move(statResult.error()));
                                }
                                std::cout << "[Statistic] Workload companion deployed: id="
                                          << statResult->statisticId.getRawValue()
                                          << (statResult->alreadyExisted ? " (reused existing)" : " (new)") << "\n";
                                std::flush(std::cout);
                                QueryStatementResult merged{.id = statResult->queryId.getDistributedQueryId()};
                                if (onCompanionAssociatedWithQuery.has_value())
                                {
                                    (*onCompanionAssociatedWithQuery)(merged.id, query, statResult->statisticId);
                                }
                                return std::expected<QueryStatementResult, Exception>{merged};
                            }
                            catch (const std::exception& e)
                            {
                                std::cout << "[Statistic] Exception deploying workload companion: " << e.what() << "\n";
                                std::flush(std::cout);
                                return std::unexpected(Exception(e.what(), ErrorCode::UnknownException));
                            }
                        }
                    }

                    /// Default path: deploy the data query as-is, then optionally deploy a DataDomain
                    /// companion as a separate statistic-collection query.
                    auto result = queryStatementHandler->apply(stmt);
                    if (result.has_value() && companionStatisticRequest.has_value())
                    {
                        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(stmt)>, QueryStatement>)
                        {
                            try
                            {
                                auto statResult = statisticRequestHandler.collectNewStatistic(*companionStatisticRequest);
                                if (statResult.has_value())
                                {
                                    std::cout << "[Statistic] Companion deployed: id=" << statResult->statisticId.getRawValue()
                                              << (statResult->alreadyExisted ? " (reused existing)" : " (new)") << "\n";
                                    std::flush(std::cout);
                                    if (onCompanionAssociatedWithQuery.has_value())
                                    {
                                        (*onCompanionAssociatedWithQuery)(result.value().id, query, statResult->statisticId);
                                    }
                                }
                                else
                                {
                                    std::cout << "[Statistic] Failed to deploy companion: " << statResult.error().what() << "\n";
                                    std::flush(std::cout);
                                }
                            }
                            catch (const std::exception& e)
                            {
                                std::cout << "[Statistic] Exception deploying companion: " << e.what() << "\n";
                                std::flush(std::cout);
                            }
                        }
                    }
                    return result;
                }
                else
                {
                    static_assert(false, "All statement types need to have a handler");
                    std::unreachable();
                }
            };
            auto result = std::visit(visitor, bindingResult.value());
            if (not result.has_value())
            {
                handleError(std::move(result.error()));
                continue;
            }
            switch (getOutputFormat(bindingResult.value()).value_or(defaultOutputFormat))
            {
                case NES::StatementOutputFormat::TEXT:
                    std::cout << std::visit(
                        [](const auto& statementResult)
                        {
                            return toText(
                                StatementOutputAssembler<std::remove_cvref_t<decltype(statementResult)>>{}.convert(statementResult));
                        },
                        result.value());
                    break;
                case NES::StatementOutputFormat::JSON:
                    std::cout << std::visit(
                        [](const auto& statementResult)
                        {
                            nlohmann::json output
                                = NES::StatementOutputAssembler<std::remove_cvref_t<decltype(statementResult)>>{}.convert(statementResult);
                            return output;
                        },
                        result.value())
                              << "\n";
            }
            std::flush(std::cout);
        }
        return true;
    }

    void run()
    {
        if (interactiveMode)
        {
            printWelcome();
        }

        while (!stopToken.stop_requested())
        {
            try
            {
                std::string input;

                if (!interactiveMode)
                {
                    /// Use std::getline for non-interactive mode to avoid terminal issues
                    if (!std::getline(std::cin, input))
                    {
                        if (std::cin.eof())
                        {
                            break;
                        }

                        continue;
                    }
                }
                else
                {
                    /// Use Replxx for interactive mode
                    const auto* const result = rx->input(getPrompt());
                    if (result == nullptr)
                    {
                        /// EoF reached
                        return;
                    }

                    input = result;
                }

                if (input.empty())
                {
                    continue;
                }

                /// Add to history (only in interactive mode)
                if (interactiveMode)
                {
                    rx->history_add(input);
                }

                /// Check if it's a command
                if (isCommand(input))
                {
                    if (handleCommand(input))
                    {
                        break;
                    }
                    continue;
                }

                /// Check if it's a single-line SQL query
                auto trim = [](const std::string& str) -> std::string
                {
                    const size_t start = str.find_first_not_of(" \t\n\r");
                    if (start == std::string::npos)
                    {
                        return "";
                    }
                    const size_t end = str.find_last_not_of(" \t\n\r");
                    return str.substr(start, end - start + 1);
                };
                auto isCompleteStatement = [&](const std::string& stmt) -> bool
                {
                    std::string trimmed = trim(stmt);
                    return !trimmed.empty() && trimmed.back() == ';';
                };
                if (isCompleteStatement(input))
                {
                    executeQuery(input);
                }
                else
                {
                    const std::string fullQuery = readMultiLineQuery(input);
                    executeQuery(fullQuery);
                }
            }
            catch (const Exception& e)
            {
                if (errorBehaviour == ErrorBehaviour::FAIL_FAST)
                {
                    throw;
                }
                std::cout << "Error: " << e.what() << "\n";
            }
        }

        if (interactiveMode)
        {
            rx->history_save(".nebuli_history");
        }
    }
};

Repl::Repl(
    SourceStatementHandler sourceStatementHandler,
    SinkStatementHandler sinkStatementHandler,
    TopologyStatementHandler topologyStatementHandler,
    std::shared_ptr<QueryStatementHandler> queryStatementHandler,
    StatisticRequestHandler statisticRequestHandler,
    StatementBinder binder,
    std::shared_ptr<SinkCatalog> sinkCatalog,
    ErrorBehaviour errorBehaviour,
    StatementOutputFormat defaultOutputFormat,
    bool interactiveMode,
    std::stop_token stopToken,
    std::optional<RequestStatisticBuildStatement> companionStatisticRequest,
    std::optional<std::function<void(DistributedQueryId, const std::string&, Statistic::StatisticId)>> onCompanionAssociatedWithQuery)
    : impl(std::make_unique<Impl>(
          std::move(sourceStatementHandler),
          std::move(sinkStatementHandler),
          std::move(topologyStatementHandler),
          std::move(queryStatementHandler),
          std::move(statisticRequestHandler),
          std::move(binder),
          std::move(sinkCatalog),
          errorBehaviour,
          defaultOutputFormat,
          interactiveMode,
          std::move(stopToken),
          std::move(companionStatisticRequest),
          std::move(onCompanionAssociatedWithQuery)))
{
}

void Repl::run()
{
    impl->run();
}

Repl::~Repl() = default;

/// NOLINTEND(readability-convert-member-functions-to-static)

}
