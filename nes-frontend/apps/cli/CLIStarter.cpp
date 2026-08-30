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

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeJson.hpp> ///NOLINT(misc-include-cleaner)
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/JsonOutputFormatter.hpp> ///NOLINT(misc-include-cleaner)
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementOutputAssembler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Signal.hpp>
#include <Util/Strings.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <DefaultStatisticQueryGenerator.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <Priority.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStateBackend.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticTuple.hpp>
#include <WorkerCatalog.hpp>

namespace
{
NES::DataType stringToFieldType(const std::string& fieldNodeType, const NES::DataType::NULLABLE isNullable)
{
    try
    {
        return NES::DataTypeProvider::provideDataType(fieldNodeType, isNullable);
    }
    catch (std::runtime_error& e)
    {
        throw NES::SLTWrongSchema("Found invalid logical source configuration. {} is not a proper datatype.", fieldNodeType);
    }
}

std::string bindIdentifierName(std::string_view identifier)
{
    auto verifyAllowedCharacters = [](std::string_view potentiallyInvalid)
    {
        if (!std::ranges::all_of(
                potentiallyInvalid, [](char character) { return std::isalnum(character) || character == '_' || character == '$'; }))
        {
            throw NES::InvalidIdentifier("{}", potentiallyInvalid);
        }
    };

    if (identifier.size() > 2 && identifier.starts_with('`') && identifier.ends_with('`'))
    {
        /// remove backticks and keep name as is;
        verifyAllowedCharacters(identifier.substr(1, identifier.size() - 2));
        return std::string(identifier.substr(1, identifier.size() - 2));
    }

    verifyAllowedCharacters(identifier);
    return NES::toUpperCase(identifier);
}
}

namespace NES::CLI
{
/// In CLI SchemaField, Sink, LogicalSource, PhysicalSource and QueryConfig are used as target for the YAML parser.
/// These types should not be used anywhere else in NES; instead we use the bound and validated types, such as LogicalSource and SourceDescriptor.
struct SchemaField
{
    SchemaField(std::string name, const std::string& typeName);
    SchemaField(std::string name, DataType type);
    SchemaField() = default;

    std::string name;
    DataType type;
};

struct Sink
{
    std::string name;
    std::vector<SchemaField> schema;
    std::string type;
    std::string host;
    std::unordered_map<std::string, std::string> config;
    std::unordered_map<std::string, std::string> parserConfig;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct PhysicalSource
{
    std::string logical;
    std::string type;
    std::string host;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

struct WorkerConfig
{
    std::string host;
    std::string dataAddress;
    std::optional<size_t> maxOperators;
    std::vector<std::string> downstream;
    std::unordered_map<std::string, std::string> config; /// Flattened dot-separated config (e.g., "worker.receiver_queue_size" -> "2")
};

struct QueryConfig
{
    std::vector<std::string> query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
    YAML::Node optimizer;
    std::vector<WorkerConfig> workers;
    Priority priority = Priority::HIGH;
};
}

namespace
{
/// Validates that a YAML map node contains only the expected keys. Throws InvalidConfigParameter if an unknown key is found.
void acceptKeys(std::initializer_list<std::string_view> allowed, const YAML::Node& node)
{
    if (!node.IsMap())
    {
        return;
    }
    for (const auto& entry : node)
    {
        const auto key = entry.first.as<std::string>();
        if (std::ranges::find(allowed, key) == allowed.end())
        {
            throw NES::InvalidConfigParameter("Unknown key '{}'. Expected one of: {}", key, fmt::join(allowed, ", "));
        }
    }
}
}

namespace YAML
{
template <>
struct convert<NES::CLI::SchemaField>
{
    static bool decode(const Node& node, NES::CLI::SchemaField& rhs)
    {
        acceptKeys({"name", "type", "nullable"}, node);
        rhs.name = bindIdentifierName(node["name"].as<std::string>());
        const bool nullable = node["nullable"].IsDefined() && node["nullable"].as<bool>();
        const auto isNullable = nullable ? NES::DataType::NULLABLE::IS_NULLABLE : NES::DataType::NULLABLE::NOT_NULLABLE;
        rhs.type = stringToFieldType(node["type"].as<std::string>(), isNullable);
        return true;
    }
};

template <>
struct convert<NES::CLI::Sink>
{
    static bool decode(const Node& node, NES::CLI::Sink& rhs)
    {
        acceptKeys({"name", "type", "schema", "host", "config", "parser_config"}, node);
        rhs.name = bindIdentifierName(node["name"].as<std::string>());
        rhs.type = node["type"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        rhs.host = node["host"].as<std::string>();
        rhs.config = node["config"].as<std::unordered_map<std::string, std::string>>();
        rhs.parserConfig = node["parser_config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::LogicalSource>
{
    static bool decode(const Node& node, NES::CLI::LogicalSource& rhs)
    {
        acceptKeys({"name", "schema"}, node);
        rhs.name = bindIdentifierName(node["name"].as<std::string>());
        rhs.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::PhysicalSource>
{
    static bool decode(const Node& node, NES::CLI::PhysicalSource& rhs)
    {
        acceptKeys({"logical", "type", "host", "parser_config", "source_config"}, node);
        rhs.logical = bindIdentifierName(node["logical"].as<std::string>());
        rhs.type = node["type"].as<std::string>();
        rhs.host = node["host"].as<std::string>();
        rhs.parserConfig = node["parser_config"].as<std::unordered_map<std::string, std::string>>();
        rhs.sourceConfig = node["source_config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::WorkerConfig>
{
    static bool decode(const Node& node, NES::CLI::WorkerConfig& rhs)
    {
        acceptKeys({"host", "data_address", "max_operators", "downstream", "config"}, node);
        if (node["max_operators"].IsDefined())
        {
            rhs.maxOperators = node["max_operators"].as<size_t>();
        }
        if (node["downstream"].IsDefined())
        {
            rhs.downstream = node["downstream"].as<std::vector<std::string>>();
        }
        rhs.host = node["host"].as<std::string>();
        rhs.dataAddress = node["data_address"].IsDefined() ? node["data_address"].as<std::string>() : "";
        return true;
    }
};

template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        acceptKeys({"query", "sinks", "logical", "physical", "optimizer", "workers", "priority"}, node);
        rhs.sinks = node["sinks"].as<std::vector<NES::CLI::Sink>>();
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>();

        if (node["optimizer"].IsDefined())
        {
            rhs.optimizer = node["optimizer"];
        }
        rhs.query = {};
        if (node["query"].IsDefined())
        {
            if (node["query"].IsSequence())
            {
                rhs.query = node["query"].as<std::vector<std::string>>();
            }
            else
            {
                rhs.query.emplace_back(node["query"].as<std::string>());
            }
        }
        rhs.workers = node["workers"].as<std::vector<NES::CLI::WorkerConfig>>();
        if (node["priority"].IsDefined())
        {
            const auto priorityValue = NES::toLowerCase(node["priority"].as<std::string>());
            if (priorityValue == "high")
            {
                rhs.priority = NES::Priority::HIGH;
            }
            else if (priorityValue == "low")
            {
                rhs.priority = NES::Priority::LOW;
            }
            else
            {
                throw NES::InvalidConfigParameter("Unknown query priority '{}'. Expected one of: HIGH, LOW", priorityValue);
            }
        }
        return true;
    }
};
}

namespace
{
NES::CLI::QueryConfig getTopologyPath(const argparse::ArgumentParser& parser)
{
    /// Check -t flag first
    if (parser.is_used("-t"))
    {
        const auto filePath = parser.get<std::string>("-t");

        /// Read topology from stdin
        if (filePath == "-")
        {
            if (isatty(STDIN_FILENO) != 0)
            {
                throw NES::InvalidConfigParameter("Cannot read topology from stdin: stdin is a terminal");
            }
            try
            {
                std::stringstream buffer;
                buffer << std::cin.rdbuf();
                const std::string yamlContent = buffer.str();
                if (yamlContent.empty())
                {
                    throw NES::InvalidConfigParameter("No topology data received from stdin");
                }
                auto validYAML = YAML::Load(yamlContent);
                NES_DEBUG("Using topology from stdin");
                return validYAML.as<NES::CLI::QueryConfig>();
            }
            catch (YAML::Exception& e)
            {
                throw NES::InvalidConfigParameter("stdin is not a valid yaml: {} ({}:{})", e.what(), e.mark.line, e.mark.column);
            }
        }

        if (!std::filesystem::exists(filePath))
        {
            throw NES::InvalidConfigParameter("Topology file specified with -t does not exist: {}", filePath);
        }
        try
        {
            auto validYAML = YAML::LoadFile(filePath);
            NES_DEBUG("Using topology file: {}", filePath);
            return validYAML.as<NES::CLI::QueryConfig>();
        }
        catch (YAML::Exception& e)
        {
            throw NES::InvalidConfigParameter("{} is not a valid yaml file: {} ({}:{})", filePath, e.what(), e.mark.line, e.mark.column);
        }
    }

    std::vector<std::string> options;
    ///NOLINTNEXTLINE(concurrency-mt-unsafe) This is only used at the start of the program on a single thread.
    if (auto* const env = std::getenv("NES_TOPOLOGY_FILE"))
    {
        options.emplace_back(env);
    }
    options.emplace_back("topology.yaml");
    options.emplace_back("topology.yml");

    for (const auto& option : options)
    {
        if (!std::filesystem::exists(option))
        {
            continue;
        }
        try
        {
            /// is valid yaml
            auto validYAML = YAML::LoadFile(option);
            NES_DEBUG("Using topology file: {}", option);
            return validYAML.as<NES::CLI::QueryConfig>();
        }
        catch (YAML::Exception& e)
        {
            /// That wasn't a valid yaml file
            NES_WARNING("{} is not a valid yaml file: {} ({}:{})", option, e.what(), e.mark.line, e.mark.column);
        }
    }
    throw NES::InvalidConfigParameter("Could not find topology file");
}

std::vector<std::string> loadQueries(
    const argparse::ArgumentParser& /*parser*/, const argparse::ArgumentParser& subcommand, const NES::CLI::QueryConfig& topologyConfig)
{
    std::vector<std::string> queries;
    if (subcommand.is_used("queries"))
    {
        for (const auto& query : subcommand.get<std::vector<std::string>>("queries"))
        {
            queries.emplace_back(query);
        }
        NES_DEBUG("loaded {} queries from commandline", queries.size());
    }
    else
    {
        for (const auto& query : topologyConfig.query)
        {
            queries.emplace_back(query);
        }
        NES_DEBUG("loaded {} queries from topology file", queries.size());
    }
    return queries;
}

/// The root worker is the topology DAG sink: the only worker with no `downstream` link of its own.
std::string deriveRootAddress(const std::vector<NES::CLI::WorkerConfig>& workers)
{
    std::vector<std::string> roots;
    for (const auto& worker : workers)
    {
        if (worker.downstream.empty())
        {
            roots.push_back(worker.host);
        }
    }
    if (roots.size() != 1)
    {
        throw NES::InvalidConfigParameter(
            "Could not derive a unique root worker (found {} workers with no downstream); "
            "set 'report_host' in the REQUEST STATISTIC SET clause",
            roots.size());
    }
    return roots.front();
}

/// nes-cli's `query:` field normally carries a plain SELECT query. A REQUEST STATISTIC DATA statement
/// is not a query (the query parser rejects statements), so we detect it, bind it, and turn it into a
/// submittable LogicalPlan via DefaultStatisticQueryGenerator — the same plan the REPL's coordinator
/// builds, but submitted through the ordinary QueryStatement path. A unique `statistic_id` SET option
/// is required (concurrent statistic queries must not collide in the per-worker store); `report_host`
/// overrides the derived root as the coordinator address the terminal sink is placed on.
/// `enableHistogramDeltaCompression` comes from the topology's `optimizer:` block and decides whether an
/// EquiWidthHistogram request is generated as the GEN/RESOLVER split.
NES::LogicalPlan buildSubmittablePlan(
    const std::string& query,
    const std::shared_ptr<NES::SourceCatalog>& sourceCatalog,
    const std::vector<NES::CLI::WorkerConfig>& workers,
    const bool enableHistogramDeltaCompression)
{
    auto trimmed = query;
    trimmed.erase(trimmed.begin(), std::ranges::find_if(trimmed, [](unsigned char c) { return std::isspace(c) == 0; }));
    if (not NES::toUpperCase(trimmed).starts_with("REQUEST"))
    {
        return NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    }

    NES::StatementBinder binder{
        sourceCatalog, [](auto&& ctx) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(ctx)>(ctx)); }};
    auto bound = binder.parseAndBindSingle(query);
    if (not bound)
    {
        throw std::move(bound.error());
    }
    auto* request = std::get_if<NES::RequestStatisticBuildStatement>(&bound.value());
    if (request == nullptr)
    {
        throw NES::InvalidConfigParameter("Query is a statement, but only REQUEST STATISTIC DATA statements are submittable via nes-cli");
    }

    const auto statisticIdIt = request->options.find("statistic_id");
    if (statisticIdIt == request->options.end())
    {
        throw NES::InvalidConfigParameter("REQUEST STATISTIC DATA requires a unique 'statistic_id' in the SET clause");
    }
    const auto statisticId = NES::StatisticTuple::StatisticId{static_cast<uint64_t>(std::stoull(statisticIdIt->second))};

    const auto reportHostIt = request->options.find("report_host");
    const auto rootAddress = reportHostIt != request->options.end() ? reportHostIt->second : deriveRootAddress(workers);

    return NES::DefaultStatisticQueryGenerator{enableHistogramDeltaCompression}.generateQuery(*request, statisticId, rootAddress);
}

std::vector<NES::Statement> loadStatements(const NES::CLI::QueryConfig& topologyConfig)
{
    const auto& [query, sinks, logical, physical, optimizer, workers, priority] = topologyConfig;
    std::vector<NES::Statement> statements;
    statements.reserve(workers.size());
    for (const auto& [host, dataAddress, maxOperators, downstream, config] : workers)
    {
        statements.emplace_back(NES::CreateWorkerStatement{
            .host = host, .dataAddress = dataAddress, .capacity = maxOperators, .downstream = downstream, .config = config});
    }
    for (const auto& [name, schemaFields] : logical)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        statements.emplace_back(NES::CreateLogicalSourceStatement{.name = name, .schema = schema});
    }

    for (const auto& [logical, type, host, parserConfig, sourceConfig] : physical)
    {
        statements.emplace_back(NES::CreatePhysicalSourceStatement{
            .attachedTo = NES::LogicalSourceName(logical),
            .sourceType = type,
            .host = NES::Host(host),
            .sourceConfig = sourceConfig,
            .parserConfig = parserConfig});
    }
    for (const auto& [name, schemaFields, type, host, config, parserConfig] : sinks)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        statements.emplace_back(NES::CreateSinkStatement{
            .name = name, .sinkType = type, .schema = schema, .host = NES::Host(host), .sinkConfig = config, .formatConfig = parserConfig});
    }
    return statements;
}

NES::QueryOptimizerConfiguration loadQueryOptimizerConfiguration(const NES::CLI::QueryConfig& topologyConfig)
{
    NES::QueryOptimizerConfiguration configuration;
    if (topologyConfig.optimizer.IsDefined())
    {
        configuration.overwriteConfigWithYAMLNode(topologyConfig.optimizer);
    }
    return configuration;
}

void doStatus(
    NES::QueryStatementHandler& queryStatementHandler,
    NES::TopologyStatementHandler& topologyStatementHandler,
    const std::vector<NES::DistributedQueryId>& queries)
{
    if (queries.empty())
    {
        auto result = topologyStatementHandler(NES::WorkerStatusStatement{{}});
        if (!result)
        {
            throw std::move(result.error());
        }
        auto jsonResult = nlohmann::json(NES::StatementOutputAssembler<NES::WorkerStatusStatementResult>{}.convert(result.value()));
        std::cout << jsonResult.dump(4) << '\n';
    }
    else
    {
        auto result = nlohmann::json::array();
        for (const auto& query : queries)
        {
            auto statementResult
                = queryStatementHandler(NES::ShowQueriesStatement{.id = query, .format = NES::StatementOutputFormat::JSON});
            if (!statementResult)
            {
                throw std::move(statementResult.error());
            }

            nlohmann::json results(NES::StatementOutputAssembler<NES::ShowQueriesStatementResult>{}.convert(statementResult.value()));
            result.insert(result.end(), results.begin(), results.end());
        }

        std::cout << result.dump(4) << '\n';
    }
}

void doStop(NES::QueryStatementHandler& queryStatementHandler, const std::vector<NES::DistributedQueryId>& queries)
{
    auto result = nlohmann::json::array();
    for (const auto& query : queries)
    {
        auto statementResult = queryStatementHandler(NES::DropQueryStatement{.id = query});
        if (!statementResult)
        {
            throw std::move(statementResult.error());
        }

        nlohmann::json results(NES::StatementOutputAssembler<NES::DropQueryStatementResult>{}.convert(statementResult.value()));
        result.insert(result.end(), results.begin(), results.end());
    }

    std::cout << result.dump(4) << '\n';
}

void doQueryManagement(const argparse::ArgumentParser& program, const argparse::ArgumentParser& subcommand)
{
    const auto topologyConfig = getTopologyPath(program);
    auto queryOptimizationConfiguration = loadQueryOptimizerConfiguration(topologyConfig);
    NES::CLI::QueryStateBackend stateBackend;

    const auto state = subcommand.get<std::vector<std::string>>("queryId")
        | std::views::transform(
                           [&stateBackend](const std::string& queryId) -> std::pair<NES::DistributedQueryId, NES::DistributedQuery>
                           {
                               auto persistedId = NES::CLI::PersistedQueryId::fromString(queryId);
                               auto distributedQuery = stateBackend.load(persistedId);
                               return {persistedId.queryId, distributedQuery};
                           })
        | std::ranges::to<std::unordered_map>();

    const auto queries = state | std::views::keys | std::ranges::to<std::vector>();

    auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    const auto queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend(), NES::QueryManagerState{state});

    NES::TopologyStatementHandler topologyHandler{queryManager, workerCatalog};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    auto queryOptimizer = std::make_shared<NES::QueryOptimizer>(queryOptimizationConfiguration, sourceCatalog, sinkCatalog, workerCatalog);
    NES::QueryStatementHandler queryHandler{queryManager, queryOptimizer};

    handleStatements(loadStatements(topologyConfig), topologyHandler, sourceHandler, sinkHandler);

    if (program.is_subcommand_used("stop"))
    {
        doStop(queryHandler, queries);
    }
    else if (program.is_subcommand_used("status"))
    {
        doStatus(queryHandler, topologyHandler, queries);
    }
    else
    {
        throw NES::InvalidConfigParameter("Invalid query management subcommand");
    }
}

void doQuerySubmission(const argparse::ArgumentParser& program, const argparse::ArgumentParser& subcommand)
{
    auto topologyConfig = getTopologyPath(program);
    auto statements = loadStatements(topologyConfig);
    auto queries = loadQueries(program, subcommand, topologyConfig);
    auto queryOptimizerConfiguration = loadQueryOptimizerConfiguration(topologyConfig);
    if (queries.empty())
    {
        throw NES::InvalidConfigParameter("No queries");
    }

    auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    auto queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend());

    NES::TopologyStatementHandler topologyHandler{queryManager, workerCatalog};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    auto queryOptimizer = std::make_shared<NES::QueryOptimizer>(queryOptimizerConfiguration, sourceCatalog, sinkCatalog, workerCatalog);
    handleStatements(statements, topologyHandler, sourceHandler, sinkHandler);

    if (program.is_subcommand_used("start"))
    {
        NES::CLI::QueryStateBackend stateBackend;
        NES::QueryStatementHandler queryStatementHandler{queryManager, queryOptimizer};
        for (const auto& query : queries)
        {
            auto plan = buildSubmittablePlan(
                query, sourceCatalog, topologyConfig.workers, queryOptimizerConfiguration.enableHistogramDeltaCompression.getValue());
            plan.setPriority(topologyConfig.priority);
            if (auto result = queryStatementHandler(NES::QueryStatement{std::move(plan), {}}))
            {
                auto queryDescriptor = queryManager->getQuery(result->id);
                INVARIANT(queryDescriptor.has_value(), "Query should exist in the query manager if statement handler succeed");
                auto persistedId = stateBackend.store(result->id, *queryDescriptor);
                std::cout << persistedId.toString() << '\n';
            }
            else
            {
                throw std::move(result.error());
            }
        }
    }
    else
    {
        NES::QueryStatementHandler queryStatementHandler{queryManager, queryOptimizer};
        for (const auto& query : queries)
        {
            auto result = queryStatementHandler(NES::ExplainQueryStatement(buildSubmittablePlan(
                query, sourceCatalog, topologyConfig.workers, queryOptimizerConfiguration.enableHistogramDeltaCompression.getValue())));
            if (result)
            {
                std::cout << result->explainString << "\n";
            }
            else
            {
                throw std::move(result.error());
            }
        }
    }
}
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        using argparse::ArgumentParser;
        ArgumentParser program("nebucli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-t").help(
            "Path to the topology file, or '-' to read from stdin. "
            "Resolution order: 1) -t flag, 2) NES_TOPOLOGY_FILE env, 3) topology.yaml/topology.yml in working directory");

        ArgumentParser startQuery("start");
        startQuery.add_argument("queries").nargs(argparse::nargs_pattern::any);

        ArgumentParser stopQuery("stop");
        stopQuery.add_argument("queryId").nargs(argparse::nargs_pattern::at_least_one);

        ArgumentParser statusQuery("status");
        statusQuery.add_argument("queryId").nargs(argparse::nargs_pattern::any);

        ArgumentParser dump("dump");
        dump.add_argument("queries").nargs(argparse::nargs_pattern::any);

        program.add_subparser(startQuery);
        program.add_subparser(stopQuery);
        program.add_subparser(statusQuery);
        program.add_subparser(dump);

        std::vector<std::reference_wrapper<ArgumentParser>> queryManagementSubcommands{stopQuery, statusQuery};
        std::vector<std::reference_wrapper<ArgumentParser>> querySubmissionCommands{startQuery, dump};

        try
        {
            program.parse_args(argc, argv);
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << "\n";
            std::cerr << program;
            return 1;
        }

        NES::Logger::setupLogging("nes-cli.log", NES::LogLevel::LOG_WARNING, program.is_used("-d"));
        if (program.get<bool>("-d"))
        {
            NES::Logger::getInstance()->changeLogLevel(NES::LogLevel::LOG_DEBUG);
        }

        if (auto subcommand = std::ranges::find_if(
                queryManagementSubcommands, [&](auto& subparser) { return program.is_subcommand_used(subparser.get()); });
            subcommand != queryManagementSubcommands.end())
        {
            doQueryManagement(program, *subcommand);
            return 0;
        }

        if (auto subcommand
            = std::ranges::find_if(querySubmissionCommands, [&](auto& subparser) { return program.is_subcommand_used(subparser.get()); });
            subcommand != querySubmissionCommands.end())
        {
            doQuerySubmission(program, *subcommand);
            return 0;
        }

        std::cerr << "No subcommand used.\n";
        std::cerr << program;
        return 1;
    }

    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return 1;
    }
}
