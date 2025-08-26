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

#include <DistributedQueryId.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h>

#include <Identifiers/Identifiers.hpp>

namespace YAML
{
template <>
struct convert<NES::LocalQuery>
{
    static bool decode(const YAML::Node& node, NES::LocalQuery& rhs)
    {
        rhs.id = NES::LocalQueryId{node["id"].as<size_t>()};
        rhs.grpcAddr = node["grpc"].as<std::string>();
        return true;
    }
    static Node encode(const NES::LocalQuery& rhs)
    {
        YAML::Node node;
        node["id"] = rhs.id.getRawValue();
        node["grpc"] = rhs.grpcAddr;
        return node;
    }
};

template <>
struct convert<NES::Query>
{
    static bool decode(const YAML::Node& node, NES::Query& rhs)
    {
        if (node["distributedQueryId"])
        {
            rhs.id = NES::DistributedQueryId{node["distributedQueryId"].as<size_t>()};
        }
        else
        {
            rhs.id = NES::getNextDistributedQueryId();
        }
        rhs.backend.embeddedOrCluster = node["localQueries"].as<std::vector<NES::LocalQuery>>();
        return true;
    }
    static Node encode(const NES::Query& rhs)
    {
        YAML::Node node;
        node["distributedQueryId"] = rhs.id.getRawValue();
        node["localQueries"] = std::get<std::vector<NES::LocalQuery>>(rhs.backend.embeddedOrCluster);
        return node;
    }
};
}

namespace NES
{
std::string Query::save(const Query& queryId)
{
    const std::string name = std::tmpnam(nullptr);
    if (std::ofstream out(name); out.is_open())
    {
        const YAML::Node node = YAML::convert<Query>::encode(queryId);
        out << node;
    }

    auto withoutPrefix = std::string_view(name);
    withoutPrefix.remove_prefix(5);
    return std::string{withoutPrefix};
}

Query Query::load(const std::string& identifier)
{
    return YAML::LoadFile(std::filesystem::path("/tmp") / identifier).as<Query>();
}

}
