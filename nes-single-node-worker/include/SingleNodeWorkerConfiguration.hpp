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
#include <Configuration/WorkerConfiguration.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ScalarOption.hpp>

namespace NES
{

class SingleNodeWorkerConfiguration final : public BaseConfiguration
{
public:
    ScalarOption<NES::URI> connection = {"connection", "Connection name. This is the {Hostname}:{PORT}"};

    /// GRPC Server Address URI. By default, it binds to any address and listens on port 8080
    ScalarOption<NES::URI> grpcAddressUri
        = {"grpc",
           "localhost:8080",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"};

    /// Enable Google Event Trace logging (Chrome tracing format)
    BoolOption enableGoogleEventTrace
        = {"enable_event_trace",
           "false",
           "Enable Google Event Trace logging that generates Chrome tracing compatible JSON files for performance analysis."};

    /// Enable backpressure event logging and emission via tcp
    BoolOption enableBackpressureStatisticsTCPEmission
        = {"enable_backpressure_statistics_tcp_emission",
           "false",
           "Enable the collection of statistics about backpressure encountered at network sinks and expose this data via tcp"};
    StringOption backpressureStatisticsTCPEmissionHost
        = {"backpressure_statistics_tcp_emission_host", "127.0.0.1", "The host for exposing backpressure statistics via tcp"};
    UIntOption backpressureStatisticsTCPEmissionPort
        = {"backpressure_statistics_tcp_emission_port", "9000", "The port for exposing backpressure statistics via tcp"};


    BoolOption enableAdaptiveNetworkSending
        = {"enable_adaptive_network_sending",
       "false",
       "Adaptively throttle lower priority queries based on backpressure"};

protected:
    std::vector<BaseOption*> getOptions() override;

    template <typename T>
    friend void generateHelp(std::ostream& ostream);

public:
    SingleNodeWorkerConfiguration() = default;
    WorkerConfiguration workerConfiguration = {"worker", "NodeEngine Configuration"};
};
}
