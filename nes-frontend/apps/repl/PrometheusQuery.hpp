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

/// Minimal, self-contained PromQL instant-query client for the Prometheus-baseline poll loop.
/// The frontend only links gRPC, not an HTTP client, and the only vendored HTTP client (the cpr
/// vcpkg port) isn't built. Since the poll loop issues a single localhost, read-only GET roughly
/// once per second behind the --baseline-prometheus flag, a tiny raw-socket HTTP/1.1 GET avoids
/// pulling in a new build dependency. Header-only; included once by ReplStarter.

#include <array>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <optional>
#include <string>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace NES::repl_baseline
{

/// Percent-encode a string for use in a URL query component (RFC 3986 unreserved set kept as-is).
inline std::string urlEncode(const std::string& s)
{
    std::string out;
    out.reserve(s.size() * 3);
    for (const unsigned char c : s)
    {
        if (std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.' || c == '~')
        {
            out += static_cast<char>(c);
        }
        else
        {
            std::array<char, 4> buf{};
            std::snprintf(buf.data(), buf.size(), "%%%02X", c);
            out += buf.data();
        }
    }
    return out;
}

/// Blocking HTTP/1.1 GET over a TCP socket. Intended for localhost + small responses (Prometheus
/// /api/v1/query). Returns the full raw response (headers + body), or nullopt on any failure.
/// `Connection: close` lets us read until EOF; 2s socket timeouts bound a stuck server.
inline std::optional<std::string> httpGet(const std::string& host, const std::string& port, const std::string& path)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0 || res == nullptr)
    {
        return std::nullopt;
    }

    int fd = -1;
    for (addrinfo* p = res; p != nullptr; p = p->ai_next)
    {
        fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0)
        {
            continue;
        }
        timeval tv{};
        tv.tv_sec = 2;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        if (connect(fd, p->ai_addr, p->ai_addrlen) == 0)
        {
            break;
        }
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0)
    {
        return std::nullopt;
    }

    const std::string request = "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";
    if (send(fd, request.data(), request.size(), 0) < 0)
    {
        close(fd);
        return std::nullopt;
    }

    std::string response;
    std::array<char, 4096> buf{};
    ssize_t n = 0;
    while ((n = recv(fd, buf.data(), buf.size(), 0)) > 0)
    {
        response.append(buf.data(), static_cast<size_t>(n));
    }
    close(fd);
    return response;
}

/// Extract the scalar value from a Prometheus instant-query JSON response. The vector result form
/// is {"data":{"result":[{"metric":{...},"value":[<ts>,"<number>"]}]}}; we pull the quoted number
/// after the first "value":[. Returns nullopt for an empty result or an unparseable value (e.g. NaN).
inline std::optional<double> extractPrometheusScalar(const std::string& body)
{
    const auto vpos = body.find("\"value\":[");
    if (vpos == std::string::npos)
    {
        return std::nullopt;
    }
    const auto comma = body.find(',', vpos);
    if (comma == std::string::npos)
    {
        return std::nullopt;
    }
    const auto q1 = body.find('"', comma);
    if (q1 == std::string::npos)
    {
        return std::nullopt;
    }
    const auto q2 = body.find('"', q1 + 1);
    if (q2 == std::string::npos)
    {
        return std::nullopt;
    }
    try
    {
        return std::stod(body.substr(q1 + 1, q2 - q1 - 1));
    }
    catch (...)
    {
        return std::nullopt;
    }
}

/// Run a PromQL instant query against `hostPort` (e.g. "localhost:9595") and return the scalar
/// result, or nullopt if Prometheus is unreachable / returned no data yet.
inline std::optional<double> queryPrometheusScalar(const std::string& hostPort, const std::string& promql)
{
    const auto colon = hostPort.find(':');
    const std::string host = colon == std::string::npos ? hostPort : hostPort.substr(0, colon);
    const std::string port = colon == std::string::npos ? std::string{"9595"} : hostPort.substr(colon + 1);
    const std::string path = "/api/v1/query?query=" + urlEncode(promql);
    const auto resp = httpGet(host, port, path);
    if (not resp.has_value())
    {
        return std::nullopt;
    }
    return extractPrometheusScalar(*resp);
}

}
