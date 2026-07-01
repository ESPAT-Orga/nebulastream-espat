# syntax=docker/dockerfile:1
#
# Combined-runtime image for the over-time benchmark in
# scripts/benchmarking/multiple-statistic-queries-over-time-prometheus/.
#
# Holds the Prometheus binary alongside the nes-runtime-base libraries so that
# both nes-single-node-worker and prometheus can run inside ONE container with
# a single shared cgroup (--cpus / --memory). The benchmark runner bind-mounts
# the host-built nes-single-node-worker binary into /usr/bin/ at run time, so
# iterating on engine code does not require rebuilding this image.
#
# Build:
#   docker build -t nes-bench-prom-combined:local \
#       -f docker/single-node-worker/SingleNodeWorkerWithPrometheus.dockerfile \
#       docker/single-node-worker
#
# Run (typical, from the benchmark runner):
#   docker run -d --rm --name nes-bench-<name> \
#       --cpus=2.5 --memory=2.5g \
#       -e NES_RUN_PROMETHEUS=1 \
#       -e NES_PROM_TARGETS="127.0.0.1:8800,127.0.0.1:8801,..." \
#       -p 8080:8080 -p 9090:9090 -p 9091:9091 -p 8800-8899:8800-8899 \
#       -v <host worker binary path>:/usr/bin/nes-single-node-worker:ro \
#       nes-bench-prom-combined:local --grpc=0.0.0.0:8080 ...

### The runtime base ships as a local image (`nes-runtime-base:test`) on the benchmark host
### per docker/runtime/RuntimeBase.dockerfile; it is NOT pushed to Docker Hub under that path.
### Override via --build-arg RUNTIME_BASE=... when running outside the benchmark host.
ARG RUNTIME_BASE=nes-runtime-base:test
ARG PROMETHEUS_VERSION=2.53.5

# --- Stage 1: fetch Prometheus tarball in a tiny stage so we don't need curl/tar in the runtime image.
FROM curlimages/curl:8.10.1 AS prom-fetch
ARG PROMETHEUS_VERSION
ARG TARGETARCH=amd64
WORKDIR /tmp
RUN curl -fsSL "https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/prometheus-${PROMETHEUS_VERSION}.linux-${TARGETARCH}.tar.gz" \
        -o prometheus.tar.gz \
 && mkdir prometheus \
 && tar -xzf prometheus.tar.gz -C prometheus --strip-components=1 \
 && rm prometheus.tar.gz

# --- Stage 2: runtime layer = nes-runtime-base + prometheus + entrypoint.
FROM ${RUNTIME_BASE} AS app

COPY --from=prom-fetch /tmp/prometheus/prometheus /usr/bin/prometheus
COPY --from=prom-fetch /tmp/prometheus/promtool   /usr/bin/promtool

# iproute2 provides `tc`, used by the distributed contention benchmark to cap the root container's
# ingress bandwidth (tc ingress + police). Harmless for the other experiments that use this image.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 \
 && rm -rf /var/lib/apt/lists/*

COPY bench-entrypoint.sh /usr/local/bin/bench-entrypoint.sh
RUN chmod +x /usr/local/bin/bench-entrypoint.sh

# Prometheus data lives under /tmp so it's automatically discarded with the container.
ENV PROM_DATA_DIR=/tmp/prom-data

ENTRYPOINT ["/usr/local/bin/bench-entrypoint.sh"]
