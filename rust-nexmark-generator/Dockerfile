# Build the producer in a Rust image, then ship it on a tiny distroless runtime.
# Image is referenced as `nes-bench-nexmark-gen:local` by the benchmark runner.

FROM rust:1-bookworm AS build
WORKDIR /work
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release && \
    cp target/release/nes-nexmark-gen /usr/bin/nes-nexmark-gen

FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /usr/bin/nes-nexmark-gen /usr/bin/nes-nexmark-gen
ENTRYPOINT ["/usr/bin/nes-nexmark-gen"]
