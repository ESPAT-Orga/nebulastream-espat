# Build the producer in a Rust image, then ship it on a tiny distroless runtime.
# Image is referenced as `nes-bench-gen:local` by the benchmark runner.

FROM rust:1-bookworm AS build
WORKDIR /work
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release && \
    cp target/release/nes-bench-gen /usr/bin/nes-bench-gen

FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /usr/bin/nes-bench-gen /usr/bin/nes-bench-gen
ENTRYPOINT ["/usr/bin/nes-bench-gen"]
