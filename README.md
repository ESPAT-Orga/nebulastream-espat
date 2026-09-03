## Quick Start

### Build & Tooling

#### Use the Nix flake
Install [Nix with flakes enabled](https://nixos.org/download/) and generate build links:

```shell
nix run .#clion-setup
```

Configure, build, and test through the Nix-wrapped helpers:

```shell
./.nix/nix-cmake.sh \
  -DCMAKE_BUILD_TYPE=Debug \
  -G Ninja \
  -S . -B cmake-build-debug

./.nix/nix-cmake.sh --build cmake-build-debug
./.nix/ctest --test-dir cmake-build-debug -j
```

For formatting and clang-tidy fixes, run `nix run .#format` and `nix run .#clang-tidy`.

#### Use the development container
Build or reuse the local development image (installs the current user inside the container to avoid permission issues):

```shell
./scripts/install-local-docker-environment.sh
```

Configure, build, and test by mounting the repository into the container:

```shell
docker run \
  --workdir $(pwd) \
  -v $(pwd):$(pwd) \
  nebulastream/nes-development:local \
  cmake -B cmake-build-debug

docker run \
  --workdir $(pwd) \
  -v $(pwd):$(pwd) \
  nebulastream/nes-development:local \
  cmake --build cmake-build-debug -j

docker run \
  --workdir $(pwd) \
  -v $(pwd):$(pwd) \
  nebulastream/nes-development:local \
  ctest --test-dir cmake-build-debug -j
```

### Execute Benchmarks

You can find the benchmarks in the [Benchmark Folder](scripts/benchmarking)