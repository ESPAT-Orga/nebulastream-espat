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

### Execute Benchmarks

You can find the benchmarks in the [Benchmark Folder](scripts/benchmarking)