# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a toolchain file for building dependencies and compiling NebulaStream
# 1. Enable the mold linker if it is available on the the system
# 2. Enable CCache if it is available on the system
# 3. Use Libc++ if available

# NebulaStream libc++ toolchain for vcpkg
# Native Linux + clang + libc++

if (NOT _NES_TOOLCHAIN_FILE)
    set(_NES_TOOLCHAIN_FILE 1)

    # We are doing native builds
    set(CMAKE_SYSTEM_NAME Linux CACHE STRING "")
    set(CMAKE_CROSSCOMPILING OFF CACHE BOOL "")

    # Prefer LLD
    find_program(LLD_EXECUTABLE NAMES ld.lld-19 ld.lld)

    if (LLD_EXECUTABLE)
        set(LINKER_FLAG "-fuse-ld=lld")
    else()
        find_program(MOLD_EXECUTABLE NAMES mold)
        if (MOLD_EXECUTABLE)
            set(LINKER_FLAG "-fuse-ld=mold")
        else()
            set(LINKER_FLAG "")
        endif()
    endif()

    # Optional: ccache
    find_program(CCACHE_EXECUTABLE NAMES ccache)
    if (CCACHE_EXECUTABLE)
        set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
    endif()

    # Clang selection
    find_program(CLANGXX_EXECUTABLE NAMES clang++-$ENV{LLVM_TOOLCHAIN_VERSION} REQUIRED)
    find_program(CLANG_EXECUTABLE NAMES clang-$ENV{LLVM_TOOLCHAIN_VERSION} REQUIRED)

    set(CMAKE_CXX_COMPILER ${CLANGXX_EXECUTABLE})
    set(CMAKE_C_COMPILER ${CLANG_EXECUTABLE})

    # libc++
    set(LIBCXX_FLAG "-stdlib=libc++")

    # Ensure pthread is always present
    set(PTHREAD_FLAG "-pthread")

    # Compiler flags
    string(APPEND CMAKE_C_FLAGS_INIT " -fPIC ${PTHREAD_FLAG} ${VCPKG_C_FLAGS}")
    string(APPEND CMAKE_CXX_FLAGS_INIT " ${LIBCXX_FLAG} -std=c++23 -fPIC ${PTHREAD_FLAG} ${VCPKG_CXX_FLAGS}")

    # Linker flags
    string(APPEND CMAKE_EXE_LINKER_FLAGS_INIT " ${PTHREAD_FLAG} ${LINKER_FLAG} ${VCPKG_LINKER_FLAGS}")
    string(APPEND CMAKE_SHARED_LINKER_FLAGS_INIT " ${PTHREAD_FLAG} ${LINKER_FLAG} ${VCPKG_LINKER_FLAGS}")

    # Debug/Release passthrough
    string(APPEND CMAKE_C_FLAGS_DEBUG_INIT " ${VCPKG_C_FLAGS_DEBUG}")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG_INIT " ${VCPKG_CXX_FLAGS_DEBUG}")
    string(APPEND CMAKE_C_FLAGS_RELEASE_INIT " ${VCPKG_C_FLAGS_RELEASE}")
    string(APPEND CMAKE_CXX_FLAGS_RELEASE_INIT " ${VCPKG_CXX_FLAGS_RELEASE}")

    string(APPEND CMAKE_EXE_LINKER_FLAGS_DEBUG_INIT " ${VCPKG_LINKER_FLAGS_DEBUG}")
    string(APPEND CMAKE_EXE_LINKER_FLAGS_RELEASE_INIT " ${VCPKG_LINKER_FLAGS_RELEASE}")
    string(APPEND CMAKE_SHARED_LINKER_FLAGS_DEBUG_INIT " ${VCPKG_LINKER_FLAGS_DEBUG}")
    string(APPEND CMAKE_SHARED_LINKER_FLAGS_RELEASE_INIT " ${VCPKG_LINKER_FLAGS_RELEASE}")

endif()
