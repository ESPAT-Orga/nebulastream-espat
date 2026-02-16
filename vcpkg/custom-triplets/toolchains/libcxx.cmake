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
# Native Linux (no cross compiling)
# clang + libc++ + pthread + LLD/mold support

if (NOT _NES_TOOLCHAIN_FILE)
    set(_NES_TOOLCHAIN_FILE 1)

    # ---------------------------------------------------
    # System setup (native build)
    # ---------------------------------------------------

    set(CMAKE_SYSTEM_NAME Linux CACHE STRING "")
    set(CMAKE_CROSSCOMPILING OFF CACHE BOOL "")

    # Ensure correct processor naming
    if (VCPKG_TARGET_ARCHITECTURE STREQUAL "arm64")
        set(CMAKE_SYSTEM_PROCESSOR aarch64 CACHE STRING "")
    elseif (VCPKG_TARGET_ARCHITECTURE STREQUAL "x64")
        set(CMAKE_SYSTEM_PROCESSOR x86_64 CACHE STRING "")
    endif()

    # ---------------------------------------------------
    # Compiler selection
    # ---------------------------------------------------

    find_program(CLANGXX_EXECUTABLE NAMES clang++-19 REQUIRED)
    find_program(CLANG_EXECUTABLE  NAMES clang-19   REQUIRED)

    set(CMAKE_C_COMPILER   ${CLANG_EXECUTABLE}  CACHE STRING "")
    set(CMAKE_CXX_COMPILER ${CLANGXX_EXECUTABLE} CACHE STRING "")

    set(CMAKE_CXX_STANDARD 23)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    # ---------------------------------------------------
    # libc++ setup
    # ---------------------------------------------------

    set(LIBCXX_COMPILER_FLAG "-stdlib=libc++")
    set(LIBCXX_LINKER_FLAG   "-stdlib=libc++")

    # ---------------------------------------------------
    # Prefer LLD, fallback to mold
    # ---------------------------------------------------

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

    # ---------------------------------------------------
    # ccache (optional)
    # ---------------------------------------------------

    find_program(CCACHE_EXECUTABLE NAMES ccache)
    if (CCACHE_EXECUTABLE)
        set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
    endif()

    # ---------------------------------------------------
    # Thread support (important!)
    # ---------------------------------------------------

    set(THREADS_PREFER_PTHREAD_FLAG ON)

    # pthread must be in compile AND link flags
    string(APPEND CMAKE_C_FLAGS_INIT               " -pthread ")
    string(APPEND CMAKE_CXX_FLAGS_INIT             " -pthread ")
    string(APPEND CMAKE_EXE_LINKER_FLAGS_INIT      " -pthread ")
    string(APPEND CMAKE_SHARED_LINKER_FLAGS_INIT   " -pthread ")

    # ---------------------------------------------------
    # Flags (not applied in try_compile)
    # ---------------------------------------------------

    get_property(_CMAKE_IN_TRY_COMPILE GLOBAL PROPERTY IN_TRY_COMPILE)

    if (NOT _CMAKE_IN_TRY_COMPILE)

        string(APPEND CMAKE_C_FLAGS_INIT
                " -fPIC ${VCPKG_C_FLAGS} "
        )

        string(APPEND CMAKE_CXX_FLAGS_INIT
                " -std=c++23 ${LIBCXX_COMPILER_FLAG} -fPIC ${VCPKG_CXX_FLAGS} "
        )

        string(APPEND CMAKE_C_FLAGS_DEBUG_INIT   "${VCPKG_C_FLAGS_DEBUG} ")
        string(APPEND CMAKE_CXX_FLAGS_DEBUG_INIT "${VCPKG_CXX_FLAGS_DEBUG} ")

        string(APPEND CMAKE_C_FLAGS_RELEASE_INIT   "${VCPKG_C_FLAGS_RELEASE} ")
        string(APPEND CMAKE_CXX_FLAGS_RELEASE_INIT "${VCPKG_CXX_FLAGS_RELEASE} ")

        string(APPEND CMAKE_EXE_LINKER_FLAGS_INIT
                " ${LIBCXX_LINKER_FLAG} ${LINKER_FLAG} ${VCPKG_LINKER_FLAGS} "
        )

        string(APPEND CMAKE_SHARED_LINKER_FLAGS_INIT
                " ${LIBCXX_LINKER_FLAG} ${LINKER_FLAG} ${VCPKG_LINKER_FLAGS} "
        )

        string(APPEND CMAKE_MODULE_LINKER_FLAGS_INIT
                " ${LIBCXX_LINKER_FLAG} ${LINKER_FLAG} ${VCPKG_LINKER_FLAGS} "
        )

    endif()

endif()
