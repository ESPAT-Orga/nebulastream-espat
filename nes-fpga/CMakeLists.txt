# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Add Source Code
#add_subdirectory(src)

# Add nes-fpga library
#get_source(nes-fpga NES_FPGA_SOURCE_FILES)
#add_library(nes-fpga ${NES_FPGA_SOURCE_FILES})
# target_link_libraries(nes-fpga PUBLIC nes-common nes-runtime)
#target_include_directories(nes-fpga PUBLIC
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
#        $<INSTALL_INTERFACE:include/nebulastream/>)

#add_executable(nes-fpga-main src/main.cpp)
#target_link_libraries(nes-fpga-main PRIVATE nes-fpga)
#target_include_directories(nes-fpga-main PUBLIC
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
#        $<INSTALL_INTERFACE:include/nebulastream/>)



# Where the platform sources were copied
set(PLATFORM_DIR ${CMAKE_SOURCE_DIR}/src/platform)
set(USER_INCLUDE_DIRECTORIES
)
set(USER_COMPILE_SOURCES
)
#include(${PLATFORM_DIR}/UserConfig.cmake) # imports USER_COMPILE_SOURCES, USER_INCLUDE_DIRECTORIES, etc.

# Build an object/static library from platform sources
# Convert USER_COMPILE_SOURCES (relative to PLATFORM_DIR) to absolute paths
set(_platform_srcs "")
foreach(_s IN LISTS USER_COMPILE_SOURCES)
    list(APPEND _platform_srcs "${PLATFORM_DIR}/${_s}")
endforeach()

# Add Xilinx driver source files if you copied them separately
# list(APPEND _platform_srcs ${CMAKE_SOURCE_DIR}/third_party/vitis_axidma/src/xaxidma.c)

# Create library target
add_library(platform_app STATIC ${_platform_srcs})

# Include directories (USER_INCLUDE_DIRECTORIES may be relative to PLATFORM_DIR)
set(_platform_includes "")
foreach(_inc IN LISTS USER_INCLUDE_DIRECTORIES)
    # If path is absolute keep it, else prefix with PLATFORM_DIR
    if(IS_ABSOLUTE "${_inc}")
        list(APPEND _platform_includes "${_inc}")
    else()
        list(APPEND _platform_includes "${PLATFORM_DIR}/${_inc}")
    endif()
endforeach()
# Also add your platform headers (xparameters.h etc.)
list(APPEND _platform_includes ${CMAKE_SOURCE_DIR}/include/platform)
target_include_directories(platform_app PUBLIC ${_platform_includes})

# IMPORTANT: remove any bare-metal link libraries from USER_LINK_LIBRARIES
# USER_LINK_LIBRARIES is likely to include "xilstandalone;xiltimer;xil;gcc;c"
# We do not want to link with the standalone BSP libs for linux native build.
# If you need some of the .c sources from BSP (e.g. xaxidma.c), copy them into the third_party/platform_app folder
# and add them to USER_COMPILE_SOURCES or manually to _platform_srcs above.

# Add compat layer files (you must create these)
#target_sources(platform_app PRIVATE
#        ${CMAKE_SOURCE_DIR}/src/xil_compat.c
#        ${CMAKE_SOURCE_DIR}/src/uio_map.c
#)

target_include_directories(platform_app PRIVATE ${CMAKE_SOURCE_DIR}/src)

# Build your main application and link to the platform_app library:
add_executable(nes-fpga-main src/main.cpp)
target_link_libraries(nes-fpga-main PRIVATE platform_app)
#add_executable(host_app src/main.cpp)
#target_link_libraries(host_app PRIVATE platform_app pthread rt)

# If you need defines from USER_COMPILE_DEFINITIONS
if(DEFINED USER_COMPILE_DEFINITIONS)
    target_compile_definitions(platform_app PUBLIC ${USER_COMPILE_DEFINITIONS})
endif()

# Add any extra compile flags from USER_COMPILE_OPTIONS (careful: some flags are toolchain-specific)
if(DEFINED USER_COMPILE_OPTIONS)
    foreach(opt IN LISTS USER_COMPILE_OPTIONS)
        string(APPEND CMAKE_C_FLAGS " ${opt}")
        string(APPEND CMAKE_CXX_FLAGS " ${opt}")
    endforeach()
endif()
