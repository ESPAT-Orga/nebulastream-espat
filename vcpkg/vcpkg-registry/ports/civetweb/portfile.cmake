vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO civetweb/civetweb
    REF d7ba35bbb649209c66e582d5a0244ba988a15159
    SHA512 2890205b32935226e905f38edcd7a6d8fd12d63ef811e8a7c9fc3a2b03aef908551e9b0ee8724396a3aa8bb633907b6bfeb78f918437773eb50a4e77db96980a
    HEAD_REF master
    PATCHES
        disable_warnings.patch # cl will simply ignore the other invalid options. 
        fix-fseeko.patch
        pkgconfig.patch
)
file(REMOVE_RECURSE "${SOURCE_PATH}/src/third_party")

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        ssl CIVETWEB_ENABLE_SSL
)

# Fixes arm64-windows build. CIVETWEB_ARCHITECTURE is used only for CPack, which is not used by vcpkg
vcpkg_replace_string("${SOURCE_PATH}/CMakeLists.txt" "determine_target_architecture(CIVETWEB_ARCHITECTURE)" "")

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DCIVETWEB_BUILD_TESTING=OFF
        -DCIVETWEB_ENABLE_DEBUG_TOOLS=OFF
        -DCIVETWEB_ENABLE_ASAN=OFF
        -DCIVETWEB_ENABLE_CXX=ON
        -DCIVETWEB_ENABLE_IPV6=OFF
        -DCIVETWEB_ENABLE_SERVER_EXECUTABLE=OFF
        -DCIVETWEB_ENABLE_SSL_DYNAMIC_LOADING=OFF
        -DCIVETWEB_ENABLE_WEBSOCKETS=OFF
        -DCIVETWEB_ALLOW_WARNINGS=ON
        -DCIVETWEB_ENABLE_ZLIB=OFF
        "-DVERSION=${VERSION}"
        ${FEATURE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/civetweb)
vcpkg_fixup_pkgconfig()

if(VCPKG_LIBRARY_LINKAGE STREQUAL "dynamic")
    vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/include/civetweb.h" "defined(CIVETWEB_DLL_IMPORTS)" 1)
    vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/include/CivetServer.h" "defined(CIVETWEB_CXX_DLL_IMPORTS)" 1)
endif()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share/pkgconfig")

file(COPY "${CURRENT_PORT_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE.md")
