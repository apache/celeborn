# C ABI shim that wraps celeborn::client::ShuffleClient* in pure-C entry
# points. Built as a STATIC archive that is whole-archived into the
# aggregated libceleborn_client.{so,dylib} (see top-level CMakeLists.txt).
add_library(
        celeborn_ffi
        STATIC
        CelebornFfi.cc)

target_include_directories(celeborn_ffi PUBLIC
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_BINARY_DIR})

target_link_libraries(
        celeborn_ffi
        client
        conf
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES})
