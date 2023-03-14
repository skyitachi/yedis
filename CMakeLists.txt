cmake_minimum_required(VERSION 3.10)

project(yedis)

set(CMAKE_CXX_STANDARD 20)

IF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    include_directories(./include deps/spdlog/include deps/gtest/googletest/include)
ELSE()
    include_directories(./include deps/spdlog/include deps/gtest/googletest/include)
ENDIF()


option(BUILD_WITH_TESTS "Build Yedis's unit tests" ON)

include_directories(include)

aux_source_directory(src SOURCE_FILES)
#add_executable(yedis ${SOURCE_FILES})
#target_link_libraries(yedis rocksdb spdlog::spdlog gtest)

add_library(yedis "" src/ydb/db_impl.cpp src/ydb/fs.hpp src/ydb/fs.cpp src/exception.cpp src/ydb/wal.cpp)

target_sources(yedis
    PRIVATE
        ${SOURCE_FILES}
)


add_subdirectory(deps/spdlog)
add_subdirectory(deps/gtest)
add_subdirectory(tests)