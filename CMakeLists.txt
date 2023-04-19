cmake_minimum_required(VERSION 3.10)

project(yedis)

set(CMAKE_CXX_STANDARD 20)

add_compile_definitions(DEBUG YEDIS_DEBUG_ALLOCATION)

IF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    include_directories(./include deps/spdlog/include deps/gtest/googletest/include deps/abseil-cpp)
ELSE()
    include_directories(./include deps/spdlog/include deps/gtest/googletest/include deps/abseil-cpp/)
ENDIF()


option(BUILD_WITH_TESTS "Build Yedis's unit tests" ON)

include_directories(include)

aux_source_directory(src SOURCE_FILES)
aux_source_directory(src/ydb SOURCE_FILES)

message(STATUS "${SOURCE_FILES}"})
#add_executable(yedis ${SOURCE_FILES})
#target_link_libraries(yedis rocksdb spdlog::spdlog gtest)

add_library(yedis "")

target_sources(yedis
    PRIVATE
        ${SOURCE_FILES}
)

target_compile_options(yedis PRIVATE -Wthread-safety)


add_subdirectory(deps/spdlog)
add_subdirectory(deps/gtest)
add_subdirectory(deps/abseil-cpp)
add_subdirectory(tests)