cmake_minimum_required(VERSION 3.10)

project(yedis)

set(CMAKE_CXX_STANDARD 20)

IF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    include_directories(./include ~/lab/rocksdb/include deps/spdlog/include deps/gtest/googletest/include)
    link_directories(~/lab/rocksdb/cmake-build-Release)
ELSE()
    include_directories(./include /home/skyitachi/lab/rocksdb/include deps/spdlog/include deps/gtest/googletest/include)
    link_directories(/home/skyitachi/lab/rocksdb/cmake-build-release)
ENDIF()


option(BUILD_WITH_TESTS "Build Yedis's unit tests" ON)

aux_source_directory(src SOURCE_FILES)
#add_executable(yedis ${SOURCE_FILES})
#target_link_libraries(yedis rocksdb spdlog::spdlog gtest)

add_library(yedis "")

target_sources(yedis
    PRIVATE
        ${SOURCE_FILES}
)


add_subdirectory(deps/spdlog)
add_subdirectory(deps/gtest)
add_subdirectory(tests)
