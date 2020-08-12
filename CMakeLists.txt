cmake_minimum_required(VERSION 3.10)

project(yedis)

include_directories(./include /home/skyitachi/lab/rocksdb/include deps/spdlog/include)
link_directories(/home/skyitachi/lab/rocksdb/cmake-build-release)

add_subdirectory(deps/spdlog)

aux_source_directory(src SOURCE_FILES)
add_executable(yedis ${SOURCE_FILES})
target_link_libraries(yedis rocksdb spdlog::spdlog)