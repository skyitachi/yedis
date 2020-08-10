cmake_minimum_required(VERSION 3.10)

project(yedis)

include_directories(/home/skyitachi/lab/rocksdb/include)
link_directories(/home/skyitachi/lab/rocksdb/cmake-build-release)

add_executable(yedis src/main.cpp)
target_link_libraries(yedis rocksdb)