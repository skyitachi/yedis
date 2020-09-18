cmake_minimum_required(VERSION 3.10)

project(yedis)

set(CMAKE_CXX_STANDARD 17)

IF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    include_directories(./include ~/lab/rocksdb/include deps/spdlog/include)
    link_directories(~/lab/rocksdb/cmake-build-Release)
ELSE()
    include_directories(./include /home/skyitachi/lab/rocksdb/include deps/spdlog/include)
    link_directories(/home/skyitachi/lab/rocksdb/cmake-build-release)
ENDIF()

add_subdirectory(deps/spdlog)

aux_source_directory(src SOURCE_FILES)
add_executable(yedis ${SOURCE_FILES} include/util.hpp include/page.hpp include/config.hpp include/buffer_pool_manager.hpp src/buffer_pool_manager.cpp include/btree_leaf_node_page.hpp include/disk_manager.hpp include/btree_index_node_page.hpp include/btree_meta_page.hpp include/btree_node_page.h src/btree_index_node_page.cpp src/btree_node_page.cpp)
target_link_libraries(yedis rocksdb spdlog::spdlog)