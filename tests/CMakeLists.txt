include_directories("${CMAKE_SOURCE_DIR}/src")

add_executable(btree_node_page_test btree_node_page_test.cpp)
target_link_libraries(btree_node_page_test yedis spdlog gtest)

add_executable(btree_small_page_test btree_small_page_test.cpp)
target_link_libraries(btree_small_page_test yedis spdlog gtest)

add_executable(btree_reopen_test btree_reopen_test.cpp)
target_link_libraries(btree_reopen_test yedis spdlog gtest)

add_executable(list_test list_test.cpp)
target_link_libraries(list_test spdlog gtest)

add_executable(btree_small_pool_test btree_small_pool_test.cpp)
target_link_libraries(btree_small_pool_test yedis spdlog gtest)

add_executable(btree_remove_test btree_remove_test.cpp)
target_link_libraries(btree_remove_test yedis spdlog gtest)

add_executable(rwlatch_test rwlatch_test.cpp)
target_link_libraries(rwlatch_test spdlog gtest)

add_executable(allocator_test allocator_test.cpp)
target_link_libraries(allocator_test yedis spdlog gtest)

find_package(absl REQUIRED)
add_executable(fs_test fs_test.cpp)
target_link_libraries(fs_test yedis spdlog gtest absl::strings crc32c)