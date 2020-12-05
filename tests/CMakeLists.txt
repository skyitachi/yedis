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

add_executable(concurrent_test btree_concurrent_test.cpp)
target_link_libraries(concurrent_test yedis spdlog gtest)