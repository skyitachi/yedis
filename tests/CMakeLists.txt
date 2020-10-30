add_executable(btree_node_page_test btree_node_page_test.cpp)
target_link_libraries(btree_node_page_test yedis spdlog::spdlog gtest)

add_executable(btree_small_page_test btree_small_page_test.cpp)
target_link_libraries(btree_small_page_test yedis spdlog::spdlog gtest)

add_executable(btree_reopen_test btree_reopen_test.cpp)
target_link_libraries(btree_reopen_test yedis spdlog::spdlog gtest)

add_executable(list_test list_test.cpp)
target_link_libraries(list_test spdlog::spdlog gtest)

add_executable(btree_small_pool_test btree_small_pool_test.cpp)
target_link_libraries(btree_small_pool_test yedis spdlog::spdlog gtest)