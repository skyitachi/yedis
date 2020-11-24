//
// Created by skyitachi on 2020/10/28.
//

#include <list>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

TEST(ListTest, NormalTest) {
  std::list<int> freeList;
  freeList.push_back(1);
  freeList.push_back(2);
  freeList.push_back(3);

  auto it = freeList.begin();
  auto back = freeList.begin();
  back++;
  back++;
  it++;
  freeList.erase(it);
  SPDLOG_INFO("before current back is: {}", *back);
  back--;
  SPDLOG_INFO("current back is: {}", *back);
  ASSERT_TRUE(freeList.size() == 2);
  ASSERT_TRUE(*it == 2);
}


int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
