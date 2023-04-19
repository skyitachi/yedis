//
// Created by Shiping Yao on 2023/4/19.
//
#include <spdlog/spdlog.h>
#include <gtest/gtest.h>
#include <thread>

#include <folly/concurrency/MPMCQueue.h>

TEST(FollyConcurrentQueue, Basic) {

}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
