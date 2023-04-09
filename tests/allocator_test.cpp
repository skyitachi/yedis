//
// Created by Shiping Yao on 2023/3/15.
//

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include "allocator.h"

TEST(AllocatorTest, Basic) {
  yedis::Allocator allocator;
  auto ptr = allocator.AllocateData(4096);
  ASSERT_TRUE(ptr);

  auto allocatedData = allocator.Allocate(4096);

}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
