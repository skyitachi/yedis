//
// Created by Shiping Yao on 2023/4/7.
//

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <glog/logging.h>
#include <folly/ConcurrentSkipList.h>
#include "ydb/memtable.h"
#include "ydb/db_format.h"
#include "util.hpp"

TEST(MemTableTest, Basic) {
  using namespace yedis;
  MemTable memtable{};
  memtable.Add(1, ValueType::kTypeValue, "k1", "v1");
  memtable.Add(2, ValueType::kTypeValue, "k2", "v2");

  memtable.Add(3, ValueType::kTypeValue, "k1", "v11");
  memtable.Add(5, ValueType::kTypeDeletion, "k1", "");

  auto lk = LookupKey("k1", 1);
  Status status;
  std::string value;
  bool success = memtable.Get(lk, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value, "v1");

  auto lk0 = LookupKey("k1", 2);
  success = memtable.Get(lk, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value, "v1");

  // 返回比6小的seq 中最大的一个
  auto lk1 = LookupKey("k1", 6);
  success = memtable.Get(lk1, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.IsNotFound());

  auto lk1_2 = LookupKey("k1", 4);
  success = memtable.Get(lk1_2, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value, "v11");

  auto lk2 = LookupKey("k2", 2);
  success = memtable.Get(lk2, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value, "v2");

  auto lk3 = LookupKey("k2", 1);
  success = memtable.Get(lk3, &value, &status);
  ASSERT_FALSE(success);
}

TEST(SkipListTest, LowerBound) {
  auto skip_list = folly::ConcurrentSkipList<int>::create(10);
  skip_list.add(1);
  skip_list.add(3);
  skip_list.add(5);
  skip_list.add(6);

  auto it = skip_list.lower_bound(3);
  ASSERT_TRUE(it != skip_list.end());
  ASSERT_EQ(*it, 3);

  {
    auto it = skip_list.lower_bound(5);
    ASSERT_TRUE(it != skip_list.end());
    ASSERT_EQ(*it, 5);
  }
  {
    auto first = skip_list.first();
    auto last = skip_list.last();
    ASSERT_EQ(*first, 1);
    ASSERT_EQ(*last, 6);
  }
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(*argv);

  return RUN_ALL_TESTS();
}
