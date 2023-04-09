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

  auto lk = LookupKey("k1", 1);
  Status status;
  std::string value;
  bool success = memtable.Get(lk, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value, "v1");

  // lookup key seq 高于最大seq，返回false
  // TODO: this case not work well
  auto lk1 = LookupKey("k1", 2);
  success = memtable.Get(lk, &value, &status);
  ASSERT_FALSE(success);

  auto lk2 = LookupKey("k2", 2);
  success = memtable.Get(lk2, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value, "v2");

  auto lk3 = LookupKey("k2", 1);
  success = memtable.Get(lk3, &value, &status);
  ASSERT_TRUE(success);
  ASSERT_TRUE(status.ok());
}

TEST(SliceAllocatorTest, Basic) {
  using namespace yedis;
  Slice s1 = "hello world";
  spdlog::info("sizeof(Slice) = {}", sizeof(s1));

  Slice s2 = s1;
  spdlog::info("sizeof(Slice) = {}", sizeof(s2));
  spdlog::info("s2: {}, size: {}", s2.ToString(), s2.size());
}

TEST(SkipListTest, LowerBound) {
  auto skip_list = folly::ConcurrentSkipList<int>::create(10);
  skip_list.add(1);
  skip_list.add(3);

  auto it = skip_list.lower_bound(3);
  ASSERT_TRUE(it != skip_list.end());
  ASSERT_EQ(*it, 3);
}

TEST(RawSliceSkipList, Basic) {
  using namespace yedis;
  MemTable memtable{};
  auto skip_list = folly::ConcurrentSkipList<Slice, MemTable::KeyComparator>::create(2);
  Slice k1 = "k1";
  Slice v1 = "v1";
  Slice entry = memtable.EncodeEntry(1, ValueType::kTypeValue, k1, v1);
  uint32_t ks;
//  auto origin_ptr = GetVarint32Ptr(entry.data(), entry.data() + 5, &ks);
//  std::cout << "got internal key length: " << ks << ", offset: " << origin_ptr - entry.data() << std::endl;
  printf("data ptr: %p, first byte: %d\n", entry.data(), entry.data()[0]);

  std::cout << "before add\n";
  skip_list.add(entry);

  std::cout << "after add\n";

  auto first = *skip_list.first();
  auto ptr = GetVarint32Ptr(first.data(), first.data() + 5, &ks);
  std::cout << "got internal key length: " << ks << ", offset: " << ptr - first.data() << std::endl;

  auto origin_ptr = GetVarint32Ptr(entry.data(), entry.data() + 5, &ks);
  std::cout << "got internal key length: " << ks << ", offset: " << origin_ptr - entry.data() << std::endl;
  std::cout << entry.size() << std::endl;

}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(*argv);

  return RUN_ALL_TESTS();
}
