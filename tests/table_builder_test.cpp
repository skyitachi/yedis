//
// Created by Shiping Yao on 2023/4/14.
//

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <vector>

#include "ydb/fs.hpp"
#include "allocator.h"
#include "ydb/table_builder.h"
#include "filter_policy.h"

using namespace yedis;

TEST(TableBuildTest, Basic) {
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("000001.ldb", O_APPEND | O_CREAT | O_RDWR);
  Options options{};
  options.filter_policy = NewBloomFilterPolicy(8);
  auto table_builder = new TableBuilder(options, file_handle.get());

  std::vector<std::pair<std::string, std::string>> kvs =
      {{"a", "v1"}, {"ab", "abc"}, {"abcd", "abcde"}, {"a", "ab"}};

  for(auto kv: kvs) {
    table_builder->Add(kv.first, kv.second);
  }
  table_builder->Finish();
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
