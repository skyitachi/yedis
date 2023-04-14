//
// Created by Shiping Yao on 2023/4/14.
//

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <vector>

#include <leveldb/table_builder.h>
#include <leveldb/options.h>
#include <leveldb/filter_policy.h>
#include <leveldb/env.h>
#include <leveldb/comparator.h>

#include "ydb/fs.hpp"
#include "allocator.h"
#include "ydb/table_builder.h"
#include "filter_policy.h"
#include "table.h"

TEST(TableBuildTest, Basic) {
  using namespace yedis;
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("000001.ydb", O_CREAT | O_RDWR | O_TRUNC);
  Options options{};
  options.compression = CompressionType::kNoCompression;
  options.filter_policy = NewBloomFilterPolicy(8);
  auto table_builder = new TableBuilder(options, file_handle.get());

  std::vector<std::pair<std::string, std::string>> kvs =
      {{"a", "v1"}, {"ab", "abc"}, {"abcd", "abcde"}, {"abcde", "ab"}};

  for(auto kv: kvs) {
    table_builder->Add(kv.first, kv.second);
  }
  table_builder->Finish();

  delete table_builder;
}

TEST(TableReaderTest, Basic) {
  using namespace yedis;
  LocalFileSystem fs;
  auto read_file_handle = fs.OpenFile("000001.ydb", O_RDONLY);
  Options options{};
  Table* table;
  Table::Open(options, read_file_handle.get(), &table);
}

TEST(LeveldbTableBuildTest, Basic) {
  using namespace leveldb;
  leveldb::Options options{};
  options.compression = leveldb::CompressionType::kNoCompression;
  options.filter_policy = leveldb::NewBloomFilterPolicy(8);
  spdlog::info("comparator: {}", options.comparator->Name());
  spdlog::info("policy name: {}", options.filter_policy->Name());
  leveldb::Env* env = leveldb::Env::Default();
  env->DeleteFile("000001.ldb");
  leveldb::WritableFile* file;
  Status status = env->NewWritableFile("000001.ldb", &file);
  ASSERT_TRUE(status.ok());

  auto table_builder = new leveldb::TableBuilder(options, file);
  std::vector<std::pair<std::string, std::string>> kvs =
      {{"a", "v1"}, {"ab", "abc"}, {"abcd", "abcde"}, {"abcde", "ab"}};

  for(auto kv: kvs) {
    table_builder->Add(kv.first, kv.second);
  }
  table_builder->Finish();
  spdlog::info("status: {}", table_builder->status().ToString());
  delete table_builder;
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
