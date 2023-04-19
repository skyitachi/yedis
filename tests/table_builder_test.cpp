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
#include "iterator.h"
#include "cache.h"

#include "random.h"
#include "test_util.h"

using KeyType = std::string;
using ValueType = std::string;
using KVType = std::pair<KeyType, ValueType>;

std::vector<std::pair<std::string, std::string>> prepare(int size) {
  using namespace yedis;
  auto* rnd = new Random();
  std::vector<std::pair<std::string, std::string>> ret;
  for (int i = 0; i < size; i++) {
    std::string value;
    std::string key;
    test::RandomString(rnd, 24, &key);
    test::RandomString(rnd, 128,&value);
    ret.emplace_back(key, value);
  }
  return ret;
}

TEST(TableBuildTest, Basic) {
  using namespace yedis;
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("000001.ydb", O_CREAT | O_RDWR | O_TRUNC);
  Options options{};
  options.compression = CompressionType::kNoCompression;
  options.filter_policy = NewBloomFilterPolicy(8);
  options.block_size = 4096;
  auto table_builder = new TableBuilder(options, file_handle.get());

  std::vector<std::pair<std::string, std::string>> kvs =
      prepare(1024);

  std::sort(kvs.begin(), kvs.end(), [](const KVType & p1, const KVType & p2) {
    return p1.first < p2.first;
  });

  for(auto kv: kvs) {
    table_builder->Add(kv.first, kv.second);
  }
  table_builder->Finish();

  delete table_builder;

  auto read_file_handle = fs.OpenFile("000001.ydb", O_RDONLY);
  options.compression = CompressionType::kNoCompression;
  options.block_cache = NewLRUCache(4096);

  Table* table;
  Status status = Table::Open(options, read_file_handle.get(), &table);
  ASSERT_TRUE(status.ok());

  ReadOptions read_opt;
  read_opt.verify_checksums = true;
  read_opt.fill_cache = true;

  auto iter = table->NewIterator(read_opt);
  iter->SeekToFirst();
  int idx = 0;
  while (iter->Valid()) {
    ASSERT_TRUE(idx < kvs.size());
    ASSERT_EQ(iter->key().ToString(), kvs[idx].first);
    iter->Next();
    idx++;
  }
  ASSERT_EQ(idx, kvs.size());

  iter->SeekToLast();
  idx = kvs.size() - 1;
  while (iter->Valid()) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), kvs[idx].first);
    iter->Prev();
    idx--;
  }
  ASSERT_EQ(idx, -1);


  delete iter;
  spdlog::info("total usage: {}", options.block_cache->TotalCharge());
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

TEST(TableTest, Basic) {
  using namespace yedis;
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("000001.ydb", O_RDONLY);
  Options options{};
  options.compression = CompressionType::kNoCompression;
  options.filter_policy = NewBloomFilterPolicy(8);
  options.block_cache = NewLRUCache(4096);

  Table* table;
  Status status = Table::Open(options, file_handle.get(), &table);
  ASSERT_TRUE(status.ok());

  ReadOptions read_opt;
  read_opt.verify_checksums = true;
  read_opt.fill_cache = true;

  auto iter = table->NewIterator(read_opt);
  iter->SeekToFirst();
  while (iter->Valid()) {
    spdlog::info("key: {}, value: {}", iter->key().ToString(), iter->value().ToString());
    iter->Next();
  }
  delete iter;
  spdlog::info("total usage: {}", options.block_cache->TotalCharge());
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
