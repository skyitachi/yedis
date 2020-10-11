//
// Created by skyitachi on 2020/8/8.
//

#include <rocksdb/db.h>
#include <iostream>
#include <vector>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_sinks.h>

#include "yedis_zset.hpp"
#include "disk_manager.hpp"
#include "buffer_pool_manager.hpp"
#include "btree.hpp"

const char *kDBPath = "data";
const char *kIndexFile = "btree.idx";

std::string* big_key(int length) {
  auto str = new std::string(length, 'a');
  return str;
}

void test_split_insert(yedis::BTree *root) {
  auto sz = yedis::PAGE_SIZE / 2;
  auto bg_key = big_key(sz);
  auto s = root->add(1, *bg_key);
  assert(s.ok());
  s = root->add(2, *bg_key);
  assert(s.ok());
}

// 没有分裂, 1个leaf node
// leaf node的容量其实和degree没什么关系
void test_normal_insert(yedis::BTree *root) {
  auto max = yedis::MAX_DEGREE;
  for (int i = 0; i < max; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    assert(s.ok());
  }
}

// 有分裂, 2个leaf node，1个index node
void test_small_value_split(yedis::BTree *root) {
  auto max = 300;
  for (int i = 0; i < max; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    assert(s.ok());
  }
}

// 3个leaf node， 1个index node
void test_medium_insert(yedis::BTree *root) {
  auto max = 500;
  for (int i = 0; i < max; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    assert(s.ok());
  }
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");
  spdlog::stdout_logger_mt("console");

//  spdlog::set_pattern("[%H:%M:%S %z] [%n] [%^---%L---%$] [thread %t] %v");
//  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");
//  spdlog::stdout_logger_mt("console");
//
//  auto console = spdlog::get("console");
//  spdlog::set_default_logger(console);
  char *index_file = const_cast<char *>(kIndexFile);
  if (argc > 1) {
    index_file = argv[argc - 1];
  }
  SPDLOG_INFO("index file is: {}", index_file);
  auto disk_manager = new yedis::DiskManager(index_file);
  auto yInstance = new yedis::YedisInstance();
  yInstance->disk_manager = disk_manager;
  auto buffer_pool_manager = new yedis::BufferPoolManager(16, yInstance);
  yInstance->buffer_pool_manager = buffer_pool_manager;
  auto zsetIndexTree = new yedis::BTree(yInstance);

//  test_split_insert(zsetIndexTree);

//  test_normal_insert(zsetIndexTree);
  // test_small_value_split(zsetIndexTree);
  test_medium_insert(zsetIndexTree);

  yInstance->buffer_pool_manager->Flush();

  yInstance->disk_manager->ShutDown();
  // zsetIndexTree->destroy();
//  rocksdb::DB* db;
//  rocksdb::Options options;
//  options.create_if_missing = true;
//  auto status = rocksdb::DB::Open(options, kDBPath, &db);
//  assert(status.ok());
//
//  auto zset = new yedis::ZSet(db);
//
//  std::vector<yedis::ScoreMember> members;
//  members.push_back(yedis::ScoreMember{1, "k1"});
//  members.push_back(yedis::ScoreMember{10, "k2"});
//  members.push_back(yedis::ScoreMember{2, "k3"});
//
//  zset->zadd("test_zset_key", members);
//
//  auto ret = zset->zrange("test_zset_key", 0, 2);
//  for(const auto& item: ret) {
//    spdlog::info("value is: {}", item);
//  }
//
//  delete db;
}