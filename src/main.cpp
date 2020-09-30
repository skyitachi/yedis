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

int main() {
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

  auto disk_manager = new yedis::DiskManager(kIndexFile);
  auto yInstance = new yedis::YedisInstance();
  yInstance->disk_manager = disk_manager;
  auto buffer_pool_manager = new yedis::BufferPoolManager(16, yInstance);
  yInstance->buffer_pool_manager = buffer_pool_manager;
  auto zsetIndexTree = new yedis::BTree(yInstance);

  test_split_insert(zsetIndexTree);

//  zsetIndexTree->add("k2]")
//  for (int i = 9; i >= 0; i--) {
//    std::string k = "k";
//    std::string v = "v";
//    zsetIndexTree->add(i, v + std::to_string(i));
//  }
//  {
//    yedis::Status s;
////    s = zsetIndexTree->add(2, "v2");
////    assert(s.ok());
//    s = zsetIndexTree->add(1, "v1");
//    assert(s.ok());
////    s = zsetIndexTree->add(3, "v1");
////    assert(s.ok());
//  }

  // test for read
//  for (int i = 0; i < 10; i++) {
//    std::string k = "k";
//    std::string v;
//    auto s = zsetIndexTree->read(i, &v);
//    if (!s.ok()) {
//      spdlog::info("not found key={}, value={}", k + std::to_string(i));
//      continue;
//    }
//    spdlog::info("found key={}, value={}", k + std::to_string(i), v);
//  }
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