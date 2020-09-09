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

int main() {
  spdlog::set_level(spdlog::level::debug);
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

//  for (int i = 0; i < 10; i++) {
//    std::string k = "k";
//    std::string v = "v";
//    zsetIndexTree->add(k + std::to_string(i), v + std::to_string(i));
//  }

  // test for read
  for (int i = 0; i < 10; i++) {
    std::string k = "k";
    std::string v;
    auto s = zsetIndexTree->read(k + std::to_string(i), &v);
    if (!s.ok()) {
      spdlog::info("not found key={}, value={}", k + std::to_string(i));
      continue;
    }
    spdlog::info("found key={}, value={}", k + std::to_string(i), v);
  }
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