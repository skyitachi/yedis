//
// Created by skyitachi on 2020/8/8.
//

#include <rocksdb/db.h>
#include <iostream>
#include <vector>
#include <spdlog/spdlog.h>
#include "yedis_zset.hpp"
#include "disk_manager.hpp"
#include "buffer_pool_manager.hpp"
#include "btree.hpp"

const char *kDBPath = "data";
const char *kIndexFile = "btree.idx";

void test_fstream() {
  std::fstream test_file("test.idx", std::ios::in | std::ios::out | std::ios::binary);
  if (test_file.is_open()) {
    spdlog::info("open test file");
    test_file << "hello world";
    test_file.flush();
    test_file.close();
  } else {
    spdlog::error("cannot open test file");
  }
}

int main() {
  test_fstream();
  spdlog::set_level(spdlog::level::debug);
  auto disk_manager = new yedis::DiskManager(kIndexFile);
  auto yInstance = new yedis::YedisInstance();
  yInstance->disk_manager = disk_manager;
  auto buffer_pool_manager = new yedis::BufferPoolManager(16, yInstance);
  yInstance->buffer_pool_manager = buffer_pool_manager;
  auto zsetIndexTree = new yedis::BTree(yInstance);
  zsetIndexTree->init();
  zsetIndexTree->add("k1", "v1");
  yInstance->buffer_pool_manager->Flush();

  yInstance->disk_manager->ShutDown();
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