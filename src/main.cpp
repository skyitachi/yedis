//
// Created by skyitachi on 2020/8/8.
//

#include <rocksdb/db.h>
#include <iostream>
#include <vector>
#include <spdlog/spdlog.h>
#include "yedis_zset.hpp"

const char *kDBPath = "data";

int main(){
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  auto status = rocksdb::DB::Open(options, kDBPath, &db);
  assert(status.ok());

  auto zset = new yedis::ZSet(db);

  std::vector<yedis::ScoreMember> members;
  members.push_back(yedis::ScoreMember{1, "k1"});
  members.push_back(yedis::ScoreMember{10, "k2"});
  members.push_back(yedis::ScoreMember{2, "k3"});

  zset->zadd("test_zset_key", members);

  auto ret = zset->zrange("test_zset_key", 0, 2);
  for(const auto& item: ret) {
    spdlog::info("value is: {}", item);
  }

  delete db;
}