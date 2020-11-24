//
// Created by skyitachi on 2020/8/12.
//

#ifndef YEDIS_INCLUDE_YEDIS_ZSET_HPP_
#define YEDIS_INCLUDE_YEDIS_ZSET_HPP_

#include <rocksdb/db.h>
#include <mutex>
#include <vector>
#include "yedis.hpp"
#include "util.hpp"

namespace yedis {
using Slice = rocksdb::Slice;
using Status = rocksdb::Status;

class ZSet {
  const char *kMetaKey = "meta_";
  const char *kIndexKeyPrefix = "index_";

 public:
  typedef std::vector<std::string> StrList;
    ZSet(rocksdb::DB *db): db_(db) {}
    Status zadd(const Slice& key, const std::vector<ScoreMember>& member);
    StrList zrange(const std::string& key, int start, int stop);
    // return counts of members removed
    Status zrem(const std::string& key, const StrList& members, int *ret);
 private:
    rocksdb::DB* db_;
    std::mutex mutex_;
    rocksdb::ReadOptions default_read_options_;
    rocksdb::WriteOptions default_write_options_;
};
}
#endif //YEDIS_INCLUDE_YEDIS_ZSET_HPP_
