//
// Created by skyitachi on 2020/8/12.
//

#ifndef YEDIS_INCLUDE_YEDIS_ZSET_HPP_
#define YEDIS_INCLUDE_YEDIS_ZSET_HPP_

#include <rocksdb/db.h>
#include <mutex>
#include <vector>
#include "yedis.hpp"

namespace yedis {
using Slice = rocksdb::Slice;

class ZSet {
 public:
    ZSet(rocksdb::DB *db): db_(db) {}
    void zadd(const Slice& key, const std::vector<ScoreMember>& member);
 private:
    rocksdb::DB* db_;
    std::mutex mutex_;
};
}
#endif //YEDIS_INCLUDE_YEDIS_ZSET_HPP_
