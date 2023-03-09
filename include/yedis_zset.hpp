//
// Created by skyitachi on 2020/8/12.
//

#ifndef YEDIS_INCLUDE_YEDIS_ZSET_HPP_
#define YEDIS_INCLUDE_YEDIS_ZSET_HPP_

#include <mutex>
#include <vector>
#include "yedis.hpp"
#include "util.hpp"
#include "common/status.h"
#include <options.h>

namespace yedis {
  class DB;

  class ZSet {
    const char *kMetaKey = "meta_";
    const char *kIndexKeyPrefix = "index_";

  public:
    typedef std::vector<std::string> StrList;

    ZSet(DB *db) : db_(db) {}

    Status zadd(const Slice &key, const std::vector<ScoreMember> &member);

    StrList zrange(const std::string &key, int start, int stop);

    // return counts of members removed
    Status zrem(const std::string &key, const StrList &members, int *ret);

  private:
    DB *db_;
    std::mutex mutex_;
    ReadOptions default_read_options_;
    WriteOptions default_write_options_;
  };
}
#endif //YEDIS_INCLUDE_YEDIS_ZSET_HPP_
