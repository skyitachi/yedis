//
// Created by skyitachi on 2020/8/12.
//

#ifndef YEDIS_INCLUDE_YEDIS_HPP_
#define YEDIS_INCLUDE_YEDIS_HPP_
#include <string>
#include <rocksdb/db.h>
namespace yedis {
using Status = rocksdb::Status;
struct ScoreMember {
  double score;
  std::string member;
  bool operator == (const ScoreMember& sm) const {
    return (sm.score == score && sm.member == member);
  }
};
}
#endif //YEDIS_INCLUDE_YEDIS_HPP_
