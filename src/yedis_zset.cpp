//
// Created by skyitachi on 2020/8/12.
//

#include <yedis_zset.hpp>
#include <spdlog/spdlog.h>
#include <write_batch.h>
#include <options.h>
#include <db.h>

namespace yedis {

Status ZSet::zadd(const Slice &key, const std::vector<ScoreMember> &member) {
  const std::string key_str = key.ToString();
  std::string meta_key = kMetaKey + key_str;
  WriteBatch batch;
  std::lock_guard<std::mutex> lock(mutex_);
  // fetch meta data need transaction or mutex
  std::string value;
  int count = 0;
  std::string meta_value;
  auto status = db_->Get(default_read_options_, meta_key, &value);

  if (status.IsNotFound()) {
    PutFixed32(&meta_value, 0);
    batch.Put(meta_key, meta_value);
  } else {
    count = DecodeFixed32(value.c_str());
    count += 1;
    PutFixed32(&meta_value, count);
    batch.Put(meta_key, meta_value);
  }
  spdlog::info("current count {}", count);
  for (auto& score_member: member) {
    std::string value_key = key_str;
    PutFixed32(&value_key, score_member.score);
    value_key.append(score_member.member);
    batch.Put(value_key, Slice());

    std::string index_key = kIndexKeyPrefix + key_str;
    PutFixed32(&index_key, count);
    spdlog::info("index_key: {}", index_key.size());
    spdlog::info("member value: {}", score_member.member);
    batch.Put(index_key, score_member.member);
    count += 1;
  }
  return db_->Write(default_write_options_, &batch);
}

ZSet::StrList ZSet::zrange(const std::string& key, int start, int stop) {
  std::string index_key = kIndexKeyPrefix + key;
  PutFixed32(&index_key, start);

  // use iterator
  ReadOptions read_options;
  Slice lower_bound(index_key);
  read_options.iterate_lower_bound = lower_bound;
  auto it = db_->NewIterator(read_options);
  std::vector<std::string> ret;
  int count = 0;
  for (it->Seek(lower_bound); it->Valid(); it->Next()) {
    ret.push_back(it->value().ToString());
    if (start + count >= stop) {
      break;
    }
    count += 1;
  }
  return ret;
}

Status ZSet::zrem(const std::string &key, const StrList &members, int* removed) {
  return Status::NotSupported("not implement");
}
}
