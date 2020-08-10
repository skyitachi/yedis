//
// Created by skyitachi on 2020/8/8.
//

#include <rocksdb/db.h>
#include <iostream>
#include <vector>
#include <mutex>
#include <cstring>

const char *kDBPath = "data";
const char *kMetaKey = "meta_";
std::mutex gMutex;
const rocksdb::ReadOptions default_read_options_;
const rocksdb::WriteOptions default_write_options_;

inline void EncodeFixed32(char* dst, uint32_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  // little endian
  std::memcpy(buffer, &value, sizeof(uint32_t));
}

inline uint32_t DecodeFixed32(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  uint32_t result;
  std::memcpy(&result, buffer, sizeof(uint32_t));
  return result;
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void zadd(rocksdb::DB* db, const std::string& key, int score, const std::string& member) {
  std::string meta_key = kMetaKey + key;
  rocksdb::WriteBatch batch;
  std::lock_guard<std::mutex> lock(gMutex);
  // fetch meta data need transaction or mutex
  std::string value;
  int count = 0;
  std::string meta_value;
  auto status = db->Get(default_read_options_, meta_key, &value);

  if (status.IsNotFound()) {
    PutFixed32(&meta_value, 1);
    batch.Put(meta_key, meta_value);
  } else {
    count = DecodeFixed32(value.c_str());
    PutFixed32(&meta_value, count + 1);
    batch.Put(meta_key, meta_value);
  }
  // value_key = key + score + member
  std::string value_key = key;
  PutFixed32(&value_key, score);
  value_key.append(member);
  batch.Put(value_key, rocksdb::Slice());

  // index
  std::string index_key = key;
  PutFixed32(&index_key, count);
  batch.Put(index_key, member);
//  std::string index_value;
//  PutFixed32(&index_value, score);
//  index_value.append(member);
//  batch.Put(index_key, index_value);

  status = db->Write(default_write_options_, &batch);
  assert(status.ok());
}

std::vector<std::string> zrange(rocksdb::DB *db, const std::string& key, int start, int stop) {
  std::string index_key = key;
  PutFixed32(&index_key, start);

  // use iterator
  rocksdb::ReadOptions read_options;
  rocksdb::Slice lower_bound(index_key);
  read_options.iterate_lower_bound = &lower_bound;
  auto it = db->NewIterator(read_options);
  std::vector<std::string> ret;
  int count = 0;
  for (it->Seek(lower_bound); it->Valid(); it->Next()) {
    ret.push_back(it->value().ToString());
    if (start + count > stop) {
      break;
    }
    count += 1;
  }
  return ret;
}

int main() {
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  auto status = rocksdb::DB::Open(options, kDBPath, &db);
  assert(status.ok());

  std::cout << "hello world" << std::endl;
  zadd(db, "test_zset_key", 1, "k1");
  zadd(db, "test_zset_key", 10, "k2");
  zadd(db, "test_zset_key", 2, "k3");

  auto ret = zrange(db, "test_zset_key", 0, 2);
  for(auto item: ret) {
    std::cout << "value is: " << item << std::endl;
  }

  delete db;
}