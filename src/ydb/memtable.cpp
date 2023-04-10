//
// Created by Shiping Yao on 2023/4/6.
//
#include <iostream>
#include <cassert>

#include <folly/ConcurrentSkipList.h>

#include "memtable.h"
#include "util.hpp"
#include "db_format.h"


namespace yedis {

MemTable::MemTable() {
  table_ = std::make_unique<SkipListType>(kMaxHeight);
}

bool MemTable::KeyComparator::operator()(const Slice& a, const Slice& b) const {
  uint32_t l_key_len;
  const char* a_start = GetVarint32Ptr(a.data(), a.data() + a.size(), &l_key_len);
  uint32_t r_key_len;
  const char* b_start = GetVarint32Ptr(b.data(), b.data() + b.size(), &r_key_len);
  const Slice l_user_key(a_start, l_key_len - 8);
  const Slice r_user_key(b_start, r_key_len - 8);

  int r = l_user_key.compare(r_user_key);
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(a_start + l_key_len - 8);
    const uint64_t bnum = DecodeFixed64(b_start + r_key_len - 8);
//    std::cout << "l user_key: " << l_user_key.ToString() <<  ", seq: " << anum << "\n r user_key: " << r_user_key.ToString()
//      << ", seq: " << bnum << std::endl;
    if (anum < bnum) {
      return true;
    }
    return false;
  }
  return r < 0;
}

void MemTable::Add(SequenceNumber seq, ValueType type, const Slice &key, const Slice &value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  uint64_t key_size = key.size();
  uint64_t value_size = value.size();
  uint64_t internal_key_size = key_size + 8;
  const uint64_t encoded_len = VarintLength(internal_key_size) + internal_key_size +
      VarintLength(value_size) + value_size;
  auto start = reinterpret_cast<char*>(alloc_.AllocateData(encoded_len));
  char *buf = EncodeVarint32(start, internal_key_size);
  std::memcpy(buf, key.data(), key_size);
  buf += key_size;
  EncodeFixed<uint64_t>(buf, (seq << 8) | (uint8_t)type);
  buf += 8;
  buf = EncodeVarint32(buf, value_size);
  std::memcpy(buf, value.data(), value_size);
  SkipList accessor(table_.get());

  accessor.add(Slice(start, encoded_len));
}

bool MemTable::Get(const LookupKey &key, std::string *value, Status *s) {
  // 这里保证了seq number >= key里的seq
  SkipList accessor(table_.get());
  auto iter = accessor.lower_bound(key.memtable_key());
  if (iter != accessor.end()) {
    uint32_t internal_key_len;
    const char* start = GetVarint32Ptr(iter->data(), iter->data() + 5, &internal_key_len);
    auto user_key = Slice(start, internal_key_len - 8);
    if (user_key.compare(key.user_key()) == 0) {
      auto tag = DecodeFixed<uint64_t>((void *) (start + internal_key_len - 8));
      auto vt = static_cast<ValueType>(tag & 0xff);
      switch (vt) {
        case ValueType::kTypeDeletion: {
          *s = Status::NotFound(Slice());
          return true;
        }
        case ValueType::kTypeValue: {
          uint32_t value_len;
          const char* value_start = GetVarint32Ptr(start + internal_key_len, start + internal_key_len + 5, &value_len);
          value->assign(value_start, value_len);
          *s = Status::OK();
          return true;
        }
      }
    }
  }
  return false;
}

Slice MemTable::EncodeEntry(SequenceNumber seq, ValueType type, const Slice &key, const Slice &value) {
  uint64_t key_size = key.size();
  uint64_t value_size = value.size();
  uint64_t internal_key_size = key_size + 8;
  const uint64_t encoded_len = VarintLength(internal_key_size) + internal_key_size +
                               VarintLength(value_size) + value_size;
  auto start = reinterpret_cast<char *>(alloc_.AllocateData(encoded_len));
  char *buf = EncodeVarint32(start, internal_key_size);
  memcpy(buf, key.data(), key_size);
  buf += key_size;
  EncodeFixed<uint64_t>(buf, (seq << 8) | (uint8_t)type);
  buf += 8;
  buf = EncodeVarint32(buf, value_size);

  std::memcpy(buf, value.data(), value_size);
  return {start, encoded_len};
}

}
