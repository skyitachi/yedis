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

int MemTable::KeyComparator::operator()(const Slice& a, const Slice& b) const {
  uint32_t l_key_len;
  const char* a_start = GetVarint32Ptr(a.data(), a.data() + a.size(), &l_key_len);
  uint32_t r_key_len;
  const char* b_start = GetVarint32Ptr(b.data(), b.data() + b.size(), &r_key_len);
  printf("a data_ptr: %p, data_ptr: %p, b data_size: %ld\n", a.data(), b.data(), b.size());
  std::cout << "l_key_len: " << l_key_len << ", r_key_len: " << r_key_len << std::endl;

  const Slice l_user_key(a_start, l_key_len - 8);
  const Slice r_user_key(b_start, r_key_len - 8);
  std::cout << "key1: " << l_user_key.ToString() << ", key2: " << r_user_key.ToString() << std::endl;
  int r = l_user_key.compare(r_user_key);
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(a_start + l_key_len - 8);
    const uint64_t bnum = DecodeFixed64(b_start + r_key_len - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = 1;
    }
  }
  return r;
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
  std::cout << "encoded_len: " << encoded_len << std::endl;
//  auto start = reinterpret_cast<char *>(alloc_.Allocate(encoded_len).get());
  auto start = reinterpret_cast<char*>(malloc(encoded_len));
  auto ptr = start;
  printf("allocated: %p\n", ptr);
  char *buf = EncodeVarint32(ptr, internal_key_size);
  uint32_t ks;
  GetVarint32Ptr(start, start + 5, &ks);
  std::cout << "got internal key length: " << ks << ", offset: " << buf - start << std::endl;
  std::memcpy(buf, key.data(), key_size);


  buf += key_size;
  EncodeFixed<uint64_t>(buf, (seq << 8) | (uint8_t)type);


  buf += 8;
  buf = EncodeVarint32(buf, value_size);


  std::memcpy(buf, value.data(), value_size);
  buf += value_size;


  SkipList accessor(table_.get());

  auto sli = Slice(start, encoded_len);
  GetVarint32Ptr(sli.data(), sli.data() + 5, &ks);
  printf("before add ptr: %p, key_len: %d, first byte: %d\n", start, ks, start[0]);

  // TODO: add 会对内存进行操作吗
//  accessor.insert(sli);
  accessor.add(sli);

  GetVarint32Ptr(sli.data(), sli.data() + 5, &ks);
  printf("after add ptr: %p, key_len: %d, first byte: %d\n", start, ks, sli.data()[0]);
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
      auto packed = DecodeFixed<uint64_t>((void *) (start + internal_key_len - 8));
      uint64_t seq = packed >> 8;
      auto vt = static_cast<ValueType>(seq & 0xff);
      if (vt == ValueType::kTypeDeletion) {
        *s = Status::NotFound("not found");
        return true;
      }
      uint32_t value_len;
      const char* value_start = GetVarint32Ptr(start + internal_key_len, start + internal_key_len + 5, &value_len);
      value->append(value_start, value_len);
      *s = Status::OK();
      return true;
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
    std::cout << "encoded_len: " << encoded_len << std::endl;
//    auto start = reinterpret_cast<char *>(alloc_.Allocate(encoded_len).get());
    auto start = (char *)malloc(encoded_len);
    auto ptr = start;
    printf("allocated: %p\n", start);
    char *buf = EncodeVarint32(ptr, internal_key_size);
//    uint32_t ks;
//    GetVarint32Ptr(start, start + 5, &ks);
//    std::cout << "got internal key length: " << ks << ", offset: " << buf - start << std::endl;
    memcpy(buf, key.data(), key_size);


    buf += key_size;
    EncodeFixed<uint64_t>(buf, (seq << 8) | (uint8_t)type);


    buf += 8;
    buf = EncodeVarint32(buf, value_size);

    std::memcpy(buf, value.data(), value_size);
    buf += value_size;

    return Slice(start, encoded_len);
  }

}
