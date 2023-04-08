//
// Created by Shiping Yao on 2023/4/6.
//
#include <string>

#include "db_format.h"
#include "util.hpp"


namespace yedis {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | (std::uint8_t)t;
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed<uint64_t>(result, PackSequenceAndType(key.sequence, key.type));
}

InternalKeyComparator::~InternalKeyComparator() {}

const char* InternalKeyComparator::Name() const {
  return "yedis.InternalKeyComparator";
}

int InternalKeyComparator::Compare(const Slice &a, const Slice &b) const {
  int r = user_comparator_->Compare(ExtractUserKey(a), ExtractUserKey(b));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(a.data() + a.size() - 8);
    const uint64_t bnum = DecodeFixed64(b.data() + b.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = 1;
    }
  }
  return r;
}

int InternalKeyComparator::Compare(const yedis::InternalKey &a, const yedis::InternalKey &b) const {
  return Compare(a.Encode(), b.Encode());
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}