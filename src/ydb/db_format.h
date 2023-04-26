//
// Created by Shiping Yao on 2023/4/6.
//

#ifndef YEDIS_DB_FORMAT_H
#define YEDIS_DB_FORMAT_H

#include <inttypes.h>
#include <limits>

#include "slice.h"
#include "comparator.h"

namespace yedis {

  enum class FileType: uint8_t {
    kLogFile,
    kDBLockFile,
    kTableFile,
    kDescriptorFile,
    kCurrentFile,
    kTempFile,
    kInfoLogFile  // Either the current one, or an old one
  };

  enum class ValueType: uint8_t { kTypeDeletion = 0x0, kTypeValue = 0x1 };

  static const ValueType kValueTypeForSeek = ValueType::kTypeValue;

  typedef uint64_t SequenceNumber;

  static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

  struct ParsedInternalKey {
    Slice user_key;
    SequenceNumber sequence;
    ValueType type;

    ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
    ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
        : user_key(u), sequence(seq), type(t) {}
  };

  void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

  inline Slice ExtractUserKey(const Slice& internal_key) {
    assert(internal_key.size() >= 8);
    return {internal_key.data(), internal_key.size() - 8};
  }

  inline ValueType ExtractValueType(const Slice& internal_key) {
    assert(internal_key.size() >= 8);
    return static_cast<ValueType>(internal_key.data()[internal_key.size() - 1]);
  }

  class InternalKey {
  public:
    InternalKey() {}
    InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
      AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
    }

    bool DecodeFrom(const Slice& s) {
      rep_.assign(s.data(), s.size());
      return !rep_.empty();
    }

    Slice Encode() const {
      assert(!rep_.empty());
      return rep_;
    }

    Slice user_key() const { return ExtractUserKey(rep_); }

    void SetFrom(const ParsedInternalKey& p) {
      rep_.clear();
      AppendInternalKey(&rep_, p);
    }

    void Clear() { rep_.clear(); }

  private:
    std::string rep_;
  };

  class InternalKeyComparator: public Comparator {
  private:
    const Comparator* user_comparator_;
  public:
    explicit InternalKeyComparator(const Comparator* c): user_comparator_(c) {}
    ~InternalKeyComparator();
    const char* Name() const override;

    int Compare(const Slice& a, const Slice& b) const override;
    int Compare(const InternalKey& a, const InternalKey& b) const;
    void FindShortestSeparator(std::string* start, const Slice& limit) const override;
    void FindShortSuccessor(std::string* key) const override;

    const Comparator* user_comparator() const { return user_comparator_; }

  };

  class LookupKey {
  public:
    // Initialize *this for looking up user_key at a snapshot with
    // the specified sequence number.
    LookupKey(const Slice& user_key, SequenceNumber sequence);

    LookupKey(const LookupKey&) = delete;
    LookupKey& operator=(const LookupKey&) = delete;

    ~LookupKey();

    // Return a key suitable for lookup in a MemTable.
    Slice memtable_key() const { return Slice(start_, end_ - start_); }

    // Return an internal key (suitable for passing to an internal iterator)
    Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

    // Return the user key
    Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

  private:
    // We construct a char array of the form:
    //    klength  varint32               <-- start_
    //    userkey  char[klength]          <-- kstart_
    //    tag      uint64
    //                                    <-- end_
    // The array is a suitable MemTable key.
    // The suffix starting with "userkey" can be used as an InternalKey.
    const char* start_;
    const char* kstart_;
    const char* end_;
    char space_[200];  // Avoid allocation for short keys
  };

  inline LookupKey::~LookupKey() {
    if (start_ != space_) delete[] start_;
  }
// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string& filename, uint64_t* number, FileType* type);
bool ConsumeDecimalNumber(Slice* in, uint64_t* val);
}

#endif //YEDIS_DB_FORMAT_H
