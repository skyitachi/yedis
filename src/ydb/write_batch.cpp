//
// Created by skyitachi on 23-4-22.
//
#include "write_batch.h"
#include "memtable.h"
#include "write_batch_internal.h"
#include "table_format.h"
#include "util.hpp"
#include "db_format.h"

namespace yedis {
  static const size_t kHeader = 12;

  WriteBatch::WriteBatch() { Clear(); }

  WriteBatch::~WriteBatch() = default;

  WriteBatch::Handler::~Handler() = default;

  void WriteBatch::Clear() {
    rep_.clear();
    rep_.resize(kHeader);
  }

  size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

  Status WriteBatch::Iterate(Handler* handler) const {
    Slice input(rep_);
    if (input.size() < kHeader) {
      return Status::Corruption("malformed WriteBatch (too small)");
    }

    input.remove_prefix(kHeader);
    Slice key, value;
    int found = 0;
    while (!input.empty()) {
      found++;
      auto tag = static_cast<ValueType>(input[0]);
      input.remove_prefix(1);
      switch (tag) {
        case ValueType::kTypeValue:
          if (GetLengthPrefixedSlice(&input, &key) &&
              GetLengthPrefixedSlice(&input, &value)) {
            handler->Put(key, value);
          } else {
            return Status::Corruption("bad WriteBatch Put");
          }
          break;
        case ValueType::kTypeDeletion:
          if (GetLengthPrefixedSlice(&input, &key)) {
            handler->Delete(key);
          } else {
            return Status::Corruption("bad WriteBatch Delete");
          }
          break;
        default:
          return Status::Corruption("unknown WriteBatch tag");
      }
    }
    if (found != WriteBatchInternal::Count(this)) {
      return Status::Corruption("WriteBatch has wrong count");
    } else {
      return Status::OK();
    }
  }

  int WriteBatchInternal::Count(const WriteBatch* b) {
    return DecodeFixed32(b->rep_.data() + 8);
  }

  void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
    EncodeFixed32(&b->rep_[8], n);
  }

  SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
    return SequenceNumber(DecodeFixed64(b->rep_.data()));
  }

  void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
    EncodeFixed64(&b->rep_[0], seq);
  }

  void WriteBatch::Put(const Slice& key, const Slice& value) {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(ValueType::kTypeValue));
    PutLengthPrefixedSlice(&rep_, key);
    PutLengthPrefixedSlice(&rep_, value);
  }

  void WriteBatch::Delete(const Slice& key) {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(ValueType::kTypeDeletion));
    PutLengthPrefixedSlice(&rep_, key);
  }

  void WriteBatch::Append(const WriteBatch& source) {
    WriteBatchInternal::Append(this, &source);
  }

  namespace {
    class MemTableInserter : public WriteBatch::Handler {
    public:
      SequenceNumber sequence_;
      MemTable* mem_;

      void Put(const Slice& key, const Slice& value) override {
        mem_->Add(sequence_, ValueType::kTypeValue, key, value);
        sequence_++;
      }
      void Delete(const Slice& key) override {
        mem_->Add(sequence_, ValueType::kTypeDeletion, key, Slice());
        sequence_++;
      }
    };
  }  // namespace

  Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
    MemTableInserter inserter;
    inserter.sequence_ = WriteBatchInternal::Sequence(b);
    inserter.mem_ = memtable;
    return b->Iterate(&inserter);
  }

  void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
    assert(contents.size() >= kHeader);
    b->rep_.assign(contents.data(), contents.size());
  }

  void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
    SetCount(dst, Count(dst) + Count(src));
    assert(src->rep_.size() >= kHeader);
    dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
  }
}