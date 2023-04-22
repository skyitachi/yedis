//
// Created by Shiping Yao on 2023/4/6.
//

#ifndef YEDIS_MEMTABLE_H
#define YEDIS_MEMTABLE_H

#include <folly/ConcurrentSkipList.h>
#include <folly/memory/Malloc.h>

#include "common/status.h"
#include "db_format.h"
#include "allocator.h"
#include "iterator.h"

namespace yedis {

class InternalKeyComparator;
class MemTableIterator;

class MemTable {
  public:
    MemTable();
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Add(SequenceNumber seq, ValueType type, const Slice& key,
             const Slice& value);
    bool Get(const LookupKey& key, std::string* value, Status* s);

    Slice EncodeEntry(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);

    Iterator* NewIterator();
    size_t ApproximateMemoryUsage() { return alloc_.size(); }

    void Ref() {
      refs_++;
    }

    void Unref() {
      refs_--;
      if (refs_ == 0) {
        delete this;
      }
    }

  struct KeyComparator {
    bool operator()(const Slice& a, const Slice& b) const;
  };

  private:
    friend class MemTableIterator;
    static constexpr int kMaxHeight = 12;

    using SkipListType = folly::ConcurrentSkipList<Slice, KeyComparator>;
    using SkipList = SkipListType::Accessor;

    std::unique_ptr<SkipListType> table_;

    Allocator alloc_;

    int refs_;


  };

}

#endif //YEDIS_MEMTABLE_H
