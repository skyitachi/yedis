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

namespace yedis {

class InternalKeyComparator;

class MemTable {
  public:
    MemTable();
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Add(SequenceNumber seq, ValueType type, const Slice& key,
             const Slice& value);
    bool Get(const LookupKey& key, std::string* value, Status* s);

    Slice EncodeEntry(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);

  struct KeyComparator {
    bool operator()(const Slice& a, const Slice& b) const;
  };

//  template <class NodeType>
//  struct MyNodeAlloc : public folly::Mallocator<NodeType> {
//    static NodeType* allocate(size_t n) {
//      return static_cast<NodeType*>((n * sizeof(NodeType))); // 使用 JEMalloc 分配器分配内存
//    }
//
//    static void deallocate(NodeType* p, size_t n) {
//      je_free(p); // 使用 JEMalloc 分配器释放内存
//    }
//  };
//
  private:
    static constexpr int kMaxHeight = 12;

    using SkipListType = folly::ConcurrentSkipList<Slice, KeyComparator>;
    using SkipList = SkipListType::Accessor;

    std::unique_ptr<SkipListType> table_;

    Allocator alloc_;


  };

}

#endif //YEDIS_MEMTABLE_H
