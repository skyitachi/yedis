//
// Created by Shiping Yao on 2023/4/18.
//

#ifndef YEDIS_CACHE_H
#define YEDIS_CACHE_H

#include <stddef.h>

#include "slice.h"

namespace yedis {

class Cache {
public:
  Cache() = default;
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  virtual ~Cache();

  struct Handle{};

  virtual Handle* Insert(const Slice& key, void *value, size_t charge,
                         void (*deleter) (const Slice& key, void *value)) = 0;

  virtual Handle* Lookup(const Slice& key) = 0;

  virtual void Release(Handle* handle) = 0;

  virtual void* Value(Handle* handle) = 0;

  virtual void Erase(const Slice& key) = 0;

  virtual uint64_t NewId() = 0;

  virtual void Prune() {};

  virtual size_t TotalCharge() const = 0;
};

Cache* NewLRUCache(size_t capacity);
}
#endif //YEDIS_CACHE_H
