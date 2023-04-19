//
// Created by Shiping Yao on 2023/4/18.
//
#include <mutex>

#include <absl/container/flat_hash_map.h>

#include "cache.h"
#include "util.hpp"

namespace yedis {

Cache::~Cache() {}

namespace {

struct HashSlice {
  size_t operator() (const Slice& key) const {
    return Hash(key.data(), key.size(), 0);
  }
};

struct LRUHandle {
  void* value;
  void (*deleter) (const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;

  size_t charge;
  size_t key_length;
  bool in_cache;
  uint32_t refs;
  char key_data[1];

  Slice key() const {
    assert(next != this);
    return Slice(key_data, key_length);
  }
};

class LRUCache: public Cache {
public:
  explicit LRUCache(size_t capacity): capacity_(capacity), usage_(0), last_id_(0) {
    lru_.next = &lru_;
    lru_.prev = &lru_;
    in_use_.next = &in_use_;
    in_use_.prev = &in_use_;
  }
  LRUCache(): LRUCache(0) {};
  ~LRUCache() override;

  void SetCapacity(size_t capacity) {
    capacity_ = capacity;
  }

  Cache::Handle* Insert(const Slice& key, void *value, size_t charge,
                        void (*deleter) (const Slice &key, void *value)) override;

  Cache::Handle* Lookup(const Slice& key) override;

  void Release(Cache::Handle* handle) override;
  void Erase(const Slice& key) override;
  void Prune() override;

  uint64_t NewId() override {
    std::lock_guard<std::mutex> lg(id_mu_);
    return ++last_id_;
  }

  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle* >(handle)->value;
  }

  size_t TotalCharge() const override {
    std::lock_guard<std::mutex> lg(mutex_);
    return usage_;
  }

private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e);

  size_t capacity_;
  mutable std::mutex mutex_;
  mutable std::mutex id_mu_;
  uint64_t last_id_;

  size_t usage_;

  LRUHandle lru_;

  LRUHandle in_use_;

  absl::flat_hash_map<Slice, LRUHandle*, HashSlice> table_;
};

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle *e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle *e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle *list, LRUHandle *e) {
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key) {
  std::lock_guard<std::mutex> lock_guard(mutex_);
  auto iter = table_.find(key);
  LRUHandle* e = nullptr;
  if (iter != table_.end()) {
    e = iter->second;
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

Cache::Handle* LRUCache::Insert(const yedis::Slice &key, void *value, size_t charge,
                                void (*deleter)(const yedis::Slice &, void *)) {
  std::lock_guard<std::mutex> lock_guard(mutex_);
  // 尾随指针的用法
  auto *e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle
  e->key_length = key.size();
  std::memcpy(e->key_data, key.data(), key.size());
  Slice key_slice = Slice(e->key_data, e->key_length);

  if (capacity_ > 0) {
    e->refs++; // for the cache's reference
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    auto [iter, succ] = table_.emplace(key_slice, e);
    if (!succ) {
      auto* old = iter->second;
      table_.erase(key_slice);
      // don't do this
      // iter->second after call erase
      FinishErase(old);
      table_.emplace(key_slice, e);
    }
  } else {
    e->next = nullptr;
  }
  while (usage_ > capacity_ && lru_.next != &lru_) {
    auto old = lru_.next;
    assert(old->refs == 1);
    table_.erase(old->key());
    FinishErase(old);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

bool LRUCache::FinishErase(LRUHandle *e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Release(Cache::Handle *handle) {
  std::lock_guard<std::mutex> lg(mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

void LRUCache::Erase(const yedis::Slice &key) {
  std::lock_guard<std::mutex> lg(mutex_);
  auto it = table_.find(key);
  if (it != table_.end()) {
    auto e = it->second;
    table_.erase(it);
    FinishErase(e);
  }
}

void LRUCache::Prune() {
  std::lock_guard<std::mutex> lg(mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1); // only lru cache refs
    table_.erase(e->key());
    FinishErase(e);
  }
}
}

Cache* NewLRUCache(size_t capacity) { return new LRUCache(capacity); }
}
