//
// Created by Shiping Yao on 2023/4/18.
//

#include <vector>

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "cache.h"
#include "util.hpp"
#include "absl/container/flat_hash_map.h"

namespace yedis {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { return reinterpret_cast<uintptr_t>(v); }

class CacheTest : public testing::Test {
  public:
    static void Deleter(const Slice& key, void* v) {
      current_->deleted_keys_.push_back(DecodeKey(key));
      current_->deleted_values_.push_back(DecodeValue(v));
    }

    static constexpr int kCacheSize = 1000;
    std::vector<int> deleted_keys_;
    std::vector<int> deleted_values_;
    Cache* cache_;

    CacheTest() : cache_(NewLRUCache(kCacheSize)) { current_ = this; }

    ~CacheTest() { delete cache_; }

    int Lookup(int key) {
      Cache::Handle* handle = cache_->Lookup(EncodeKey(key));
      const int r = (handle == nullptr) ? -1 : DecodeValue(cache_->Value(handle));
      if (handle != nullptr) {
        cache_->Release(handle);
      }
      return r;
    }

    void Insert(int key, int value, int charge = 1) {
      cache_->Release(cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                                     &CacheTest::Deleter));
    }

    Cache::Handle* InsertAndReturnHandle(int key, int value, int charge = 1) {
      return cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                            &CacheTest::Deleter);
    }

    void Erase(int key) { cache_->Erase(EncodeKey(key)); }
    static CacheTest* current_;
  };

CacheTest* CacheTest::current_;

TEST_F(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);
}

TEST(FlatHashMapTest, Basic) {
  std::unordered_map<int, int> f_map;
  auto [it, succ] = f_map.emplace(1, 1);
  ASSERT_TRUE(succ);
  auto res = f_map.emplace(1, 2);
  ASSERT_FALSE(res.second);
  ASSERT_EQ(res.first->first, 1);
  ASSERT_EQ(res.first->second, 1);
}

}


int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}


