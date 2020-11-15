//
// Created by skyitachi on 2020/10/30.
//
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <btree_node_page.h>
#include <buffer_pool_manager.hpp>
#include <disk_manager.hpp>
#include <btree.hpp>
#include <random.h>
#include <test_util.h>

namespace yedis {
class BTreeSmallPoolTest : public testing::Test {
 protected:
  void SetUp() override {
    options.page_size = 128;
    disk_manager_ = new DiskManager("btree_small_pool_debug.idx", options);
    yedis_instance_ = new YedisInstance();
    yedis_instance_->disk_manager = disk_manager_;
    // 太小的buffer pool也不够, 当前最小的buffer pool size是7个
    // meta page, root, parent, current, new_leaf_node for the split page, new leaf node for the key
    buffer_pool_manager_ = new BufferPoolManager(7, yedis_instance_, options);
    yedis_instance_->buffer_pool_manager = buffer_pool_manager_;
    root = new BTree(yedis_instance_);
  }
  BTree *root;
  Random* rnd = new Random();
  BTreeOptions options;
  int counter = 0;

  void Flush() {
    yedis_instance_->buffer_pool_manager->Flush();
  }

  void ShutDown() {
    yedis_instance_->disk_manager->ShutDown();
  }

  void TearDown() override {
    SPDLOG_INFO("gtest teardown: success {}", counter);
    Flush();
    ShutDown();
    // root->destroy();
    delete root;
    delete buffer_pool_manager_;
    delete disk_manager_;
    delete yedis_instance_;
  }

 private:
  BufferPoolManager *buffer_pool_manager_;
  DiskManager *disk_manager_;
  YedisInstance *yedis_instance_;
};

TEST_F(BTreeSmallPoolTest, DebugFixedCase1) {
  int64_t keys[] = {3, 5, 1, 4, 2};
  int lens[] = {5, 61, 13, 52, 11};
  int limit = 5;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase2) {
  int64_t keys[] = {3, 5, 1, 6, 2};
  int lens[] = {5, 61, 13, 52, 11};
  int limit = 5;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase3) {
  int64_t keys[] = {2, 1, 3, 5, 4};
  int lens[] = {7, 59, 36, 5, 59};
  int limit = 5;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

// leaf_split with no nodes to new leaf page
TEST_F(BTreeSmallPoolTest, DebugFixedCase4) {
  int64_t keys[] = {2, 1, 3, 5, 4};
  int lens[] = {7, 8, 10, 82, 30};
  int limit = 5;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase5) {
  int64_t keys[] = {3, 2, 1};
  int lens[] = {53, 18, 70};
  int limit = 3;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase6) {
  int64_t keys[] = {2, 6, 3, 4};
  int lens[] = {53, 41, 3, 30};
  int limit = 4;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase7) {
  int64_t keys[] = {2, 6, 3, 4};
  int lens[] = {53, 41, 3, 30};
  int limit = 4;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase8) {
  int64_t keys[] = {1, 8, 6, 2, 4, 11, 10, 18, 7, 9};
  int lens[] = {39, 64, 35, 37, 60, 12, 70, 90, 40, 60};
  int limit = 6;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase9) {
  int64_t keys[] = {1, 8, 6, 2, 4, 11, 10, 18, 7, 9};
  int lens[] = {39, 64, 35, 37, 60, 12, 70, 90, 40, 60};
  int limit = 7;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase10) {
  int64_t keys[] = {5, 4, 9, 12, 1, 11, 8, 6, 7, 3, 10, 2};
  int lens[] = {36, 43, 60, 48, 19, 37, 18, 34, 53, 22, 56, 63};
  int limit = 12;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}

TEST_F(BTreeSmallPoolTest, DebugFixedCase11) {
  int64_t keys[] = {1, 3, 2};
  int lens[] = {16, 25, 25};
  int limit = 3;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(keys[i], &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *values[i]);
  }
}


TEST_F(BTreeSmallPoolTest, RandomInsert) {
  std::unordered_map<int64_t, std::string*> presets_;
  int limit = 30;
  for (int i = 0; i <= limit; i++) {
    auto key = rnd->NextInt64();
    std::string* s = new std::string();
    test::RandomString(rnd, rnd->IntN(options.page_size / 2) + 1, s);
    presets_.insert(std::pair(key, s));
  }
  int count = 0;
  for (const auto it: presets_) {
    SPDLOG_INFO("inserted key {}, v_len= {}, count = {}", it.first, it.second->size(), count++);
    auto s = root->add(it.first, *it.second);
    ASSERT_TRUE(s.ok());
    std::string tmp;
    s = root->read(it.first, &tmp);
    ASSERT_EQ(tmp, *it.second);
  }
}
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

