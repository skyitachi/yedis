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
    disk_manager_ = new DiskManager("btree_node_page_test.idx", options);
    yedis_instance_ = new YedisInstance();
    yedis_instance_->disk_manager = disk_manager_;
    // 太小的buffer pool也不够
    buffer_pool_manager_ = new BufferPoolManager(5, yedis_instance_, options);
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
     root->destroy();
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
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
