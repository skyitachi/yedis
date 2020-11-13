//
// Created by skyitachi on 2020/10/22.
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
class BTreeSmallPageTest : public testing::Test {
 protected:
  void SetUp() override {
    SPDLOG_INFO("gtest setup");
    options.page_size = 128;
    disk_manager_ = new DiskManager("btree_node_page_test.idx", options);
    yedis_instance_ = new YedisInstance();
    yedis_instance_->disk_manager = disk_manager_;
    buffer_pool_manager_ = new BufferPoolManager(16, yedis_instance_, options);
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


TEST_F(BTreeSmallPageTest, NormalInsert) {
  for (int i = 0; i <= 10; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < 10; i++) {
    std::string tmp;
    auto s = root->read(i, &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, "v" + std::to_string(i));
  }
}

TEST_F(BTreeSmallPageTest, SplitInsert) {
  int limit = 10;
  std::string* big_value = new std::string();

  test::RandomString(rnd, rnd->IntN(options.page_size / 2) + 1, big_value);
  for (int i = 0; i < limit; i++) {
    SPDLOG_INFO("before inserted key {}", i);
    auto s = root->add(i, *big_value);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(i, &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *big_value);
  }
}

TEST_F(BTreeSmallPageTest, DebugFixedCase8) {
  int limit = 10;
  std::string* big_value = new std::string();

  test::RandomString(rnd, rnd->IntN(options.page_size / 2) + 1, big_value);
  for (int i = 0; i < limit; i++) {
    SPDLOG_INFO("before inserted key {}", i);
    auto s = root->add(i, *big_value);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(i, &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *big_value);
  }
}


TEST_F(BTreeSmallPageTest, DebugFixedCase1) {
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

TEST_F(BTreeSmallPageTest, DebugFixedCase2) {
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

TEST_F(BTreeSmallPageTest, DebugFixedCase3) {
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
TEST_F(BTreeSmallPageTest, DebugFixedCase4) {
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

TEST_F(BTreeSmallPageTest, DebugFixedCase5) {
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

TEST_F(BTreeSmallPageTest, DebugFixedCase6) {
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

TEST_F(BTreeSmallPageTest, DebugFixedCase7) {
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

TEST_F(BTreeSmallPageTest, DebugFixedCase9) {
  int64_t keys[] = {1, 2, 3};
  int lens[] = {10, 30, 60};
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


TEST_F(BTreeSmallPageTest, IndexNodeAddChildWithIndexSplit) {
  int64_t keys[] = {0, 1, 2, 3, 4, 6, 5};
  int lens[] = {70, 70, 70, 70, 10, 60, 80};
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

TEST_F(BTreeSmallPageTest, IndexNodeAddChildWithIndexSplit_Normal) {
  int64_t keys[] = {0, 1, 3, 4, 5, 6, 2};
  int lens[] = {70, 10, 60, 70, 70, 70, 70};
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

TEST_F(BTreeSmallPageTest, RandomInsert) {
  std::unordered_map<int64_t, std::string*> presets_;
  int limit = 15;
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
  }
  for (const auto it: presets_) {
    std::string tmp;
    auto s = root->read(it.first, &tmp);
    ASSERT_EQ(tmp, *it.second);
  }
}

TEST_F(BTreeSmallPageTest, RandomBigInsert) {
  std::unordered_map<int64_t, std::string*> presets_;
  int limit = 300;
  for (int i = 0; i <= limit; i++) {
    auto key = rnd->NextInt64();
    std::string* s = new std::string();
    test::RandomString(rnd, rnd->IntN(options.page_size / 2) + 1, s);
    if (presets_.find(key) == presets_.end()) {
      presets_.insert(std::pair(key, s));
    }
  }
  for (const auto it: presets_) {
    auto s = root->add(it.first, *it.second);
    ASSERT_TRUE(s.ok());
  }
  for (const auto it: presets_) {
    std::string tmp;
    auto s = root->read(it.first, &tmp);
    if (!s.ok()) {
      SPDLOG_INFO("expected key = {}, value = {}", it.first, *it.second);
    }
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, *it.second);
    counter++;
  }
}

// TODO: failed
TEST_F(BTreeSmallPageTest, LeafNodePrevAndNextTest) {
  auto limit = 1000;
  for (int i = 0; i < limit; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(s.ok());
  }
  auto all_child_by_pointers = root->GetAllLeavesByPointer();
  auto all_child_by_iterate = root->GetAllLeavesByIterate();
  ASSERT_TRUE(all_child_by_iterate.size() == all_child_by_pointers.size());
  for (int i = 0; i < all_child_by_pointers.size(); i++) {
    ASSERT_TRUE(all_child_by_pointers.at(i) == all_child_by_iterate.at(i));
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

