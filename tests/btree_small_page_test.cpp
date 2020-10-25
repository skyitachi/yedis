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
//    root->destroy();
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
  for (int i = 0; i <= limit; i++) {
    auto s = root->add(i, *big_value);
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(i, &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, "v" + std::to_string(i));
  }
}

TEST_F(BTreeSmallPageTest, Debug) {
  int64_t keys[] = {3, 5, 2, 4, 1};
  int lens[] = {60, 61, 13, 52, 44};
  int limit = 5;

  for (int i = 0; i < limit; i++) {
    std::string *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
}

TEST_F(BTreeSmallPageTest, RandomInsert) {
  std::unordered_map<int64_t, std::string*> presets_;
  int limit = 30;
  for (int i = 0; i <= limit; i++) {
    auto key = rnd->NextInt64();
    std::string* s = new std::string();
    test::RandomString(rnd, rnd->IntN(options.page_size / 2) + 1, s);
    presets_.insert(std::pair(key, s));
  }
  for (const auto it: presets_) {
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

