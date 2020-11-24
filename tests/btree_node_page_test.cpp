//
// Created by admin on 2020/10/13.
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
class BTreeNodePageTest : public testing::Test {
 protected:
  void SetUp() override {
    SPDLOG_INFO("gtest setup");
    disk_manager_ = new DiskManager("btree_node_page_test.idx");
    yedis_instance_ = new YedisInstance();
    yedis_instance_->disk_manager = disk_manager_;
    buffer_pool_manager_ = new BufferPoolManager(16, yedis_instance_);
    yedis_instance_->buffer_pool_manager = buffer_pool_manager_;
    root = new BTree(yedis_instance_);
  }
  BTree *root;
  Random* rnd = new Random();
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
    delete buffer_pool_manager_;
    delete disk_manager_;
    delete yedis_instance_;
  }

 private:
  BufferPoolManager *buffer_pool_manager_;
  DiskManager *disk_manager_;
  YedisInstance *yedis_instance_;
};


TEST_F(BTreeNodePageTest, NormalInsert) {
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

TEST_F(BTreeNodePageTest, SplitInsert) {
  int limit = 300;
  for (int i = 0; i <= limit; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(s.ok());
  }
  for (int i = 0; i < limit; i++) {
    std::string tmp;
    auto s = root->read(i, &tmp);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tmp, "v" + std::to_string(i));
  }
}

TEST_F(BTreeNodePageTest, RandomInsert) {
  std::unordered_map<int64_t, std::string*> presets_;
  int limit = 30;
  for (int i = 0; i <= limit; i++) {
    auto key = rnd->NextInt64();
    std::string* s = new std::string();
    test::RandomString(rnd, rnd->IntN(100) + 1, s);
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

TEST_F(BTreeNodePageTest, RandomBigInsert) {
  std::unordered_map<int64_t, std::string*> presets_;
  int limit = 300;
  for (int i = 0; i <= limit; i++) {
    auto key = rnd->NextInt64();
    std::string* s = new std::string();
    test::RandomString(rnd, rnd->IntN(100) + 1, s);
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

TEST_F(BTreeNodePageTest, LeafNodePrevAndNextTest) {
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
