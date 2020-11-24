//
// Created by skyitachi on 2020/11/21.
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

class BTreeRemoveTest : public testing::Test {
 protected:
  void SetUp() override {
  }
  BTree *root;
  Random *rnd = new Random();
  int counter = 0;

  void Flush() {
    yedis_instance_->buffer_pool_manager->Flush();
  }

  void Open(const char* index_file_name, size_t page_size, size_t pool_size) {
    BTreeOptions options;
    options.page_size = page_size;
    disk_manager_ = new DiskManager(index_file_name, options);
    yedis_instance_ = new YedisInstance();
    yedis_instance_->disk_manager = disk_manager_;
    buffer_pool_manager_ = new BufferPoolManager(pool_size, yedis_instance_, options);
    yedis_instance_->buffer_pool_manager = buffer_pool_manager_;
    root = new BTree(yedis_instance_);
  }

  void Close() {
    Flush();
    ShutDown();
    delete root;
    delete buffer_pool_manager_;
    delete disk_manager_;
    delete yedis_instance_;
  }

  void ShutDown() {
    yedis_instance_->disk_manager->ShutDown();
  }

  void TearDown() override {
  }

 private:
  BufferPoolManager *buffer_pool_manager_;
  DiskManager *disk_manager_;
  YedisInstance *yedis_instance_;
};

TEST_F(BTreeRemoveTest, LeafNodeRemoveTest) {
  Open("btree_leaf_remove_test.idx", 4096, 16);
  for (int i = 0; i <= 100; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(s.ok());
  }

  {
    auto s = root->remove(1);
    ASSERT_TRUE(s.ok());
    std::string tmp;
    s = root->read(1, &tmp);
    ASSERT_TRUE(!s.ok());
  }

  {
    auto s = root->remove(101);
    ASSERT_TRUE(!s.ok());
  }

}

TEST_F(BTreeRemoveTest, IndexRemoveSimpleTest) {
  Open("btree_index_remove_test.idx", 128, 16);

  int64_t keys[] ={1, 2, 3, 4, 5, 6};
  int lens[] = {60, 60, 60, 60, 60, 60};
  int limit = 6;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    auto *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }

  {
    auto s = root->remove(1);
    ASSERT_TRUE(s.ok());
    std::string tmp;
    s = root->read(1, &tmp);
    ASSERT_TRUE(!s.ok());
    s = root->remove(4);
    ASSERT_TRUE(s.ok());
    s = root->read(4, &tmp);
    ASSERT_TRUE(!s.ok());
    s = root->remove(2);
    ASSERT_TRUE(s.ok());
    s = root->read(2, &tmp);
    ASSERT_TRUE(!s.ok());
    s = root->remove(3);
    ASSERT_TRUE(s.ok());
    s = root->read(3, &tmp);
    ASSERT_TRUE(!s.ok());

    s = root->remove(6);
    ASSERT_TRUE(s.ok());
    s = root->read(6, &tmp);
    ASSERT_TRUE(!s.ok());
    s = root->read(5, &tmp);
    ASSERT_TRUE(s.ok());
  }


  Flush();
}

TEST_F(BTreeRemoveTest, IndexRemoveRedistribute) {
  Open("btree_index_remove_redistribute_test.idx", 128, 16);

  int64_t keys[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
  int lens[] = {60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60};
  int limit = 11;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    auto *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  std::string tmp;
  {
    // first remove 3
    auto s = root->remove(3);
    ASSERT_TRUE(s.ok());
    s = root->read(3, &tmp);
    ASSERT_FALSE(s.ok());
  }

  Flush();
}

}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

