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

class BTreeReopenTest : public testing::Test {
 protected:
  void SetUp() override {
  }
  BTree *root;
  Random *rnd = new Random();
  int counter = 0;

  void Flush() {
    yedis_instance_->buffer_pool_manager->Flush();
  }

  void Open() {
    BTreeOptions options;
    options.page_size = 128;
    disk_manager_ = new DiskManager("btree_reopen_test.idx", options);
    yedis_instance_ = new YedisInstance();
    yedis_instance_->disk_manager = disk_manager_;
    buffer_pool_manager_ = new BufferPoolManager(16, yedis_instance_, options);
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

TEST_F(BTreeReopenTest, NoramlTest) {
  Open();
  for (int i = 0; i <= 100; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(s.ok());
  }
  Close();

  SPDLOG_INFO("insert already done");
  Open();
  for (int i = 0; i <= 100; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(!s.ok());
  }

  for (int i = 101; i < 200; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(s.ok());
  }
  Close();
}

}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

