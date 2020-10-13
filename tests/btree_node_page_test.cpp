//
// Created by admin on 2020/10/13.
//

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <btree_node_page.h>
#include <buffer_pool_manager.hpp>
#include <disk_manager.hpp>
#include <btree.hpp>

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

  void Flush() {
    yedis_instance_->buffer_pool_manager->Flush();
  }

  void ShutDown() {
    yedis_instance_->disk_manager->ShutDown();
  }

  void TearDown() override {
    SPDLOG_INFO("gtest teardown");
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
}

TEST_F(BTreeNodePageTest, SplitInsert) {
  int limit = 300;
  for (int i = 0; i <= limit; i++) {
    auto s = root->add(i, "v" + std::to_string(i));
    ASSERT_TRUE(s.ok());
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
