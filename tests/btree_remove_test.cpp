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

  void Destroy() {
    root->destroy();
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

TEST_F(BTreeRemoveTest, IndexRemoveBorrowRightMost) {
  Open("btree_remove_borrow_right_most.idx", 128, 16);

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

  std::ofstream graphFile;
  graphFile.open("btree.dot");
  root->ToGraph(graphFile);
  graphFile.close();
  std::string tmp;
  {
    // first remove 3
    auto s = root->remove(3);
    ASSERT_TRUE(s.ok());
    s = root->read(3, &tmp);
    ASSERT_FALSE(s.ok());
  }

  std::ofstream deleted_file;
  deleted_file.open("btree_remove_borrow_right_most.dot");
  root->ToGraph(deleted_file);
  deleted_file.close();
  Flush();
}

TEST_F(BTreeRemoveTest, IndexRemoveBorrowParent) {
  Open("btree_remove_borrow_parent.idx", 128, 16);

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

  std::ofstream graphFile;
  graphFile.open("btree.dot");
  root->ToGraph(graphFile);
  graphFile.close();
  std::string tmp;
  {
    // first remove 2
    auto s = root->remove(2);
    ASSERT_TRUE(s.ok());
    s = root->read(2, &tmp);
    ASSERT_FALSE(s.ok());
  }

  std::ofstream deleted_file;
  deleted_file.open("btree_borrow_parent.dot");
  root->ToGraph(deleted_file);
  deleted_file.close();
  Flush();
}

TEST_F(BTreeRemoveTest, IndexRemoveRedistribute) {
  Open("btree_remove_redistribute.idx", 128, 16);

  int64_t keys[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
  int lens[] = {60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60};
  int limit = 9;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    auto *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    SPDLOG_INFO("inserted key {}, v_len= {}", keys[i], lens[i]);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }

  std::ofstream graphFile;
  graphFile.open("btree_redis.dot");
  root->ToGraph(graphFile);
  graphFile.close();
  std::string tmp;
//  {
//    // first remove 3
//    auto s = root->remove(3);
//    ASSERT_TRUE(s.ok());
//    s = root->read(3, &tmp);
//    ASSERT_FALSE(s.ok());
//  }
//  {
//    // first remove 2
//    auto s = root->remove(2);
//    ASSERT_TRUE(s.ok());
//    s = root->read(2, &tmp);
//    ASSERT_FALSE(s.ok());
//  }
  {
    // first remove 1
    auto s = root->remove(1);
    ASSERT_TRUE(s.ok());
    s = root->read(1, &tmp);
    ASSERT_FALSE(s.ok());
  }

  std::ofstream deleted_file;
  deleted_file.open("btree_remove_redis.dot");
  root->ToGraph(deleted_file);
  deleted_file.close();
  Flush();
}

TEST_F(BTreeRemoveTest, IndexRemoveRightMostBorrowParent) {
  Open("btree_index_remove_test.idx", 128, 16);

  int64_t keys[] ={1, 2, 3, 4, 5, 6, 7};
  int lens[] = {60, 60, 60, 60, 60, 60, 60};
  int limit = 7;
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
    auto s = root->remove(7);
    assert(s.ok());
  }

  {
    auto s = root->remove(6);
    assert(s.ok());
  }

  std::ofstream graphFile;
  graphFile.open("btree_right_most_borrow_parent.dot");
  root->ToGraph(graphFile);
  graphFile.close();
//  Flush();
}

TEST_F(BTreeRemoveTest, Redistribute2) {
  Open("btree_redistribute_2.idx", 128, 16);

  int64_t keys[] ={1, 2, 6, 7, 8, 9, 3, 4, 5};
  int lens[] = {60, 60, 60, 60, 60, 60, 60, 60, 60, 60};
  int limit = 9;
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
    std::ofstream graphFile;
    graphFile.open("btree_redistribute_2_inserted.dot");
    root->ToGraph(graphFile);
    graphFile.close();
  }

  {
    auto s = root->remove(9);
    assert(s.ok());
  }

  auto expected_root_id = 9;
  ASSERT_EQ(expected_root_id, root->GetRoot());
  auto root_page = root->get_page(expected_root_id);
  ASSERT_EQ(4, root_page->GetKey(0));

  auto left_child_page = root->get_page(3);
  ASSERT_TRUE(left_child_page->keys_equals({1, 2, 3}));

  auto right_child_page = root->get_page(8);
  ASSERT_TRUE(right_child_page->keys_equals({5 ,6, 7}));

  std::ofstream graphFile;
  graphFile.open("btree_after_redistribute_2_result.dot");
  root->ToGraph(graphFile);
  graphFile.close();


  {
    auto s = root->remove(8);
    assert(s.ok());
  }

//  Flush();
}

TEST_F(BTreeRemoveTest, IndexRemoveBranch3) {
  Open("btree_index_remove_branch_3.idx", 128, 16);

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
  {
    std::ofstream graphFile;
    graphFile.open("btree_index_remove_branch_3.dot");
    root->ToGraph(graphFile);
    graphFile.close();
  }
  {
    // case redis-5
    auto s = root->remove(6);
    ASSERT_TRUE(s.ok());

    // assertions
    auto root_page_id = 9;
    ASSERT_EQ(root_page_id, root->GetRoot());
    auto root_page = root->get_page(root_page_id);
    ASSERT_TRUE(root_page->keys_equals({3, 8}));
    ASSERT_TRUE(root_page->child_equals({3, 8, 13}));

    auto child_page_left_id = 8;
    auto child_page_left = root->get_page(child_page_left_id);
    ASSERT_TRUE(child_page_left->keys_equals({4, 6, 7}));
    ASSERT_TRUE(child_page_left->child_equals({5, 6, 10, 11}));

    auto child_right_page_id = 13;
    auto child_page_right = root->get_page(child_right_page_id);
    ASSERT_TRUE(child_page_right->keys_equals({9, 10}));
    ASSERT_TRUE(child_page_right->child_equals({12, 14, 15}));
  }

  std::ofstream graphFile;
  graphFile.open("btree_index_remove_branch_3_result.dot");
  root->ToGraph(graphFile);
  graphFile.close();

//  Flush();
}
TEST_F(BTreeRemoveTest, IndexRemoveBranch3_1) {
  Open("btree_index_remove_branch_3_1.idx", 128, 16);

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
  {
    std::ofstream graphFile;
    graphFile.open("btree_index_remove_branch_3.dot");
    root->ToGraph(graphFile);
    graphFile.close();
  }
  {
    // case redis-5
    auto s = root->remove(5);
    ASSERT_TRUE(s.ok());

    // assertions
    auto root_page_id = 9;
    ASSERT_EQ(root_page_id, root->GetRoot());
    auto root_page = root->get_page(root_page_id);
    ASSERT_TRUE(root_page->keys_equals({3, 8}));
    ASSERT_TRUE(root_page->child_equals({3, 8, 13}));

    auto child_page_left_id = 8;
    auto child_page_left = root->get_page(child_page_left_id);
    ASSERT_TRUE(child_page_left->keys_equals({4, 6, 7}));
    ASSERT_TRUE(child_page_left->child_equals({5, 7, 10, 11}));

    auto child_right_page_id = 13;
    auto child_page_right = root->get_page(child_right_page_id);
    ASSERT_TRUE(child_page_right->keys_equals({9, 10}));
    ASSERT_TRUE(child_page_right->child_equals({12, 14, 15}));
  }

  std::ofstream graphFile;
  graphFile.open("btree_index_remove_branch_3_5_result.dot");
  root->ToGraph(graphFile);
  graphFile.close();

//  Flush();
}

TEST_F(BTreeRemoveTest, IndexRemoveBranch3_2) {
  Open("btree_index_remove_branch_3_2.idx", 128, 16);

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
  {
    std::ofstream graphFile;
    graphFile.open("btree_index_remove_branch_3.dot");
    root->ToGraph(graphFile);
    graphFile.close();
  }
  {
    // case redis-5
    auto s = root->remove(4);
    ASSERT_TRUE(s.ok());

    // assertions
    auto root_page_id = 9;
    ASSERT_EQ(root_page_id, root->GetRoot());
    auto root_page = root->get_page(root_page_id);
    ASSERT_TRUE(root_page->keys_equals({3, 8}));
    ASSERT_TRUE(root_page->child_equals({3, 8, 13}));

    auto child_page_left_id = 8;
    auto child_page_left = root->get_page(child_page_left_id);
    ASSERT_TRUE(child_page_left->keys_equals({5, 6, 7}));
    ASSERT_TRUE(child_page_left->child_equals({6, 7, 10, 11}));

    auto child_right_page_id = 13;
    auto child_page_right = root->get_page(child_right_page_id);
    ASSERT_TRUE(child_page_right->keys_equals({9, 10}));
    ASSERT_TRUE(child_page_right->child_equals({12, 14, 15}));
  }

  std::ofstream graphFile;
  graphFile.open("btree_index_remove_branch_3_2_result.dot");
  root->ToGraph(graphFile);
  graphFile.close();

//  Flush();
}
TEST_F(BTreeRemoveTest, IndexRemoveBranch3_case_redis_3) {
  Open("btree_index_remove_branch_3_case_redis_3.idx", 128, 16);

  int64_t keys[] = {1, 2, 6, 7, 8, 9, 10, 11, 12, 13, 14, 3, 4, 5};
  int lens[] = {60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60};
  int limit = 14;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    auto *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  {
    std::ofstream graphFile;
    graphFile.open("btree_index_remove_branch_3_case_redis_3.dot");
    root->ToGraph(graphFile);
    graphFile.close();
  }
  {
    // case redis-3
    auto s = root->remove(7);
    ASSERT_TRUE(s.ok());

    // assertions
    auto root_page_id = 9;
    ASSERT_EQ(root_page_id, root->GetRoot());
    auto root_page = root->get_page(root_page_id);
    ASSERT_TRUE(root_page->keys_equals({4, 9}));
    ASSERT_TRUE(root_page->child_equals({3, 8, 13}));
//
    auto child_page_left_id = 3;
    auto child_page_left = root->get_page(child_page_left_id);
    ASSERT_TRUE(child_page_left->keys_equals({1, 2, 3}));
    ASSERT_TRUE(child_page_left->child_equals({1, 2, 16, 17}));

    auto child_right_page_id = 8;
    auto child_page_right = root->get_page(child_right_page_id);
    ASSERT_TRUE(child_page_right->keys_equals({5, 6, 8}));
    ASSERT_TRUE(child_page_right->child_equals({18, 4, 6, 7}));
  }

  std::ofstream graphFile;
  graphFile.open("btree_index_remove_branch_3_case_redis_3_result.dot");
  root->ToGraph(graphFile);
  graphFile.close();

//  Flush();
  Destroy();
}

TEST_F(BTreeRemoveTest, IndexRemoveBranch3_case_merge_2) {
  Open("btree_index_remove_branch_3_case_merge_1.idx", 128, 16);

  int64_t keys[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int lens[] = {60, 60, 60, 60, 60, 60, 60, 60, 60, 60};
  int limit = 10;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    auto *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  {
    auto s = root->remove(9);
    ASSERT_TRUE(s.ok());
  }
  {
    std::ofstream graphFile;
    graphFile.open("btree_index_remove_branch_3_case_merge_2.dot");
    root->ToGraph(graphFile);
    graphFile.close();
  }
  {
    // case merge-1
    auto s = root->remove(5);
    ASSERT_TRUE(s.ok());

    // assertions
    auto root_page_id = 9;
    ASSERT_EQ(root_page_id, root->GetRoot());
    auto root_page = root->get_page(root_page_id);
    ASSERT_TRUE(root_page->keys_equals({3}));
    ASSERT_TRUE(root_page->child_equals({3, 8}));
//
    auto child_page_left_id = 8;
    auto child_page_left = root->get_page(child_page_left_id);
    ASSERT_TRUE(child_page_left->keys_equals({4, 6, 7, 8}));
    ASSERT_TRUE(child_page_left->child_equals({5, 7, 10, 11, 14}));
  }

  std::ofstream graphFile;
  graphFile.open("btree_index_remove_branch_3_case_merge_2_result.dot");
  root->ToGraph(graphFile);
  graphFile.close();

//  Flush();
  Destroy();
}

TEST_F(BTreeRemoveTest, IndexRemoveUltimate) {
  Open("btree_index_remove_ultimate.idx", 128, 16);

  int64_t keys[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int lens[] = {60, 60, 60, 60, 60, 60, 60, 60, 60, 60};
  int limit = 10;
  std::vector<std::string*> values;

  for (int i = 0; i < limit; i++) {
    auto *v = new std::string();
    test::RandomString(rnd, lens[i], v);
    values.push_back(v);
    auto s = root->add(keys[i], *v);
    ASSERT_TRUE(s.ok());
  }
  {
    std::ofstream graphFile;
    graphFile.open("btree_index_remove_ultimate.dot");
    root->ToGraph(graphFile);
    graphFile.close();
  }

  for (int i = 0; i < limit - 1; i++) {
    auto s = root->remove(keys[i]);
    assert(s.ok());
    std::ofstream graphFile;
    char buf[50];
    sprintf(buf, "btree_index_remove_ultimate_%lld_result.dot", keys[i]);
    graphFile.open(buf);
    root->ToGraph(graphFile);
    graphFile.close();
  }

//  Flush();
  Destroy();
}

}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

