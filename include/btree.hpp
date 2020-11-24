//
// Created by skyitachi on 2020/8/20.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_HPP_
#define YEDIS_INCLUDE_BTREE_NODE_HPP_
#include <string>
#include "yedis.hpp"
#include "config.hpp"
#include "yedis_zset.hpp"
#include "option.hpp"

namespace yedis {

typedef uint8_t byte;
struct Entry {
  // delete or normal
  byte flag;
  uint32_t key_len;
  byte* key;
  uint32_t value_len;
  byte* value;
  inline size_t size() { return 9 + key_len + value_len; }
};

class BTreeLeafNodePage;
class BTreeMetaPage;
class BTreeNodePage;
class YedisInstance;
class BTree {
 public:
  BTree(YedisInstance* yedis_instance, BTreeOptions options): yedis_instance_(yedis_instance), options_(options) {
    init();
  }
  BTree(YedisInstance* yedis_instance): BTree(yedis_instance, BTreeOptions{}) {}
  Status init();
  Status add(int64_t key, const Slice &value);
  Status read(int64_t key, std::string *value);
  Status remove(int64_t key);
  Status destroy();
  page_id_t GetFirstLeafPage();
  std::vector<page_id_t> GetAllLeavesByPointer();
  std::vector<page_id_t> GetAllLeavesByIterate();
 private:
  inline BTreeNodePage* get_page(page_id_t page_id);
  BTreeMetaPage* meta_;
  BTreeLeafNodePage * leaf_root_;
  BTreeNodePage *root_;
  std::string file_name_;
  YedisInstance* yedis_instance_;
  BTreeOptions options_;
};
}
#endif //YEDIS_INCLUDE_BTREE_NODE_HPP_
