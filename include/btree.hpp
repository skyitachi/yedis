//
// Created by skyitachi on 2020/8/20.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_HPP_
#define YEDIS_INCLUDE_BTREE_NODE_HPP_
#include <string>
#include "yedis.hpp"
#include "config.hpp"
#include "yedis_zset.hpp"

namespace yedis {

typedef uint8_t byte;
struct Entry {
  // delete or normal
  byte flag;
  uint32_t key_len;
  byte* key;
  uint32_t value_len;
  byte* value;
};

// 继承page
class BTreeNode {
  public:
    BTreeNode(): t_(MAX_DEGREE){
      n_current_entry_ = 0;
      flag_ = 0;
    }
    BTreeNode(int t): t_(t){}
    Status add(byte* key, byte* value);
  private:
    // entries
    int n_current_entry_;
    // degree
    int t_;
    // leaf node flag
    byte flag_;
    // key pos
    int64_t *keyPos_;
    // child pos
    int64_t *childPos_;
    // node offset in file;
    int64_t offset_;
    Entry* entries_;
    // Page
};

class BTreeNodePage;
class YedisInstance;
class BTree {
 public:
  BTree(YedisInstance* yedis_instance): yedis_instance_(yedis_instance) {};
  Status init();
  Status add(const Slice &key, const Slice &value);
  Status destroy();
 private:
  BTreeNodePage * root;
  std::string file_name_;
  YedisInstance* yedis_instance_;
};
}
#endif //YEDIS_INCLUDE_BTREE_NODE_HPP_
