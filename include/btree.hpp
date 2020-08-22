//
// Created by skyitachi on 2020/8/20.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_HPP_
#define YEDIS_INCLUDE_BTREE_NODE_HPP_
#include <string>
#include "yedis.hpp"

namespace yedis {
#define PAGE_BLOCK 4096
#define MAX_DEGREE (PAGE_BLOCK / 33)

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

class BTree {
 public:
   Status add(byte* key, byte* value);
 private:
   BTreeNode* root;
};
}
#endif //YEDIS_INCLUDE_BTREE_NODE_HPP_
