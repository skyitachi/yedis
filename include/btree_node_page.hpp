//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_PAGE_HPP_
#define YEDIS_INCLUDE_BTREE_NODE_PAGE_HPP_

#include "btree.hpp"
#include "page.hpp"

namespace yedis {
class BTreeNodePage: public Page {
 public:

 private:
  // TODO: 不需要这些字段，通过解释data即可实现
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
  //
};
}
#endif //YEDIS_INCLUDE_BTREE_NODE_PAGE_HPP_
