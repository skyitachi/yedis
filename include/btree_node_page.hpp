//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_PAGE_HPP_
#define YEDIS_INCLUDE_BTREE_NODE_PAGE_HPP_

#include "btree.hpp"
#include "page.hpp"

namespace yedis {
/**
 * header + slots + entries
 * header:
 * 4 byte(page_id) + 4 byte(entries count) + 4 byte(degree) + 1 byte(flag)
 */
class BTreeNodePage: public Page {
  static constexpr int ENTRY_COUNT_OFFSET = 4;
  static constexpr int DEGREE_OFFSET = 8;
  static constexpr int FLAG_OFFSET = 12;
  static constexpr int KEY_POS_OFFSET = 13;

 public:
  // n_current_entry_
  inline int GetCurrentEntries() { return *reinterpret_cast<int *>(GetData() + ENTRY_COUNT_OFFSET); }
  // degree t
  inline int GetDegree() { return *reinterpret_cast<int *>(GetData() + DEGREE_OFFSET); }
  // is leaf node
  inline bool IsLeafNode() {
    return *reinterpret_cast<byte *>(GetData() + FLAG_OFFSET) == 1;
  }
  // key pos start
  inline int64_t* KeyPosStart() {
    return reinterpret_cast<int64_t*>(GetData() + KEY_POS_OFFSET);
  }
  // child pos start
  inline int64_t* ChildPosStart() {
    return reinterpret_cast<int64_t*>(GetData() + KEY_POS_OFFSET + (2 * GetDegree() - 1) * sizeof(int64_t));
  }
};
}
#endif //YEDIS_INCLUDE_BTREE_NODE_PAGE_HPP_
