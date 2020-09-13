//
// Created by skyitachi on 2020/9/13.
//

#ifndef YEDIS_INCLUDE_BTREE_INDEX_NODE_PAGE_HPP_
#define YEDIS_INCLUDE_BTREE_INDEX_NODE_PAGE_HPP_

#include "btree.hpp"
#include "page.hpp"

namespace yedis {

class BTreeIndexNodePage: public Page {
 public:
  // page_id
  inline page_id_t GetPageID() {
    return *reinterpret_cast<page_id_t*>(GetData());
  }
  // n_current_entry_
  inline int GetCurrentEntries() { return *reinterpret_cast<int *>(GetData() + ENTRY_COUNT_OFFSET); }
  // degree t
  inline int GetDegree() { return *reinterpret_cast<int *>(GetData() + DEGREE_OFFSET); }
  // is leaf node
  inline bool IsLeafNode() {
    return *reinterpret_cast<byte *>(GetData() + FLAG_OFFSET) == 0;
  }
  // key pos start
  inline byte* KeyPosStart() {
    return reinterpret_cast<byte*>(GetData() + KEY_POS_OFFSET);
  }
  // child pos start
  inline byte* ChildPosStart() {
    return reinterpret_cast<byte*>(GetData() + KEY_POS_OFFSET + (2 * GetDegree() - 1) * sizeof(int64_t));
  }

 private:

};
}
#endif //YEDIS_INCLUDE_BTREE_INDEX_NODE_PAGE_HPP_
