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
  inline int64_t* KeyPosStart() {
    return reinterpret_cast<int64_t*>(GetData() + KEY_POS_OFFSET);
  }
  // child pos start
  inline int64_t* ChildPosStart() {
    return reinterpret_cast<int64_t*>(GetData() + KEY_POS_OFFSET + (2 * GetDegree() - 1) * sizeof(int64_t));
  }
  // entries pointer
  inline Entry* EntryPosStart() {
    auto offset = KEY_POS_OFFSET + (4 * t_ - 1) * sizeof(int64_t);
    auto entryStart = reinterpret_cast<char *>(GetData() + offset);
    return reinterpret_cast<Entry*>(entryStart);
  }
  //
  Status add(const byte *key, size_t k_len, const byte *value, size_t v_len);

  void init(int degree, page_id_t page_id);

 private:
  void writeHeader();
  size_t available();
  int t_;
  int cur_entries_;
  int upper_bound(const byte *key);
  // TODO: value page cnt
  int value_page_cnt_;
  // TODO: value page should be allocated by buffer_pool_manger
  // innodb how to design primary key data format

};
}
#endif //YEDIS_INCLUDE_BTREE_NODE_PAGE_HPP_
