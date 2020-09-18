//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_BTREE_LEAF_NODE_PAGE_HPP_
#define YEDIS_INCLUDE_BTREE_LEAF_NODE_PAGE_HPP_

#include <unordered_map>

#include "btree.hpp"
#include "page.hpp"
#include "btree_node_page.h"


namespace yedis {
/**
 * header + slots + entries
 * header:
 * 4 byte(page_id) + 4 byte(entries count) + 4 byte(degree) + 1 byte(flag)
 */
class BTreeLeafNodePage: public BTreeNodePage {

 public:
  // key pos start
  inline byte* KeyPosStart() {
    return reinterpret_cast<byte*>(GetData() + KEY_POS_OFFSET);
  }
  // child pos start
  inline int64_t* ChildPosStart() {
    return reinterpret_cast<int64_t*>(GetData() + KEY_POS_OFFSET + (2 * GetDegree() - 1) * sizeof(int64_t));
  }
  // entries pointer
  inline byte* EntryPosStart() {
    auto offset = KEY_POS_OFFSET + (4 * t_ - 1) * sizeof(int64_t);
    auto entryStart = reinterpret_cast<byte *>(GetData() + offset);
    return reinterpret_cast<byte*>(entryStart);
  }
  //
  Status add(const byte *key, size_t k_len, const byte *value, size_t v_len) override;
  Status read(const byte *key, std::string *result) override;

  void init(int degree, page_id_t page_id);


 private:
  void writeHeader();
  size_t available();
  int upper_bound(const byte *key);
  int t_;
  int cur_entries_;
  // entry buffer
  std::string buf_;
  std::vector<Entry> entries_;
  // entry tail
  size_t entry_tail_;

  int64_t* key_pos_ptr_;
  // TODO: value page cnt
  int value_page_cnt_;
  // TODO: value page should be allocated by buffer_pool_manger
  // innodb how to design primary key data format

};
}
#endif //YEDIS_INCLUDE_BTREE_LEAF_NODE_PAGE_HPP_
