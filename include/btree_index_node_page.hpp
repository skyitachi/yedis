//
// Created by skyitachi on 2020/9/13.
//

#ifndef YEDIS_INCLUDE_BTREE_INDEX_NODE_PAGE_HPP_
#define YEDIS_INCLUDE_BTREE_INDEX_NODE_PAGE_HPP_

#include "btree.hpp"
#include "btree_node_page.h"
#include <tuple>

namespace yedis {

class BTreeIndexNodePage: public BTreeNodePage {
 public:
  // key pos start
  inline int64_t *KeyPosStart() {
    return reinterpret_cast<int64_t *>(GetData() + KEY_POS_OFFSET);
  }
  // child pos start
  inline byte* ChildPosStart() {
    return reinterpret_cast<byte*>(GetData() + KEY_POS_OFFSET + (2 * GetDegree() - 1) * sizeof(int64_t));
  }

  page_id_t search(const byte* key, size_t k_len);
  std::tuple<page_id_t, page_id_t> search(int64_t key);

  // 初始化keys数组
  // 或者说如何让底层数据直接以iterator的形式访问
  void init(int degree, page_id_t page_id) override;

 private:
  typedef std::tuple<byte*, size_t> Byte;
  std::vector<Byte> keys_;
};
}
#endif //YEDIS_INCLUDE_BTREE_INDEX_NODE_PAGE_HPP_
