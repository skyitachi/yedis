//
// Created by skyitachi on 2020/9/13.
//

#ifndef YEDIS_INCLUDE_BTREE_META_PAGE_HPP_
#define YEDIS_INCLUDE_BTREE_META_PAGE_HPP_

#include "page.hpp"
#include "btree.hpp"

/**
 * Meta Page Format
 * page_id(4) + root_page_offset(4) + btree_levels(4)
 */
namespace yedis {

class BTreeMetaPage: public Page {
 public:
  // page_id
  inline page_id_t GetPageID() {
    return *reinterpret_cast<page_id_t*>(GetData());
  }
  // root_page_offset
  inline page_id_t GetRootPageId() {
    return DecodeFixed32(GetData() + sizeof(page_id_t));
  }
  // btree_levels, start from 1
  inline int32_t GetLevels() {
    return DecodeFixed32(GetData() + 2 * sizeof(page_id_t));
  }
};
}
#endif //YEDIS_INCLUDE_BTREE_META_PAGE_HPP_
