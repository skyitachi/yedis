//
// Created by admin on 2020/9/17.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_PAGE_H_
#define YEDIS_INCLUDE_BTREE_NODE_PAGE_H_

#include "page.hpp"
#include "btree.hpp"

namespace yedis {
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
    // get parent id
    inline page_id_t GetParentPageID() {
      return *reinterpret_cast<page_id_t*>(GetData() + PARENT_OFFSET);
    }
    // interface
    virtual Status add(const byte *key, size_t k_len, const byte *value, size_t v_len);
    virtual Status read(const byte *key, std::string *result);

    virtual void init(int degree, page_id_t page_id);
   private:

  };
}

#endif //YEDIS_INCLUDE_BTREE_NODE_PAGE_H_
