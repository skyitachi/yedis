//
// Created by admin on 2020/9/17.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_PAGE_H_
#define YEDIS_INCLUDE_BTREE_NODE_PAGE_H_

#include "page.hpp"
#include "btree.hpp"

namespace yedis {
  class YedisInstance;
  class BTreeNodePage: public Page {
   public:
    // page_id
    inline page_id_t GetPageID() {
      return *reinterpret_cast<page_id_t*>(GetData());
    }
    inline void SetPageID(page_id_t page_id) {
      return EncodeFixed32(GetData(), page_id);
    }
    // n_current_entry_
    inline int GetCurrentEntries() { return *reinterpret_cast<int *>(GetData() + ENTRY_COUNT_OFFSET); }
    inline void SetCurrentEntries(uint32_t n) { EncodeFixed32(GetData() + ENTRY_COUNT_OFFSET, n); }
    // degree t
    inline int GetDegree() { return *reinterpret_cast<int *>(GetData() + DEGREE_OFFSET); }
    inline void SetDegree(uint32_t degree) { EncodeFixed32(GetData() + DEGREE_OFFSET, degree); }
    // is leaf node
    inline bool IsLeafNode() {
      return *reinterpret_cast<byte *>(GetData() + FLAG_OFFSET) == 0;
    }
    inline void SetLeafNode(bool is_leaf = true) {
      if (is_leaf) {
        *(GetData() + FLAG_OFFSET) = 0;
      } else {
        *(GetData() + FLAG_OFFSET) = 1;
      }
    }
    inline bool IsFull(size_t sz) {
      if (!IsLeafNode()) {
        return GetDegree() == MAX_DEGREE;
      }
      return available() >= sz;
    }
    // get parent id
    inline page_id_t GetParentPageID() {
      return *reinterpret_cast<page_id_t*>(GetData() + PARENT_OFFSET);
    }

    inline int64_t* KeyPosStart() {
      return reinterpret_cast<int64_t *>(GetData() + KEY_POS_OFFSET);
    }

    inline page_id_t * ChildPosStart() {
      return reinterpret_cast<page_id_t *>(GetData() + KEY_POS_OFFSET + (2 * GetDegree() - 1) * sizeof(page_id_t));
    }
    // interface
    virtual Status add(const byte *key, size_t k_len, const byte *value, size_t v_len, BTreeNodePage** root);
    virtual Status read(const byte *key, std::string *result);

    page_id_t search(int64_t key, const byte* value, size_t v_len);

    virtual void init(int degree, page_id_t page_id);
    void init(BTreeNodePage* dst, int degree, int n, page_id_t page_id, bool is_leaf);

    inline void leaf_init(BTreeNodePage* dst, int n, page_id_t page_id) {
      return init(dst, 0, n, page_id, true);
    }

    class EntryIterator {
     public:
      EntryIterator(char *ptr): data_(ptr) {}
      EntryIterator& operator = (const EntryIterator& iter) { data_ = iter.data_; }
      bool operator != (const EntryIterator& iter) const;
      EntryIterator& operator ++();
      EntryIterator& operator ++(int);
      int64_t key() const;
      char *data_;
    };
    BTreeNodePage* NewLeafPage(int cnt, const EntryIterator& start, const EntryIterator& end);
    BTreeNodePage* NewIndexPage(int cnt, int64_t key, page_id_t left,  page_id_t right);

   private:
    YedisInstance* yedis_instance_;
    size_t available() {
      return PAGE_SIZE - entry_tail_;
    }
    void index_split();
    BTreeNodePage* leaf_split(BTreeNodePage* parent, int child_idx);
    void index_node_add_child(int pos, int64_t key, page_id_t child);
    // entry tail
    size_t entry_tail_;
    // degree
    int t_;

  };
}

#endif //YEDIS_INCLUDE_BTREE_NODE_PAGE_H_
