//
// Created by admin on 2020/9/17.
//

#ifndef YEDIS_INCLUDE_BTREE_NODE_PAGE_H_
#define YEDIS_INCLUDE_BTREE_NODE_PAGE_H_

#include <spdlog/spdlog.h>

#include "page.hpp"
#include "btree.hpp"

namespace yedis {
  class BufferPoolManager;
  class BTreeNodePage: public Page {
   public:
    // page_id
    inline page_id_t GetPageID() {
      return *reinterpret_cast<page_id_t*>(GetData());
    }
    // n_current_entry_
    inline int GetCurrentEntries() { return *reinterpret_cast<int *>(GetData() + ENTRY_COUNT_OFFSET); }
    inline void SetCurrentEntries(uint32_t n) { EncodeFixed32(GetData() + ENTRY_COUNT_OFFSET, n); }
    // degree t
    inline int GetDegree() { return *reinterpret_cast<int *>(GetData() + DEGREE_OFFSET); }
    inline void SetDegree(uint32_t degree) { EncodeFixed32(GetData() + DEGREE_OFFSET, degree); }
    // set available
    inline uint32_t GetAvailable() {
      auto av = *reinterpret_cast<uint32_t*>(GetData() + AVAILABLE_OFFSET);
      assert(av <= options_.page_size);
      return av;
    }
    inline void SetAvailable(uint32_t available) {
      assert(available <= options_.page_size);
      EncodeFixed32(GetData() + AVAILABLE_OFFSET, available);
    }
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
    inline bool IsFull(size_t sz = 0) {
      if (!IsLeafNode()) {
        return GetCurrentEntries() >= GetDegree();
      }
      return GetAvailable() < sz;
    }
    // get parent id
    inline page_id_t GetParentPageID() {
      return *reinterpret_cast<page_id_t*>(GetData() + PARENT_OFFSET);
    }
    // set parent id
    inline void SetParentPageID(page_id_t parent) {
      SPDLOG_INFO("current page {} set to to parent {}", GetPageID(), parent);
      EncodeFixed32(GetData() + PARENT_OFFSET, parent);
    }

    inline int64_t* KeyPosStart() {
      return reinterpret_cast<int64_t *>(GetData() + KEY_POS_OFFSET);
    }
    // index node
    inline int64_t GetKey(int idx) {
      assert(idx < GetCurrentEntries());
      return KeyPosStart()[idx];
    }

    inline page_id_t * ChildPosStart() {
      return reinterpret_cast<page_id_t *>(GetData() + KEY_POS_OFFSET + (2 * GetDegree() - 1) * sizeof(int64_t));
    }

    inline page_id_t GetChild(int idx) {
      // NOTE: len(children) = len(entries) + 1
      assert(idx <= GetCurrentEntries());
      return ChildPosStart()[idx];
    }

    // get and set prev page_id
    inline page_id_t GetPrevPageID() {
      return *reinterpret_cast<page_id_t*>(GetData() + PREV_NODE_PAGE_ID_OFFSET);
    }
    inline void SetPrevPageID(page_id_t page_id) {
      EncodeFixed32(GetData() + PREV_NODE_PAGE_ID_OFFSET, page_id);
    }

    inline page_id_t GetNextPageID() {
      return *reinterpret_cast<page_id_t*>(GetData() + NEXT_NODE_PAGE_ID_OFFSET);
    }

    inline void SetNextPageID(page_id_t page_id) {
      SPDLOG_INFO("current page_id {}, next_page_id {}", GetPageID(), page_id);
      assert(page_id != 0);
      EncodeFixed32(GetData() + NEXT_NODE_PAGE_ID_OFFSET, page_id);
    }

    // entries pointer
    inline byte* EntryPosStart() {
      auto entryStart = reinterpret_cast<byte *>(GetData() + ENTRY_OFFSET);
      return reinterpret_cast<byte*>(entryStart);
    }
    // only leaf node needs
    inline size_t GetEntryTail() {
      return options_.page_size - GetAvailable() - LEAF_HEADER_SIZE;
    }
    
    inline uint32_t MaxAvailable() {
      return options_.page_size - LEAF_HEADER_SIZE;
    }

    inline int lower_bound_index(int64_t key) {
      assert(!IsLeafNode());
      auto key_start = KeyPosStart();
      auto entries = GetCurrentEntries();
      auto it = std::lower_bound(key_start, key_start + entries, key);
      return it - key_start;
    }

    // interface
    Status add(const byte *key, size_t k_len, const byte *value, size_t v_len, BTreeNodePage** root);
    Status add(BufferPoolManager* buffer_pool_manager, int64_t key, const byte *value, size_t v_len, BTreeNodePage** root);
    Status read(const byte *key, std::string *result);
    // 必须是root结点出发
    Status read(int64_t key, std::string* result);
    Status read(BufferPoolManager*, int64_t, std::string *result);
    // 带分裂的搜索
    BTreeNodePage * search(BufferPoolManager* buffer_pool_manager, int64_t key, const byte* value, size_t v_len, BTreeNodePage** root);


//    virtual void init(int degree, page_id_t page_id);
    void init(int degree, page_id_t page_id);
    void init(BTreeNodePage* dst, int degree, int n, page_id_t page_id, bool is_leaf);

    // TODO: 这里的degree不能为0
    // TODO: 静态方法
    inline void leaf_init(BTreeNodePage* dst, int n, page_id_t page_id) {
      return init(dst, 0, n, page_id, true);
    }
    // insert kv pair to leaf node
    Status leaf_insert(int64_t key, const byte* value, int32_t v_len);

    // leaf node search key
    Status leaf_search(int64_t key, std::string *dst);
    // check key exists
    bool leaf_exists(int64_t key);

    void debug_available(BufferPoolManager*); 
    class EntryIterator {
     public:
      EntryIterator(char *ptr): data_(ptr) {}
      EntryIterator& operator = (const EntryIterator& iter) = default;
      bool operator != (const EntryIterator& iter) const;
      EntryIterator& operator ++();
      EntryIterator& operator ++(int);
      int64_t key() const;
      int32_t size() const;
      int32_t value_size() const;
      Slice value() const;
      bool ValueLessThan(const byte* value, int32_t v_len);
      char *data_;
    };
    BTreeNodePage* NewLeafPage(BufferPoolManager*, int cnt, const EntryIterator& start, const EntryIterator& end);
    BTreeNodePage* NewIndexPage(BufferPoolManager*, int cnt, int64_t key, page_id_t left,  page_id_t right);
    BTreeNodePage* NewIndexPage(BufferPoolManager*, const std::vector<int64_t>& keys, const std::vector<page_id_t>& children);
    // 迁移start到末尾的key和child到新的index page上
    BTreeNodePage* NewIndexPageFrom(BufferPoolManager*, BTreeNodePage* src, int start);
    size_t available() {
      return GetAvailable();
    }
    BTreeNodePage* index_split(BufferPoolManager*, BTreeNodePage* parent, int child_idx);
    BTreeNodePage* leaf_split(BufferPoolManager*, int64_t new_key, uint32_t total_size, BTreeNodePage* parent, int child_key_idx, int *ret_child_pos, BTreeNodePage** result);
    void index_node_add_child(int key_pos, int64_t key, int child_pos, page_id_t child, BTreeNodePage* parent = nullptr);
    void index_node_add_child(int64_t key, page_id_t child);
  };
}

#endif //YEDIS_INCLUDE_BTREE_NODE_PAGE_H_
