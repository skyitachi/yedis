//
// Created by admin on 2020/9/17.
//

#include <btree_index_node_page.hpp>

namespace yedis {
  page_id_t BTreeIndexNodePage::search(const byte *key, size_t k_len) {
//    byte* key_start = KeyPosStart();
//    int n_keys = GetCurrentEntries();
    return INVALID_PAGE_ID;
  }

  std::tuple<page_id_t, page_id_t> BTreeIndexNodePage::search(int64_t key) {
    int64_t* key_start = KeyPosStart();
    int n_keys = GetCurrentEntries();
    int l = 0, h = n_keys;
    while(l < h) {
      int m = l + (h - l) / 2;
      if (key_start[m] >= key) {
        h = m;
      } else {
        l = m + 1;
      }
    }
    h = l;
    if (key_start[l] == key) {
      for (; h < n_keys && key_start[h] == key; h++);
    }
    return std::make_tuple(l, h);
  }

}