//
// Created by admin on 2020/9/17.
//

#include <btree_index_node_page.hpp>
#include <algorithm>

namespace yedis {
  page_id_t BTreeIndexNodePage::search(const byte *key, size_t k_len) {
//    byte* key_start = KeyPosStart();
//    int n_keys = GetCurrentEntries();
    return INVALID_PAGE_ID;
  }

  std::vector<page_id_t> BTreeIndexNodePage::search(int64_t key) {
    int64_t* key_start = KeyPosStart();
    int n_keys = GetCurrentEntries();
    auto child_start = ChildPosStart();
    auto key_end = key_start + n_keys;
    auto first = std::lower_bound(key_start, key_end, key);
    auto ret = std::vector<page_id_t>();
    if (first == key_end) {
      // key is smallest
      ret.push_back(child_start[0]);
      return ret;
    }
    auto second = std::upper_bound(first, key_end, key);
    for (auto it = first; it != second; it++) {
      ret.push_back(child_start[it - key_start]);
    }
    return ret;
  }

}