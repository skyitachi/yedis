//
// Created by skyitachi on 2020/8/25.
//
#include "btree_node_page.hpp"

namespace yedis {

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len) {
  if (IsLeafNode()) {
    upper_bound(key);
    auto entries = EntryPosStart();
    // write value
    entries[cur_entries_].flag = 0;
    entries[cur_entries_].key_len = k_len;
    memcpy(entries[cur_entries_].key, key, k_len);
    entries[cur_entries_].value_len = v_len;
    memcpy(entries[cur_entries_].value, value, v_len);
    cur_entries_ += 1;
  }
  SetIsDirty(true);
  return Status::OK();
}

int BTreeNodePage::upper_bound(const byte *key) {
  auto cur_entries = GetCurrentEntries();
  auto start = KeyPosStart();
  auto entries = EntryPosStart();
  int i;
  for(i = cur_entries - 1; i >= 0; i--) {
    auto offset = start[i];
    auto target_key = entries[offset].key;
    if (memcmp(key, target_key, entries[offset].key_len) >= 0) {
      break;
    }
    start[i + 1] = start[i];
  }
  if (i == -1) {
    i = 0;
  }
  start[i] = cur_entries;
  return i;
}
}

