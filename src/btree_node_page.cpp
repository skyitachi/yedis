//
// Created by skyitachi on 2020/8/25.
//
#include <spdlog/spdlog.h>

#include "btree_node_page.hpp"
namespace yedis {

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len) {
  if (IsLeafNode()) {
    upper_bound(key);
    auto entries = EntryPosStart();
    printf("entry start address: %p\n", entries);
    printf("data start address: %p\n", GetData());
    spdlog::debug("entry offset: {}", reinterpret_cast<char*>(entries) - GetData());
    // write value
    entries[cur_entries_].flag = 0;
    entries[cur_entries_].key_len = k_len;
    spdlog::debug("before memcpy");
    memcpy(entries[cur_entries_].key, key, k_len);
    spdlog::debug("after memcpy");
    entries[cur_entries_].value_len = v_len;
//    memcpy(entries[cur_entries_].value, value, v_len);
    cur_entries_ += 1;
  }
  SetIsDirty(true);
  writeHeader();
  return Status::OK();
}

void BTreeNodePage::writeHeader() {
  spdlog::debug("before write page_id");
  memcpy(GetData(), reinterpret_cast<char *>(&page_id_), sizeof(page_id_t));
  spdlog::debug("after write page_id: {}", page_id_);

  memcpy(GetData() + ENTRY_COUNT_OFFSET, reinterpret_cast<char *>(&cur_entries_), sizeof(int));
  spdlog::debug("after write entry count: {}", cur_entries_);

  memcpy(GetData() + DEGREE_OFFSET, reinterpret_cast<char*>(&t_), sizeof(int));
  spdlog::debug("after write degree: {}", t_);
}

void BTreeNodePage::init(int degree, page_id_t page_id) {
  t_ = degree;
  spdlog::info("degree from file: {}", GetDegree());
  cur_entries_ = GetCurrentEntries();
  spdlog::info("cur_entries from file: {}", GetCurrentEntries());
  page_id_ = page_id;
  spdlog::info("page_id from file: {}", GetPageID());
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

size_t BTreeNodePage::available() {
  auto n_entries = GetCurrentEntries();
  auto entries = EntryPosStart();
  auto sz = 0;
  for(int i = 0; i < n_entries; i++) {
    sz += entries[i].key_len + entries[i].value_len + sizeof(uint32_t) * 2 + sizeof(byte);
  }


}

}

