//
// Created by skyitachi on 2020/8/25.
//
#include <spdlog/spdlog.h>

#include "btree_node_page.hpp"
namespace yedis {

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len) {
  if (IsLeafNode()) {
    upper_bound(key);
    auto entries = GetData() + entry_tail_;
    printf("entry start address: %p\n", entries);
    printf("data start address: %p\n", GetData());
    spdlog::debug("entry offset: {}", reinterpret_cast<char*>(entries) - GetData());
    // write value
    buf_.resize(0);
    PutByte(&buf_, 0);
    PutFixed32(&buf_, k_len);
    buf_.append(reinterpret_cast<const char *>(key), k_len);
    PutFixed32(&buf_, v_len);
    buf_.append(reinterpret_cast<const char *>(value), v_len);
    spdlog::debug("buf size: {}", buf_.size());
    memcpy(entries, buf_.data(), buf_.size());
    cur_entries_ += 1;
    entry_tail_ += buf_.size();
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
  // read entries
  // must
  auto entries_start = EntryPosStart();
  spdlog::info("entries_offset: {}", reinterpret_cast<char *>(entries_start) - GetData());
  entry_tail_ = reinterpret_cast<char *>(entries_start) - GetData();

  printf("entries_start %p\n", entries_start + 1);
  printf("data_ %p\n", GetData() + 1990);

  for (int i = 0; i < cur_entries_; i++) {
    Entry entry;
    entry.flag = reinterpret_cast<byte>(entries_start[0]);
    entry.key_len = DecodeFixed32(reinterpret_cast<const char *>(entries_start + 1));
    entry.key = reinterpret_cast<byte *>(entries_start + 5);
    spdlog::info("key_len: {}", entry.key_len);
    spdlog::info("key: {}", std::string(reinterpret_cast<char *>(entry.key), entry.key_len));
    entry.value_len = DecodeFixed32(reinterpret_cast<const char*>(entries_start + 5 + entry.key_len));
    entry.value = reinterpret_cast<byte *>(entries_start + 9 + entry.key_len);
    spdlog::info("value_len: {}", entry.value_len);
    spdlog::info("value: {}", std::string(reinterpret_cast<char *>(entry.value), entry.value_len));
    entries_.push_back(entry);
    entry_tail_ += entry.size();
  }
}

int BTreeNodePage::upper_bound(const byte *key) {
  auto cur_entries = GetCurrentEntries();
  auto start = KeyPosStart();
  int i;
  for(i = cur_entries - 1; i >= 0; i--) {
    auto offset = start[i];
    auto target_key = entries_[offset].key;
    if (memcmp(key, target_key, entries_[offset].key_len) >= 0) {
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
  auto sz = 0;
  for(int i = 0; i < n_entries; i++) {
    sz += entries_[i].key_len + entries_[i].value_len + sizeof(uint32_t) * 2 + sizeof(byte);
  }
  return sz;
}





}

