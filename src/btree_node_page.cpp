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
    // write value
    buf_.resize(0);
    PutByte(&buf_, 0);
    PutFixed32(&buf_, k_len);
    buf_.append(reinterpret_cast<const char *>(key), k_len);
    PutFixed32(&buf_, v_len);
    buf_.append(reinterpret_cast<const char *>(value), v_len);
    // step 1: write to page data
    memcpy(entries, buf_.data(), buf_.size());
    cur_entries_ += 1;
    // write entry
    Entry entry{};
    entry.flag = 0;
    entry.key_len = k_len;
    entry.key = reinterpret_cast<byte *>(entries + 5);
    entry.value_len = v_len;
    entry.value = reinterpret_cast<byte *>(entries + 9 + k_len);
    entry_tail_ += buf_.size();
    entries_.push_back(entry);
  }
  SetIsDirty(true);
  // step 2: update header
  writeHeader();
  // step 3: update key pos
  memcpy(KeyPosStart(), reinterpret_cast<byte*>(key_pos_ptr_), sizeof(int64_t) * t_);
  return Status::OK();
}

Status BTreeNodePage::read(const byte *key, std::string *result) {
  for (int i = 0; i < cur_entries_; i++) {
    auto off = key_pos_ptr_[i];
    assert(off < entries_.size());
    if (memcmp(key, entries_[off].key, entries_[off].key_len) == 0) {
      result->append(reinterpret_cast<const char*>(entries_[off].value), entries_[off].value_len);
      return Status::OK();
    }
  }
  spdlog::debug("not found key={}", key);
  return Status::NotFound();
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
  cur_entries_ = GetCurrentEntries();
  page_id_ = page_id;
  // TODO: read key_post_ptr_
  key_pos_ptr_ = new int64_t[t_];
  // read entries
  // must
  auto entries_start = EntryPosStart();
  spdlog::info("entries_offset: {}", reinterpret_cast<char *>(entries_start) - GetData());
  entry_tail_ = reinterpret_cast<char *>(entries_start) - GetData();

  for (int i = 0; i < cur_entries_; i++) {
    Entry entry{};
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
    entries_start += entry.size();
  }
}

// TODO: start[i] error
int BTreeNodePage::upper_bound(const byte *key) {
  auto start = key_pos_ptr_;
  int i;
  spdlog::info("current entries {}", cur_entries_);
  for (int k = 0; k < cur_entries_; k++) {
    spdlog::info("current key_pos: {}", start[k]);
  }
  for(i = cur_entries_ - 1; i >= 0; i--) {
    auto offset = *(start + i);
    assert(offset < entries_.size());
    spdlog::info("offset = {}, entries length: {}", offset, entries_.size());
    auto target_key = entries_[offset].key;
    if (memcmp(key, target_key, entries_[offset].key_len) >= 0) {
      break;
    }
    spdlog::debug("start[{}] = {}", i, start[i]);
    start[i + 1] = start[i];
  }
  if (i == -1) {
    i = 0;
  }
  start[i] = cur_entries_;
  spdlog::debug("start[{}] = {}", i, start[i]);
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

