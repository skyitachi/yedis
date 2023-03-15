//
// Created by skyitachi on 2020/8/25.
//
#include <spdlog/spdlog.h>
#include <spdlog/fmt/bin_to_hex.h>

#include "btree_leaf_node_page.hpp"


namespace yedis {

Status BTreeLeafNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len) {
  // 没有多余的空间，需要分裂
  if (available() < v_len + k_len + 8) {
    // 没有空间了
    return Status::NoSpace();
  }
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

Status BTreeLeafNodePage::read(const byte *key, std::string *result) {
  for (int i = 0; i < cur_entries_; i++) {
    auto off = key_pos_ptr_[i];
    assert(off < entries_.size());
    if (memcmp(key, entries_[off].key, entries_[off].key_len) == 0) {
      result->append(reinterpret_cast<const char*>(entries_[off].value), entries_[off].value_len);
      return Status::OK();
    }
  }
  return Status::NotFound("not found key", "");
}

void BTreeLeafNodePage::writeHeader() {
  memcpy(GetData(), reinterpret_cast<char *>(&page_id_), sizeof(page_id_t));
  memcpy(GetData() + ENTRY_COUNT_OFFSET, reinterpret_cast<char *>(&cur_entries_), sizeof(int));
  memcpy(GetData() + DEGREE_OFFSET, reinterpret_cast<char*>(&t_), sizeof(int));
}

void BTreeLeafNodePage::init(int degree, page_id_t page_id) {
  t_ = degree;
  cur_entries_ = GetCurrentEntries();
  page_id_ = page_id;

  // init key_pos array
  key_pos_ptr_ = new int64_t[t_];
  memcpy(key_pos_ptr_, KeyPosStart(), sizeof(int64_t) * t_);

  // read entries
  // must
  auto entries_start = EntryPosStart();
  entry_tail_ = reinterpret_cast<char *>(entries_start) - GetData();

  for (int i = 0; i < cur_entries_; i++) {
    Entry entry{};
    entry.flag = reinterpret_cast<byte>(entries_start[0]);
    entry.key_len = DecodeFixed32(reinterpret_cast<const char *>(entries_start + 1));
    entry.key = reinterpret_cast<byte *>(entries_start + 5);
    entry.value_len = DecodeFixed32(reinterpret_cast<const char*>(entries_start + 5 + entry.key_len));
    entry.value = reinterpret_cast<byte *>(entries_start + 9 + entry.key_len);
    entries_.push_back(entry);
    entry_tail_ += entry.size();
    entries_start += entry.size();
  }
}

int BTreeLeafNodePage::upper_bound(const byte *key) {
  auto start = key_pos_ptr_;
  int i;
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

size_t BTreeLeafNodePage::available() {
  return PAGE_SIZE - entry_tail_;
}


}

