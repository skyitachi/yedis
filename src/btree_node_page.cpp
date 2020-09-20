//
// Created by admin on 2020/9/18.
//
#include <btree_node_page.h>
#include <buffer_pool_manager.hpp>


namespace yedis {

using BTreeNodeIter = BTreeNodePage::EntryIterator;

void BTreeNodePage::init(int degree, page_id_t page_id) {
  if (IsLeafNode()) {
    // leaf_node init
  } else {
    // index_node_init
  }
}

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len) {
  if (IsLeafNode()) {
    auto index_node = reinterpret_cast<BTreeIndexNodePage*>(this);
  }
}

// 搜索改key对应的leaf node
page_id_t BTreeNodePage::search(int64_t key, const byte *value, size_t v_len) {
  auto cur = this;
  if (cur->IsFull(v_len + sizeof(int64_t) + sizeof(size_t))) {
    // TODO: split
  }
}

void BTreeNodePage::index_split() {

}

// 返回new root
BTreeNodePage* BTreeNodePage::leaf_split() {
  int n_entries = GetCurrentEntries();
  BTreeNodeIter start(GetData());
  BTreeNodeIter end(GetData() + entry_tail_);
  int count = 0;
  auto it = start;
  // 寻找mid
  for (int i = 0; i < n_entries; i++, it++) {
    count++;
    if (count == n_entries / 2) {
      break;
    }
  }
  // new index page
  // new leaf node page
  auto new_leaf_page = NewLeafPage(n_entries - count, it, end);


}

BTreeNodePage* BTreeNodePage::NewLeafPage(int cnt, const EntryIterator &start, const EntryIterator &end) {
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  leaf_init(next_page, cnt, new_page_id);
  // 复制内容
  memcpy(next_page->GetData(), start.data_, end.data_ - start.data_);
  return next_page;
}

BTreeNodePage* BTreeNodePage::NewIndexPage(int cnt, const EntryIterator &start, const EntryIterator &end) {
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  init(next_page, GetDegree(), cnt, new_page_id, false);
  // 复制内容
  // TODO:

}



void BTreeNodePage::init(BTreeNodePage* dst, int degree, int n, page_id_t page_id, bool is_leaf) {
  dst->SetPageID(page_id);
  dst->SetDegree(degree);
  dst->SetCurrentEntries(n);
  dst->SetLeafNode(is_leaf);
}

// iterator
bool BTreeNodeIter::operator !=(const EntryIterator &iter) const {
  return data_ != iter.data_;
}

BTreeNodeIter& BTreeNodeIter::operator ++() {
  auto v_len = DecodeFixed32(data_);
  data_ += 4 + v_len;
  return *this;
}

BTreeNodeIter& BTreeNodeIter::operator ++(int) {
  auto v_len = DecodeFixed32(data_);
  data_ += 4 + v_len;
  return *this;
}


}

