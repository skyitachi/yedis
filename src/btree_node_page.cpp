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

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len, BTreeNodePage** root) {
  if (IsLeafNode()) {
    auto index_node = reinterpret_cast<BTreeIndexNodePage*>(this);
  }
}

// 搜索改key对应的leaf node, 默认是从根结点向下搜索
// NOTE: 暂时只考虑不重复key的情况
page_id_t BTreeNodePage::search(int64_t key, const byte *value, size_t v_len, BTreeNodePage** root) {
  auto cur = this;
  assert(root != nullptr);
  if (cur->IsLeafNode()) {
    if (cur->IsFull(v_len + sizeof(int64_t) + sizeof(size_t))) {
      // TODO: split
      auto new_root = leaf_split(nullptr, -1);
      if (*root != nullptr) {
        //　update root
        *root = new_root;
      }
      // 此时root只有一个结点
      if (key < new_root->GetKey(0)) {
        return new_root->GetChild(0);
      }
      return new_root->GetChild(1);
    }
    // leaf node没有满直接返回
    return GetPageID();
  }
  // 非叶子结点
  auto it = this;
  while(!it->IsLeafNode()) {
    if (it->IsFull()) {
      // TODO: index_split
      if (*root == this) {
        // 当前index node就是根节点
        auto new_root = index_split(nullptr, 0);
        *root = new_root;
      } else {
        // TODO: 这里要记住parent的信息，包括在parent中的index
      }
    }
  }
}

// 返回new root
BTreeNodePage* BTreeNodePage::index_split(BTreeNodePage* parent, int child_idx) {
  auto key_start = KeyPosStart();
  int n_entries = GetCurrentEntries();
  assert(n_entries >= 2);
  auto mid_key  = key_start[n_entries / 2];
  // 需要将right部分的数据迁移到新的index node上
  auto new_index_page = NewIndexPageFrom(this, n_entries / 2 + 1);
  if (parent == nullptr) {
    auto new_root = NewIndexPage(1, mid_key, GetPageID(), new_index_page->GetPageID());
    return new_root;
  }
  parent->index_node_add_child(child_idx, mid_key, new_index_page->GetPageID());
  return nullptr;
}

// 返回new root
BTreeNodePage* BTreeNodePage::leaf_split(BTreeNodePage* parent, int child_idx) {
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
  auto mid_key = it.key();
  it++;
  count++;
  auto new_leaf_page = NewLeafPage(n_entries - count, it, end);
  if (parent == nullptr) {
    // TODO: notice update root
    auto new_index_page = NewIndexPage(1, mid_key, GetPageID(), new_leaf_page->GetPageID());
    return new_index_page;
  }
  // parent添加child
  parent->index_node_add_child(child_idx, mid_key, new_leaf_page->GetPageID());

  return nullptr;
}

BTreeNodePage* BTreeNodePage::NewLeafPage(int cnt, const EntryIterator &start, const EntryIterator &end) {
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  leaf_init(next_page, cnt, new_page_id);
  // 复制内容
  memcpy(next_page->GetData(), start.data_, end.data_ - start.data_);
  next_page->SetIsDirty(true);
  return next_page;
}

BTreeNodePage* BTreeNodePage::NewIndexPage(int cnt, int64_t key, page_id_t left, page_id_t right) {
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  init(next_page, GetDegree(), cnt, new_page_id, false);
  // 复制内容
  auto key_start = next_page->KeyPosStart();
  key_start[0] = key;

  auto child_start = next_page->ChildPosStart();
  child_start[0] = left;
  child_start[1] = right;
  next_page->SetIsDirty(true);

}

BTreeNodePage* BTreeNodePage::NewIndexPageFrom(BTreeNodePage *src, int start) {
  page_id_t page_id;
  auto new_page = reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&page_id));
  auto cnt = src->GetCurrentEntries() - start;
  new_page->SetCurrentEntries(cnt);
  new_page->SetPageID(page_id);
  // index_node需要设置degree
  new_page->SetDegree(src->GetDegree());
  // 复制key
  memcpy(new_page->KeyPosStart(), src->KeyPosStart() + start, cnt);
  // 复制child
  memcpy(new_page->ChildPosStart(), src->KeyPosStart() + start - 1, cnt + 1);
  new_page->SetIsDirty(true);
}

void BTreeNodePage::index_node_add_child(int pos, int64_t key, page_id_t child) {
  //
  auto key_start = KeyPosStart();
  auto n_entry = GetCurrentEntries();
  memcpy(key_start + pos + 1, key_start + pos, n_entry - pos + 1);
  key_start[pos]= key;

  auto child_start = ChildPosStart();
  memcpy(child_start + pos + 1, child_start + pos, n_entry - pos + 1);
  child_start[pos] = child;

  SetIsDirty(true);
  SetCurrentEntries(n_entry + 1);
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
  // key + sizeof(v_len) + v_len
  data_ += sizeof(int64_t) + 4 + v_len;
  return *this;
}

int64_t BTreeNodePage::EntryIterator::key() const {
  return DecodeFixed64(data_);
}

}

