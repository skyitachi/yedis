//
// Created by admin on 2020/9/18.
//
#include <spdlog/spdlog.h>

#include <btree_node_page.h>
#include <buffer_pool_manager.hpp>


namespace yedis {

using BTreeNodeIter = BTreeNodePage::EntryIterator;

void BTreeNodePage::init(int degree, page_id_t page_id) {
  if (GetAvailable() == 0) {
    spdlog::debug("page_id {} set available", page_id);
    SetAvailable(PAGE_SIZE - HEADER_SIZE);
    SetIsDirty(true);
    assert(GetAvailable() == PAGE_SIZE - HEADER_SIZE);
  }
  SetPageID(page_id);
  spdlog::debug("curent page available {}", GetAvailable());
}

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len, BTreeNodePage** root) {
  return Status::NotSupported();
}

Status BTreeNodePage::add(BufferPoolManager* buffer_pool_manager, int64_t key, const byte *value, size_t v_len, BTreeNodePage** root) {
  auto target_leaf_page = search(buffer_pool_manager, key, value, v_len, root);
  spdlog::info("target_left_page: {}", target_leaf_page->GetPageID());
  return target_leaf_page->leaf_insert(key, value, v_len);
}

Status BTreeNodePage::read(int64_t key, std::string *result) {
  return Status::NotSupported();
}

Status BTreeNodePage::read(const byte *key, std::string *result) {
  return Status::NotSupported();
}

// 搜索改key对应的leaf node, 默认是从根结点向下搜索
// NOTE: 暂时只考虑不重复key的情况
BTreeNodePage * BTreeNodePage::search(BufferPoolManager* buffer_pool_manager, int64_t key, const byte *value, size_t v_len, BTreeNodePage** root) {
  auto cur = this;
  assert(root != nullptr);
  if (cur->IsLeafNode()) {
    spdlog::debug("page {} is leaf node", cur->GetPageID());
    if (cur->IsFull(v_len + sizeof(int64_t) + sizeof(size_t))) {
      spdlog::debug("page {} leaf node is full", cur->GetPageID());
      auto new_root = leaf_split(buffer_pool_manager, nullptr, -1);
      if (*root != nullptr) {
        spdlog::debug("root not null root page_id: {}", (*root)->GetPageID());
        //　update root
        *root = new_root;
      }
      // 此时root只有一个结点
      if (key < new_root->GetKey(0)) {
        return reinterpret_cast<BTreeNodePage*>(new_root->GetChild(0));
      }
      return reinterpret_cast<BTreeNodePage*>(new_root->GetChild(1));
    }
    // leaf node没有满直接返回
    return this;
  }
  // 非叶子结点
  auto it = this;
  BTreeNodePage* parent = nullptr;
  int pos = -1;
  while(!it->IsLeafNode()) {
    if (it->IsFull()) {
      if (*root == this) {
        // 当前index node就是根节点
        auto new_root = index_split(buffer_pool_manager, nullptr, 0);
        *root = new_root;
      } else {
        assert(parent != nullptr && pos != -1);
        index_split(buffer_pool_manager, parent, pos);
      }
    }
    // ignore duplicate key
    int64_t* key_start = KeyPosStart();
    int n_keys = GetCurrentEntries();
    auto child_start = ChildPosStart();
    auto key_end = key_start + n_keys;
    auto result = std::lower_bound(key_start, key_end, key);
    parent = it;
    if (result != key_end) {
      // found
      pos = result - key_start;
      spdlog::debug("child pos: {}", pos);
      spdlog::debug("child pos: {}, child page_id: {}", pos, child_start[pos]);
      it =  reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
    } else {
      // key 最大
      pos = n_keys;
      it = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
    }
  }
  return it;
}

// 返回new root
BTreeNodePage* BTreeNodePage::index_split(BufferPoolManager* buffer_pool_manager, BTreeNodePage* parent, int child_idx) {
  auto key_start = KeyPosStart();
  int n_entries = GetCurrentEntries();
  assert(n_entries >= 2);
  auto mid_key  = key_start[n_entries / 2];
  // 需要将right部分的数据迁移到新的index node上
  auto new_index_page = NewIndexPageFrom(buffer_pool_manager, this, n_entries / 2 + 1);
  if (parent == nullptr) {
    auto new_root = NewIndexPage(buffer_pool_manager, 1, mid_key, GetPageID(), new_index_page->GetPageID());
    return new_root;
  }
  parent->index_node_add_child(child_idx, mid_key, new_index_page->GetPageID());
  return nullptr;
}

// 返回new root
BTreeNodePage* BTreeNodePage::leaf_split(BufferPoolManager* buffer_pool_manager, BTreeNodePage* parent, int child_idx) {
  int n_entries = GetCurrentEntries();
  BTreeNodeIter start(GetData());
  BTreeNodeIter end(GetData() + GetEntryTail());
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
  auto new_leaf_page = NewLeafPage(buffer_pool_manager, n_entries - count, it, end);
  if (parent == nullptr) {
    auto new_index_page = NewIndexPage(buffer_pool_manager, 1, mid_key, GetPageID(), new_leaf_page->GetPageID());
    return new_index_page;
  }
  // parent添加child
  parent->index_node_add_child(child_idx, mid_key, new_leaf_page->GetPageID());

  return nullptr;
}

// NOTE: 要确保有足够的空间
Status BTreeNodePage::leaf_insert(int64_t key, const byte *value, size_t v_len) {
  auto entry_pos_start = reinterpret_cast<char *>(EntryPosStart());
  BTreeNodeIter start(entry_pos_start);
  BTreeNodeIter end(GetData() + GetEntryTail());
  size_t offset = entry_pos_start - GetData();
  for(auto it = start; it != end; it++) {
    if (key < it.key()) {
      break;
    } else if (key == it.key()) {
      if (!it.ValueLessThan(value, v_len)) {
        break;
      }
    }
    offset += it.size();
  }
  auto total_len = sizeof(key) + 4 + v_len;
  spdlog::debug("value total len: {}", total_len);
  memcpy(entry_pos_start + offset + total_len, entry_pos_start + offset, GetEntryTail() - offset);
  // write value
  auto pos_start = entry_pos_start + offset;
  EncodeFixed64(pos_start, key);
  EncodeFixed32(pos_start + sizeof(int64_t), v_len);
  memcpy(pos_start + sizeof(int64_t) + v_len, value, v_len);
  spdlog::debug("leaf_insert ok");
  return Status::OK();
}

BTreeNodePage* BTreeNodePage::NewLeafPage(BufferPoolManager* buffer_pool_manager, int cnt, const EntryIterator &start, const EntryIterator &end) {
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  leaf_init(next_page, cnt, new_page_id);
  // 复制内容
  memcpy(next_page->GetData(), start.data_, end.data_ - start.data_);
  next_page->SetIsDirty(true);
  return next_page;
}

BTreeNodePage* BTreeNodePage::NewIndexPage(BufferPoolManager* buffer_pool_manager, int cnt, int64_t key, page_id_t left, page_id_t right) {
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  init(next_page, GetDegree(), cnt, new_page_id, false);
  // 复制内容
  auto key_start = next_page->KeyPosStart();
  key_start[0] = key;

  auto child_start = next_page->ChildPosStart();
  child_start[0] = left;
  child_start[1] = right;
  next_page->SetIsDirty(true);
  return next_page;
}

BTreeNodePage* BTreeNodePage::NewIndexPageFrom(BufferPoolManager* buffer_pool_manager, BTreeNodePage *src, int start) {
  page_id_t page_id;
  auto new_page = static_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&page_id));
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
  return new_page;
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
  data_ += size();
  return *this;
}

int64_t BTreeNodePage::EntryIterator::key() const {
  return DecodeFixed64(data_);
}

size_t BTreeNodeIter::size() const {
  return sizeof(int64_t) + 4 + value_size();
}

size_t BTreeNodeIter::value_size() const {
  return DecodeFixed32(data_ + sizeof(int64_t));
}

bool BTreeNodeIter::ValueLessThan(const byte *target, size_t v_len) {
  size_t min_len = std::min(v_len, value_size());
  auto value = data_ + sizeof(int64_t) + 4;
  int ret = std::memcmp(value, target, min_len);
  if (ret < 0) {
    return true;
  } else if (ret ==0) {
    if (value_size() < v_len) {
      return true;
    }
    return false;
  }
  return false;
}

}

