//
// Created by admin on 2020/9/18.
//
#include <spdlog/spdlog.h>

#include <btree_node_page.h>
#include <buffer_pool_manager.hpp>


namespace yedis {

using BTreeNodeIter = BTreeNodePage::EntryIterator;

void BTreeNodePage::init(int degree, page_id_t page_id) {
  // 没有初始化过的一定是leaf node
  // TODO: 这个判断条件明显不准
  if (GetAvailable() == 0) {
    SetAvailable(options_.page_size - LEAF_HEADER_SIZE);
    SetIsDirty(true);
    SetPrevPageID(INVALID_PAGE_ID);
    assert(GetAvailable() == options_.page_size - LEAF_HEADER_SIZE);
  }
  SetPageID(page_id);
  // degree is needed
  SetDegree(degree);
}

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len, BTreeNodePage** root) {
  return Status::NotSupported();
}

Status BTreeNodePage::add(BufferPoolManager* buffer_pool_manager, int64_t key, const byte *value, size_t v_len, BTreeNodePage** root) {
  SPDLOG_INFO("[{}], root page_id {}, available={}", key, GetPageID(), GetAvailable());
  auto target_leaf_page = search(buffer_pool_manager, key, value, v_len, root);
  assert(target_leaf_page != nullptr);
  SPDLOG_INFO("key={}, search leaf page: {}, successfully, available={}", key, target_leaf_page->GetPageID(), target_leaf_page->GetAvailable());
  auto s = target_leaf_page->leaf_insert(key, value, v_len);
  return s;
}

Status BTreeNodePage::read(const byte* key, std::string *result) {
  return Status::NotSupported();
}

Status BTreeNodePage::read(int64_t key, std::string *result) {
  return Status::NotSupported();
}

Status BTreeNodePage::read(BufferPoolManager* buffer_pool_manager, int64_t key, std::string *result) {
  auto it = this;
  std::vector<page_id_t> path;
  while (!it->IsLeafNode()) {
    path.push_back(it->GetPageID());
    int64_t* key_start = it->KeyPosStart();
    int n_keys = it->GetCurrentEntries();
    auto child_start = it->ChildPosStart();
    auto key_end = key_start + n_keys;
    auto found = std::lower_bound(key_start, key_end, key);
    auto pos = found - key_start;
    assert(pos <= it->GetCurrentEntries());
    it = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
  }
  assert(it->IsLeafNode());
  path.push_back(it->GetPageID());
  printf("[%ld] read search path: ---------------------\n", key);
  for(auto p: path) {
    printf(" %d", p);
  }
  printf("\n");
  return it->leaf_search(key, result);
}

// 搜索改key对应的leaf node, 默认是从根结点向下搜索
// NOTE: 暂时只考虑不重复key的情况
// buffer_pool_manager的依赖如何管理
BTreeNodePage * BTreeNodePage::search(BufferPoolManager* buffer_pool_manager, int64_t key, const byte *value, size_t v_len, BTreeNodePage** root) {
  assert(root != nullptr);
  auto total_len = v_len + sizeof(int64_t) + sizeof(int32_t);
  // 非叶子结点
  auto it = this;
  BTreeNodePage* parent = nullptr;
  int pos = 0;
  SPDLOG_INFO("search to index node: key={}, page_id={}", key, it->GetPageID());
  while(!it->IsLeafNode()) {
    if (it->IsFull()) {
      SPDLOG_INFO("[{}] found index node={} full", key, it->GetPageID());
      if (*root == this) {
        // 当前index node就是根节点
        auto new_root = index_split(buffer_pool_manager, nullptr, 0);
        *root = new_root;
      } else {
        assert(parent != nullptr && pos != -1);
        index_split(buffer_pool_manager, parent, pos);
      }
    }
    // TODO: ignore duplicate key
    int64_t* key_start = it->KeyPosStart();
    int n_keys = it->GetCurrentEntries();
    auto child_start = it->ChildPosStart();
    auto key_end = key_start + n_keys;
    SPDLOG_INFO("[{}] page_id: {}, key: {}, n_keys: {}, degree: {}", key, it->GetPageID(), key, n_keys, it->GetDegree());
    auto result = std::lower_bound(key_start, key_end, key);
    parent = it;
    if (result != key_end) {
      // found
      pos = result - key_start;
      spdlog::debug("key={} found child pos: {}, child page_id: {}", key, pos, child_start[pos]);
      it = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
    } else {
      // key 最大
      pos = n_keys;
      spdlog::debug("biggest key: key={} found child pos: {}, child page_id: {}", key, pos, child_start[pos]);
      assert(child_start[pos] != 0);
      it = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
    }
  }
  if (it->IsFull(total_len)) {
    SPDLOG_INFO("page {} leaf node is full", it->GetPageID());
    debug_available(buffer_pool_manager);
    auto new_root = it->leaf_split(buffer_pool_manager, parent, pos);
    SPDLOG_INFO("after leaf_split");
    debug_available(buffer_pool_manager);
    if (*root != nullptr) {
      //　update root
      SPDLOG_INFO("root not null root page_id: {}", (*root)->GetPageID());
      if (new_root != nullptr) {
        SPDLOG_INFO("search found new root: {}", new_root->GetPageID());
        *root = new_root;
      } else {
        SPDLOG_INFO("root not changed");
      }
    }
    if (parent == nullptr) {
      parent = *root;
    }
    assert(parent != nullptr);
    debug_available(buffer_pool_manager);
    SPDLOG_INFO("root page_id={}, new_root entries: {}, degree: {}", parent->GetPageID(), parent->GetCurrentEntries(), parent->GetDegree());
    SPDLOG_INFO("root page_id={},  pos: {}, index key: {}, first key: {}", parent->GetPageID(), pos, parent->GetKey(pos), parent->GetKey(0));
    SPDLOG_INFO("current_key = {}, pos_key = {}", key, parent->GetKey(pos));
    if (key < parent->GetKey(pos)) {
      auto child_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(parent->GetChild(pos)));
      SPDLOG_INFO("target leaf node page_id: {}, available={}", parent->GetChild(pos), child_page->GetAvailable());
      return reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(parent->GetChild(pos)));
    }
    debug_available(buffer_pool_manager);
    // 只有在第一次leaf node 分裂的情况下才会出现
    auto child_page_id = parent->GetChild(pos + 1);
    auto child_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_page_id));
    SPDLOG_INFO("[{}] target leaf node page_id: {}, available={}", key, child_page_id, child_page->GetAvailable());
    return child_page;
  }
  // leaf node没有满直接返回
  return it;
}

// 返回new root
BTreeNodePage* BTreeNodePage::index_split(BufferPoolManager* buffer_pool_manager, BTreeNodePage* parent, int child_idx) {
  auto key_start = KeyPosStart();
  int n_entries = GetCurrentEntries();
  SPDLOG_INFO("page_id {}, n_entries = {}", GetPageID(), n_entries);
  assert(n_entries >= 2);
  auto mid_key  = key_start[n_entries / 2];
  // 需要将right部分的数据迁移到新的index node上
  auto new_index_page = NewIndexPageFrom(buffer_pool_manager, this, n_entries / 2 + 1);
  if (parent == nullptr) {
    auto new_root = NewIndexPage(buffer_pool_manager, 1, mid_key, GetPageID(), new_index_page->GetPageID());
    SetParentPageID(new_root->GetPageID());
    new_index_page->SetParentPageID(new_root->GetPageID());
    return new_root;
  }
  new_index_page->SetParentPageID(parent->GetPageID());
  parent->index_node_add_child(child_idx, mid_key, new_index_page->GetPageID());
  return nullptr;
}

// 返回new root
BTreeNodePage* BTreeNodePage::leaf_split(BufferPoolManager* buffer_pool_manager, BTreeNodePage* parent, int child_idx) {
  assert(IsLeafNode());
  int n_entries = GetCurrentEntries();
  SPDLOG_INFO("current page_id: {}, current_entries: {}, available={}", GetPageID(), n_entries, GetAvailable());
  auto entry_pos_start = reinterpret_cast<char *>(EntryPosStart());
  BTreeNodeIter start(entry_pos_start);
  BTreeNodeIter end(entry_pos_start + GetEntryTail());
  int count = 0;
  auto it = start;
  // 寻找mid
  for (int i = 0; i < n_entries; i++, it++) {
    count++;
    if (count >= n_entries / 2) {
      break;
    }
  }
  // new index page
  // new leaf node page
  auto mid_key = it.key();
  SPDLOG_INFO("mid key: {}, count: {}", mid_key, count);
  it++;
  auto new_leaf_page = NewLeafPage(buffer_pool_manager, n_entries - count, it, end);
  assert(new_leaf_page->IsLeafNode());
  // update entries and count
  SetCurrentEntries(count);
  SetAvailable(GetAvailable() + (end.data_ - it.data_));

  // update pointers
  SetNextPageID(new_leaf_page->GetPageID());
  new_leaf_page->SetPrevPageID(GetPageID());

  if (parent != nullptr) {
    SetParentPageID(parent->GetPageID());
    new_leaf_page->SetParentPageID(parent->GetPageID());
  }

  SPDLOG_INFO("page_id={}, after split available={}", GetPageID(), GetAvailable());
  SPDLOG_INFO("after split: new_page_id={}, available={}", new_leaf_page->GetPageID(), new_leaf_page->GetAvailable());
  SPDLOG_INFO("before debug: -------------------------------");
  debug_available(buffer_pool_manager);
  SPDLOG_INFO("end debug: --------------------------------");

  if (parent == nullptr) {
    auto new_index_page = NewIndexPage(buffer_pool_manager, 1, mid_key, GetPageID(), new_leaf_page->GetPageID());
    SPDLOG_INFO("after new index page: --------------------------------");
    debug_available(buffer_pool_manager);
    SPDLOG_INFO("end debug: --------------------------------");
    SetParentPageID(new_index_page->GetPageID());
    new_leaf_page->SetParentPageID(new_index_page->GetPageID());
    return new_index_page;
  }
  // parent添加child
  // TODO:先不考虑有重复key的情况
  assert(child_idx >= 0);
  parent->index_node_add_child(child_idx, mid_key, new_leaf_page->GetPageID());

  return nullptr;
}

// NOTE: 要确保有足够的空间
Status BTreeNodePage::leaf_insert(int64_t key, const byte *value, int32_t v_len) {
  auto entry_pos_start = reinterpret_cast<char *>(EntryPosStart());
  BTreeNodeIter start(entry_pos_start);
  BTreeNodeIter end(entry_pos_start + GetEntryTail());
  size_t offset = 0;
//  SPDLOG_INFO("current entry tail {}, page_id {}", GetEntryTail(), GetPageID());
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
  auto total_len = sizeof(key) + sizeof(int32_t) + v_len;
  memcpy(entry_pos_start + offset + total_len, entry_pos_start + offset, GetEntryTail() - offset);

  // write value
  auto pos_start = entry_pos_start + offset;
  // write key
  EncodeFixed64(pos_start, key);
  // write v_len
  EncodeFixed32(pos_start + sizeof(int64_t), v_len);
  // write value
  memcpy(pos_start + sizeof(int64_t) + sizeof(int32_t), value, v_len);

  // update entry count
  auto cur_entries = GetCurrentEntries();
  SetCurrentEntries(cur_entries + 1);
  // update available size
  SetAvailable(GetAvailable() - total_len);
  SetIsDirty(true);
  SPDLOG_INFO("[page_id {}] key={}, offset={}, available={}, entry_tail= {}, move size= {}", GetPageID(), key, offset, GetAvailable(), GetEntryTail(), GetEntryTail() - offset);
  return Status::OK();
}

Status BTreeNodePage::leaf_search(int64_t target, std::string *dst) {
  assert(IsLeafNode());
  int n_entries = GetCurrentEntries();
  SPDLOG_INFO("current page_id: {}, current_entries: {}, available={}", GetPageID(), n_entries, GetAvailable());
  auto entry_pos_start = reinterpret_cast<char *>(EntryPosStart());
  BTreeNodeIter start(entry_pos_start);
  BTreeNodeIter end(entry_pos_start + GetEntryTail());
  for (auto it = start; it != end; it++) {
    if (it.key() == target) {
      SPDLOG_INFO("key: {}, value_size: {}", target, it.value_size());
      dst->assign(it.value().data(), it.value_size());
      return Status::OK();
    }
  }
  return Status::NotFound();
}

// TODO: should be static method
BTreeNodePage* BTreeNodePage::NewLeafPage(BufferPoolManager* buffer_pool_manager, int cnt, const EntryIterator &start, const EntryIterator &end) {
  assert(cnt >= 0);
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);

  leaf_init(next_page, cnt, new_page_id);
  // 复制内容
  auto copied_sz = end.data_ - start.data_;
  // NOTE: 应该从entry start开始复制
  memcpy(next_page->EntryPosStart(), start.data_, copied_sz);
  next_page->SetIsDirty(true);
  next_page->SetAvailable(options_.page_size - LEAF_HEADER_SIZE - copied_sz);
  next_page->SetNextPageID(INVALID_PAGE_ID);
  next_page->SetLeafNode(true);
  return next_page;
}

// TODO: make static method
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
  SPDLOG_INFO("index page child: {} {}, child offset: {}", left, right, reinterpret_cast<char *>(child_start) - next_page->GetData());
  next_page->SetIsDirty(true);
  // 设置entries
  next_page->SetCurrentEntries(cnt);
  auto right_child = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(right));
  SPDLOG_INFO("after new index node page_id = {}, available = {}", right_child->GetPageID(), right_child->GetAvailable());
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
  assert(pos >= 0);
  auto key_start = KeyPosStart();
  auto n_entry = GetCurrentEntries();
  SPDLOG_INFO("[page_id {}] key_pos={}, key={}, n_entries = {}, child_page_id={}", GetPageID(), pos, key, n_entry, child);
  // NOTE: 这个问题居然看了好久, 居然是size的问题...
  memmove(key_start + pos + 1, key_start + pos, (n_entry - pos) * sizeof(int64_t));
  key_start[pos]= key;
  // 移动child
  auto child_start = ChildPosStart();
  if (n_entry - pos > 0) {
    memmove(child_start + pos + 2, child_start + pos + 1, (n_entry - pos) * sizeof(page_id_t));
  }
  child_start[pos + 1] = child;

  SetIsDirty(true);
  SetCurrentEntries(n_entry + 1);
}

void BTreeNodePage::init(BTreeNodePage* dst, int degree, int n, page_id_t page_id, bool is_leaf) {
  dst->SetPageID(page_id);
  dst->SetDegree(degree);
  dst->SetCurrentEntries(n);
  dst->SetLeafNode(is_leaf);
  // leaf 节点需要设置available
  if (is_leaf) {
    dst->SetAvailable(options_.page_size - LEAF_HEADER_SIZE);
  }
  SPDLOG_INFO("init page: page_id={}, is_leaf={}, available={}", page_id, is_leaf, dst->GetAvailable());
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

int32_t BTreeNodeIter::size() const {
  return sizeof(int64_t) + sizeof(int32_t) + value_size();
}

int32_t BTreeNodeIter::value_size() const {
  return DecodeFixed32(data_ + sizeof(int64_t));
}

bool BTreeNodeIter::ValueLessThan(const byte *target, int32_t v_len) {
  size_t min_len = std::min(v_len, value_size());
  auto value = data_ + sizeof(int64_t) + sizeof(int32_t);
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

Slice BTreeNodeIter::value() const {
  return Slice(data_ + sizeof(int64_t) + sizeof(int32_t), value_size());
}

void BTreeNodePage::debug_available(BufferPoolManager* buffer_pool) {
  for (auto page_id: buffer_pool->GetAllBTreePageID()) {
    auto btree_page = reinterpret_cast<BTreeNodePage*>(buffer_pool->FetchPage(page_id));
    assert(btree_page->GetPageID() == page_id);
    if (!btree_page->IsLeafNode()) {
      SPDLOG_INFO("root page_id={}, childs = {}", page_id, btree_page->GetCurrentEntries());
    } else {
      SPDLOG_INFO("leaf page_id={}, keys = {}, available = {}", page_id, btree_page->GetCurrentEntries(), btree_page->GetAvailable());
    }
  }
}

}

