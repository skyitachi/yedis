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
    SetNextPageID(INVALID_PAGE_ID);
    assert(GetAvailable() == options_.page_size - LEAF_HEADER_SIZE);
  }
  SetPageID(page_id);
  // degree is needed
  SetDegree(degree);
}

Status BTreeNodePage::add(const byte *key, size_t k_len, const byte *value, size_t v_len, BTreeNodePage** root) {
  return Status::NotSupported();
}

// NOTE: only root node can add key
Status BTreeNodePage::add(BufferPoolManager* buffer_pool_manager, int64_t key, const byte *value, size_t v_len, BTreeNodePage** root) {
  SPDLOG_INFO("[{}], root page_id {}, available={}", key, GetPageID(), GetAvailable());

  if (buffer_pool_manager->PinnedSize() != 2) {
    // 不考虑并发的话，之后meta和root会被pin住
    // DELETE: debug
    buffer_pool_manager->debug_pinned_records();
    buffer_pool_manager->Flush();
    assert(buffer_pool_manager->PinnedSize() == 2);
  } else {
    assert(!buffer_pool_manager->IsFull());
  }
  auto target_leaf_page = search(buffer_pool_manager, key, value, v_len, root);
  if (target_leaf_page == nullptr) {
    // NOTE: just for current design
    return Status::NotSupported(Status::SubCode::kNone);
  }
  assert(target_leaf_page->Pinned());
  assert(sizeof(int64_t) + sizeof(int32_t) + v_len <= MaxAvailable());

  SPDLOG_INFO("key={}, search leaf page: {}, successfully, available={}", key, target_leaf_page->GetPageID(), target_leaf_page->GetAvailable());
  auto s = target_leaf_page->leaf_insert(key, value, v_len);
  if (target_leaf_page != *root) {
    // 非root 节点可以UnPin
    buffer_pool_manager->UnPin(target_leaf_page);
  }
  if (buffer_pool_manager->PinnedSize() != 2) {
    // DELETE: debug
    buffer_pool_manager->Flush();
    buffer_pool_manager->debug_pinned_records();
    assert(buffer_pool_manager->PinnedSize() == 2);
  }
  return s;
}

Status BTreeNodePage::read(const byte* key, std::string *result) {
  return Status::NotSupported();
}

Status BTreeNodePage::read(int64_t key, std::string *result) {
  return Status::NotSupported();
}

// TODO: Pin Or UnPin
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
  printf("[%lld] read search path: ---------------------\n", key);
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
      if (*root == it) {
        // 当前index node就是根节点
        auto new_root = it->index_split(buffer_pool_manager, nullptr, 0);
        buffer_pool_manager->Pin(new_root);
        *root = new_root;
        // look from new root
        // prevent never unpin
        buffer_pool_manager->UnPin(it);
        it = new_root;
      } else {
        assert(parent != nullptr && pos != -1);
        assert(parent->Pinned());
        it->index_split(buffer_pool_manager, parent, pos);
        // prevent never unpin
        buffer_pool_manager->UnPin(it);
        // NOTE: this will make parent equals current page
        it = parent;
      }
    }
    // TODO: ignore duplicate key
    int64_t* key_start = it->KeyPosStart();
    int n_keys = it->GetCurrentEntries();
    auto child_start = it->ChildPosStart();
    // 这里算的真有问题吗
    int64_t* key_end = key_start + n_keys;
    SPDLOG_INFO("[{}] page_id: {}, key: {}, n_keys: {}, degree: {}", key, it->GetPageID(), key, n_keys, it->GetDegree());
    printf("---------------------------------\n");
    for (int i = 0; i < n_keys; i++) {
      printf("%lld ", key_start[i]);
    }
    printf("\n---------------------------------\n");
    int64_t* result = std::lower_bound(key_start, key_end, key);
    if (parent != nullptr && parent != *root && parent != it) {
      buffer_pool_manager->UnPin(parent);
    }
    printf("result = %p, key_start = %p\n", result, key_start);
    // NOTE: 实时更新parent_id
    if (parent != nullptr && parent != it) {
      it->SetParentPageID(parent->GetPageID());
    }
    parent = it;
    if (result != key_end) {
      // found
      pos = result - key_start;
      SPDLOG_INFO("key={} found child pos: {}, child page_id: {}", key, pos, child_start[pos]);
      it = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
    } else {
      // key 最大
      pos = n_keys;
      SPDLOG_INFO("biggest key: key={} found child pos: {}, child page_id: {}", key, pos, child_start[pos]);
      assert(child_start[pos] != 0);
      it = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
    }
    // NOTE: make sure parent and leaf node is pinned
    buffer_pool_manager->Pin(it);
  }
  if (parent != nullptr) {
    // NOTE: make parent relation right
    it->SetParentPageID(parent->GetPageID());
  }
  if (it->leaf_exists(key)) {
    if (parent != nullptr && parent != *root) {
      // NOTE: important
      buffer_pool_manager->UnPin(parent);
    }
    if (it != *root) {
      buffer_pool_manager->UnPin(it);
    }
    return nullptr;
  }
  if (it->IsFull(total_len)) {
    SPDLOG_INFO("page {} leaf node is full", it->GetPageID());
    // debug_available(buffer_pool_manager);
    // TODO: child_pos has some problem
    // NOTE: why default is pos + 1
    int child_pos = pos + 1;
    SPDLOG_INFO("before leaf_split child_pos = {}, parent page_id = {}", child_pos, it->GetParentPageID());
    // NOTE: leaf_split will change parent relation
    // NOTE: here we can already know which leaf_node the key will insert
    BTreeNodePage *target_leaf_page;
    auto new_root = it->leaf_split(buffer_pool_manager, key, total_len, parent, pos, &child_pos, &target_leaf_page);
    SPDLOG_INFO("after leaf_split child_pos = {}, parent page_id = {}", child_pos, it->GetParentPageID());
    // debug_available(buffer_pool_manager);
    // *root will never be nullptr
    // TODO: the following line is unnecessary
    if (*root != nullptr) {
      //　update root
      SPDLOG_INFO("root not null root page_id: {}", (*root)->GetPageID());
      if (new_root != nullptr) {
        buffer_pool_manager->Pin(new_root);
        // NOTE: only one leaf node meet with split will cause it unpinned
        // NOTE: when just have just 1 leaf page
        if (*root != parent && *root != it) {
          // NOTE: prevent unpin parent
          buffer_pool_manager->UnPin(*root);
        }
        SPDLOG_INFO("search found new root: {}", new_root->GetPageID());
        *root = new_root;
      } else {
        SPDLOG_INFO("root not changed");
      }
    }
    if (target_leaf_page != nullptr) {
      SPDLOG_INFO("split target_leaf_page page_id = {}", target_leaf_page->GetPageID());
    }
    assert(target_leaf_page != nullptr && target_leaf_page->Pinned());
    if (target_leaf_page != it) {
      buffer_pool_manager->UnPin(it);
    }
    if (parent != nullptr && parent != *root) {
      // NOTE: important
      buffer_pool_manager->UnPin(parent);
    }
    return target_leaf_page;
  } else {
    // leaf node没有满直接返回
    // it was pinned in the last loop
    if (parent != nullptr && parent != *root) {
      // NOTE: important
      buffer_pool_manager->UnPin(parent);
    }

  }
  return it;
}

// 返回new root
// index_split will never recursive
BTreeNodePage* BTreeNodePage::index_split(BufferPoolManager* buffer_pool_manager, BTreeNodePage* parent, int child_idx) {
  auto key_start = KeyPosStart();
  int n_entries = GetCurrentEntries();
  SPDLOG_INFO("page_id {}, n_entries = {}", GetPageID(), n_entries);
  assert(n_entries >= 4);
  assert(!IsLeafNode());
  // NOTE: make mid_key to parent level
  auto mid_key  = key_start[n_entries / 2];
  // 需要将right部分的数据迁移到新的index node上
  auto new_index_page = NewIndexPageFrom(buffer_pool_manager, this, n_entries / 2 + 1);
  SPDLOG_INFO("split mid_key = {}", mid_key);
  // no need to rewrite key_start[n_entries / 2]
  SetCurrentEntries(n_entries / 2);
  if (parent == nullptr) {
    auto new_root = NewIndexPage(buffer_pool_manager, 1, mid_key, GetPageID(), new_index_page->GetPageID());
    SetParentPageID(new_root->GetPageID());
    new_index_page->SetParentPageID(new_root->GetPageID());
    return new_root;
  }
  SPDLOG_INFO("with parent {}", parent->GetPageID());
  new_index_page->SetParentPageID(parent->GetPageID());
  // parent never overflow
  parent->index_node_add_child(child_idx, mid_key, child_idx + 1, new_index_page->GetPageID());
  return nullptr;
}

// 返回new root, 设置child pos的位置
// 新key有可能插入老节点，这里还要判断老节点分裂之后能否容下新key
// NOTE: assume current leaf_split page is pinned
// @param {int} child_key_idx - key position in parent index node
BTreeNodePage* BTreeNodePage::leaf_split(
    BufferPoolManager* buffer_pool_manager,
    int64_t new_key,
    uint32_t total_size,
    BTreeNodePage* parent,
    int child_key_idx,
    int *ret_child_pos,
    BTreeNodePage **result
    ) {
  assert(Pinned());
  if (parent != nullptr) {
    assert(parent->Pinned());
  }
  assert(IsLeafNode());
  int n_entries = GetCurrentEntries();
  SPDLOG_INFO("current page_id: {}, current_entries: {}, available={}", GetPageID(), n_entries, GetAvailable());
  auto entry_pos_start = reinterpret_cast<char *>(EntryPosStart());
  BTreeNodeIter start(entry_pos_start);
  BTreeNodeIter end(entry_pos_start + GetEntryTail());
  int count = 0;
  int64_t mid_key;
  BTreeNodePage *new_leaf_page = nullptr;
  auto it = start;
  auto prev_key = it.key();
  auto sz = 0;
  auto used = MaxAvailable() - GetAvailable();
  std::vector<int64_t > child_keys;
  std::vector<page_id_t> child_page_ids;
  page_id_t right;
  page_id_t left;
  // 寻找mid
  for (int i = 0; i < n_entries; i++, it++) {
    // 不考虑key重复的情况
    if (new_key < it.key()) {
      break;
    } else {
      sz += it.size();
      count++;
    }
    prev_key = it.key();
  }
  if (sz == 0) {
    // new_key is smallest
    SPDLOG_INFO("[page_id {}] to insert smallest key", GetPageID());
    mid_key = new_key;
    new_leaf_page = NewLeafPage(buffer_pool_manager, 0, it, it);
    // 手动pin?
    buffer_pool_manager->Pin(new_leaf_page);
    left = new_leaf_page->GetPageID();
    child_keys.push_back(mid_key);
    child_page_ids.push_back(left);
    child_page_ids.push_back(GetPageID());

    *ret_child_pos = child_key_idx;
    auto child_pos = child_key_idx;
    // prev & next
    // 这里能正确反映插入顺序
    new_leaf_page->SetPrevPageID(GetPrevPageID());
    new_leaf_page->SetNextPageID(GetPageID());
    SetPrevPageID(left);
    *result = new_leaf_page;

    if (parent != nullptr) {
      SetParentPageID(parent->GetPageID());
      new_leaf_page->SetParentPageID(parent->GetPageID());
      assert(child_key_idx >= 0);
      // NOTE: always new_leaf_page, parent never overflow
      parent->index_node_add_child(child_key_idx, mid_key, child_pos, new_leaf_page->GetPageID());
//      buffer_pool_manager->UnPin(new_leaf_page);
      return nullptr;
    } else {
      // 不支持并发
      auto new_index_page = NewIndexPage(buffer_pool_manager, child_keys, child_page_ids);
      buffer_pool_manager->Pin(new_index_page);
      SetParentPageID(new_index_page->GetPageID());
      new_leaf_page->SetParentPageID(new_index_page->GetPageID());
//      buffer_pool_manager->UnPin(new_leaf_page);
      return new_index_page;
    }
  } else if (sz == used) {
    // new_key is biggest
    SPDLOG_INFO("leaf_split with max key: {}, prev_key: {}", new_key, prev_key);
    mid_key = prev_key;
    new_leaf_page = NewLeafPage(buffer_pool_manager, 0, it, it);
    buffer_pool_manager->Pin(new_leaf_page);
    child_keys.push_back(mid_key);
    child_page_ids.push_back(GetPageID());
    child_page_ids.push_back(new_leaf_page->GetPageID());

    *ret_child_pos = child_key_idx + 1;
    auto child_pos = child_key_idx + 1;
    *result = new_leaf_page;

    // prev & next
    new_leaf_page->SetPrevPageID(GetPageID());
    new_leaf_page->SetNextPageID(GetNextPageID());
    SetNextPageID(new_leaf_page->GetPageID());

    if (parent != nullptr) {
      SetParentPageID(parent->GetPageID());
      new_leaf_page->SetParentPageID(parent->GetPageID());
      assert(child_key_idx >= 0);
      // NOTE: always new_leaf_page, parent never overflow
      parent->index_node_add_child(child_key_idx, mid_key, child_pos, new_leaf_page->GetPageID());
//      buffer_pool_manager->UnPin(new_leaf_page);
      return nullptr;
    } else {
      auto new_index_page = NewIndexPage(buffer_pool_manager, child_keys, child_page_ids);
      // NOTE: make sure new_index_page will pinned, if new_index_page is not null, it will be return as root
      // pin here will be more accurate
      buffer_pool_manager->Pin(new_index_page);
      SetParentPageID(new_index_page->GetPageID());
      new_leaf_page->SetParentPageID(new_index_page->GetPageID());
//      buffer_pool_manager->UnPin(new_leaf_page);
      return new_index_page;
    }
  } else {
    if (sz + total_size <= MaxAvailable())  {
      SPDLOG_INFO("leaf_split insert node with left child, key: {}", new_key);
      // 放入前一个node上
      mid_key = new_key;
      // right page
      new_leaf_page = NewLeafPage(buffer_pool_manager, n_entries - count, it, end);
      buffer_pool_manager->Pin(new_leaf_page);
      // 先不插入，所以count不会加一
      SetCurrentEntries(count);

      child_keys.push_back(mid_key);
      child_page_ids.push_back(GetPageID());
      child_page_ids.push_back(new_leaf_page->GetPageID());

      SetAvailable(MaxAvailable() - sz);
      new_leaf_page->SetPrevPageID(GetPageID());
      new_leaf_page->SetNextPageID(GetNextPageID());
      SetNextPageID(new_leaf_page->GetPageID());

      *ret_child_pos = child_key_idx;
      auto new_child_pos = child_key_idx + 1;
      // current page
      *result = this;
      assert(this->Pinned());
      // make sure pin the page

      if (parent != nullptr) {
        SetParentPageID(parent->GetPageID());
        new_leaf_page->SetParentPageID(parent->GetPageID());
        assert(child_key_idx >= 0);
        // NOTE: always new_leaf_page, parent never overflow
        parent->index_node_add_child(child_key_idx, mid_key, new_child_pos, new_leaf_page->GetPageID());
        buffer_pool_manager->UnPin(new_leaf_page);
        return nullptr;
      } else {
        auto new_index_page = NewIndexPage(buffer_pool_manager, child_keys, child_page_ids);
        buffer_pool_manager->Pin(new_index_page);
        SetParentPageID(new_index_page->GetPageID());
        new_leaf_page->SetParentPageID(new_index_page->GetPageID());
        buffer_pool_manager->UnPin(new_leaf_page);
        return new_index_page;
      }

    } else if (used - sz + total_size <= MaxAvailable()) {
      // 放入后一个node中
      SPDLOG_INFO("leaf_split insert node with right child, key: {}, prev_key: {}", new_key, prev_key);
      mid_key = prev_key;

      // right page
      new_leaf_page = NewLeafPage(buffer_pool_manager, n_entries - count, it, end);
      buffer_pool_manager->Pin(new_leaf_page);
      SetCurrentEntries(count);
      SetAvailable(MaxAvailable() - sz);

      child_keys.push_back(mid_key);
      child_page_ids.push_back(GetPageID());
      child_page_ids.push_back(new_leaf_page->GetPageID());

      new_leaf_page->SetPrevPageID(GetPageID());
      new_leaf_page->SetNextPageID(GetNextPageID());
      SetNextPageID(new_leaf_page->GetPageID());

      // right child
      *ret_child_pos = child_key_idx + 1;
      auto new_child_pos = child_key_idx + 1;
      *result = new_leaf_page;

      if (parent != nullptr) {
        SetParentPageID(parent->GetPageID());
        new_leaf_page->SetParentPageID(parent->GetPageID());
        assert(child_key_idx >= 0);
        // NOTE: always new_leaf_page, parent never overflow
        parent->index_node_add_child(child_key_idx, mid_key, new_child_pos, new_leaf_page->GetPageID());
//        buffer_pool_manager->UnPin(new_leaf_page);
        buffer_pool_manager->UnPin(this);
        return nullptr;
      } else {
        // TODO: need test
        auto new_index_page = NewIndexPage(buffer_pool_manager, child_keys, child_page_ids);
        buffer_pool_manager->Pin(new_index_page);
        SetParentPageID(new_index_page->GetPageID());
        new_leaf_page->SetParentPageID(new_index_page->GetPageID());
//        buffer_pool_manager->UnPin(new_leaf_page);
        return new_index_page;
      }
    } else {
      // 只能新建一个page来存储new_key
      // 要插入两个page
      SPDLOG_INFO("need 2 page to storage new_key");
      auto single_page = NewLeafPage(buffer_pool_manager, 0, it, it);
      buffer_pool_manager->Pin(single_page);
      assert(single_page->IsLeafNode());
      new_leaf_page = NewLeafPage(buffer_pool_manager, n_entries - count, it, end);
      buffer_pool_manager->Pin(new_leaf_page);
      SetCurrentEntries(count);
      SetAvailable(MaxAvailable() - sz);

      child_keys.push_back(prev_key);
      child_keys.push_back(new_key);

      child_page_ids.push_back(GetPageID());
      child_page_ids.push_back(single_page->GetPageID());
      child_page_ids.push_back(new_leaf_page->GetPageID());

      new_leaf_page->SetNextPageID(GetNextPageID());
      new_leaf_page->SetPrevPageID(single_page->GetPageID());
      single_page->SetPrevPageID(GetPageID());
      single_page->SetNextPageID(new_leaf_page->GetPageID());

      SetNextPageID(single_page->GetPageID());

      // NOTE: here ret_child_pos has no effect, never use ret_child_pos here
      *ret_child_pos = child_key_idx + 1;
      *result = single_page;

      if (parent != nullptr) {
        // TODO: parent update has some problem
        single_page->SetParentPageID(parent->GetPageID());
        new_leaf_page->SetParentPageID(parent->GetPageID());
        assert(child_key_idx >= 0);
        auto single_page_id = single_page->GetPageID();
        auto new_leaf_page_id = new_leaf_page->GetPageID();
        // NOTE: always new_leaf_page, parent never overflow
        parent->index_node_add_child(child_key_idx, prev_key, child_key_idx + 1, single_page_id);
        if (!parent->IsFull()) {
          // NOTE: parent can add more child
          // TODO: need test case
          parent->index_node_add_child(child_key_idx + 1, new_key, child_key_idx + 2, new_leaf_page_id);
          // unpin as soon as possible
          // NOTE: single_page as target_leaf_page no need to Unpin
          // buffer_pool_manager->UnPin(single_page);
          buffer_pool_manager->UnPin(new_leaf_page);
          return nullptr;
        }
        SPDLOG_INFO("parent {} is full", parent->GetPageID());
        if (parent->GetParentPageID() == INVALID_PAGE_ID) {
          SPDLOG_INFO("parent {} was root page", parent->GetPageID());
          // NOTE: parent is root page
          auto new_root = parent->index_split(buffer_pool_manager, nullptr, INVALID_PAGE_ID);
          // NOTE: judge current page is in new_root's left or right
          buffer_pool_manager->Pin(new_root);
          BTreeNodePage* target_index_page;
          page_id_t target_index_page_id;
          if (new_key > new_root->GetKey(0)) {
            // should insert new root right child
            target_index_page_id = new_root->GetChild(1);
          } else {
            target_index_page_id = new_root->GetChild(0);
          }
          if (child_key_idx > MAX_DEGREE / 2) {
            SPDLOG_INFO("new_page will add the right half, child_idx {}", child_key_idx);
            // current page split into right half
            SetParentPageID(new_root->GetChild(1));
            single_page->SetParentPageID(new_root->GetChild(1));
            new_leaf_page->SetParentPageID(new_root->GetChild(1));
          }
          // unpin as soon as possible
          // buffer_pool_manager->UnPin(single_page);
          buffer_pool_manager->UnPin(new_leaf_page);
          assert(target_index_page_id != INVALID_PAGE_ID);
          SPDLOG_INFO("new_target_index_page_id {}", target_index_page_id);
          // NOTE: target_index_page_id maybe same as parent
          if (target_index_page_id == parent->GetPageID()) {
            SPDLOG_INFO("found target index page id equals parent {}", target_index_page_id);
            parent->index_node_add_child(new_key, new_leaf_page_id);
            return new_root;
          }
          target_index_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(target_index_page_id));
          buffer_pool_manager->Pin(target_index_page);
          target_index_page->index_node_add_child(new_key, new_leaf_page_id);
          buffer_pool_manager->UnPin(target_index_page);
          return new_root;
        }
        // need 7 pinned records
        auto grandparent = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(parent->GetParentPageID()));
        buffer_pool_manager->Pin(grandparent);
        assert(!grandparent->IsFull());
        // search parent in grandparent index
        auto first = parent->GetKey(0);
        auto idx_pos = std::lower_bound(grandparent->KeyPosStart(), grandparent->KeyPosStart() + grandparent->GetCurrentEntries(), first);
        auto idx = idx_pos - grandparent->KeyPosStart();
        SPDLOG_INFO("parent {} is grandparent {} 's {} child", parent->GetPageID(), grandparent->GetPageID(), idx);
        parent->index_split(buffer_pool_manager, grandparent, idx);
        auto target_index_page_id = INVALID_PAGE_ID;
        if (new_key > grandparent->GetKey(idx)) {
          target_index_page_id = grandparent->GetChild(idx + 1);
        } else {
          target_index_page_id = grandparent->GetChild(idx);
        }
        assert(target_index_page_id != INVALID_PAGE_ID);

        // TODO: need test
        if (child_key_idx > MAX_DEGREE / 2) {
          SPDLOG_INFO("child idx {} will split into right page", child_key_idx);
          // current page split into right half
          SetParentPageID(grandparent->GetChild(idx + 1));
          single_page->SetParentPageID(grandparent->GetChild(idx + 1));
          new_leaf_page->SetParentPageID(grandparent->GetChild(idx + 1));
        }
        // unpin as soon as possible
        // buffer_pool_manager->UnPin(single_page);
        buffer_pool_manager->UnPin(new_leaf_page);
        // NOTE: grandparent also need unpin
        if (grandparent->GetParentPageID() != INVALID_PAGE_ID) {
          SPDLOG_INFO("unpin grandparent page: {}, current parent_page id: {}", grandparent->GetPageID(), grandparent->GetParentPageID());
          buffer_pool_manager->UnPin(grandparent);
        }

        auto target_index_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(target_index_page_id));
        buffer_pool_manager->Pin(target_index_page);
        target_index_page->index_node_add_child(new_key, new_leaf_page_id);
        buffer_pool_manager->UnPin(target_index_page);
        return nullptr;
      } else {
        // TODO: need test
        auto new_index_page = NewIndexPage(buffer_pool_manager, child_keys, child_page_ids);
        buffer_pool_manager->Pin(new_index_page);
        SetParentPageID(new_index_page->GetPageID());
        new_leaf_page->SetParentPageID(new_index_page->GetPageID());
        single_page->SetParentPageID(new_index_page->GetPageID());
        // buffer_pool_manager->UnPin(single_page);
        buffer_pool_manager->UnPin(new_leaf_page);
        return new_index_page;
      }
    }
  }
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

bool BTreeNodePage::leaf_exists(int64_t target) {
  assert(IsLeafNode());
  int n_entries = GetCurrentEntries();
  SPDLOG_INFO("current page_id: {}, current_entries: {}, available={}", GetPageID(), n_entries, GetAvailable());
  auto entry_pos_start = reinterpret_cast<char *>(EntryPosStart());
  BTreeNodeIter start(entry_pos_start);
  BTreeNodeIter end(entry_pos_start + GetEntryTail());
  for (auto it = start; it != end; it++) {
    if (it.key() == target) {
      SPDLOG_INFO("key: {}, value_size: {}", target, it.value_size());
      return true;
    }
  }
  return false;
}

// TODO: make static method
BTreeNodePage* BTreeNodePage::NewIndexPage(BufferPoolManager* buffer_pool_manager, int cnt, int64_t key, page_id_t left, page_id_t right) {
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  // TODO: make sure
  init(next_page, MAX_DEGREE, cnt, new_page_id, false);
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
  SPDLOG_INFO("after new index node right page_id = {}, available = {}", right_child->GetPageID(), right_child->GetAvailable());
  return next_page;
}

// TODO: should be static method
BTreeNodePage* BTreeNodePage::NewLeafPage(BufferPoolManager* buffer_pool_manager, int cnt, const EntryIterator &start, const EntryIterator &end) {
  assert(cnt >= 0);
  SPDLOG_INFO("[page_id {}] leaf page with count {}", GetPageID(), cnt);
  page_id_t new_page_id;
  auto next_page = static_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);

  leaf_init(next_page, cnt, new_page_id);
  // 复制内容
  if (cnt > 0) {
    auto copied_sz = end.data_ - start.data_;
    // NOTE: 应该从entry start开始复制
    memcpy(next_page->EntryPosStart(), start.data_, copied_sz);
    next_page->SetAvailable(MaxAvailable() - copied_sz);
  } else {
    next_page->SetAvailable(MaxAvailable());
  }
  next_page->SetIsDirty(true);
  next_page->SetNextPageID(INVALID_PAGE_ID);
  next_page->SetLeafNode(true);
  return next_page;
}

// 一定要make static，因为当前页可能被换出到内存了
BTreeNodePage* BTreeNodePage::NewIndexPage(BufferPoolManager *buffer_pool_manager,
                                           const std::vector<int64_t> &keys,
                                           const std::vector<page_id_t> &children) {
  page_id_t new_page_id;
  auto next_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&new_page_id));
  assert(new_page_id != INVALID_PAGE_ID);
  init(next_page, MAX_DEGREE, keys.size(), new_page_id, false);
  auto key_start = next_page->KeyPosStart();
  for (int i = 0; i < keys.size(); i++) {
    key_start[i] = keys[i];
  }
  auto child_start = next_page->ChildPosStart();
  for (int i = 0; i < children.size(); i++) {
    child_start[i] = children[i];
  }

  next_page->SetIsDirty(true);
  // 设置entries
  next_page->SetCurrentEntries(keys.size());
  return next_page;
}

BTreeNodePage* BTreeNodePage::NewIndexPageFrom(BufferPoolManager* buffer_pool_manager, BTreeNodePage *src, int start) {
  page_id_t page_id;
  auto new_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->NewPage(&page_id));
  auto cnt = src->GetCurrentEntries() - start;
  SPDLOG_INFO("new index page_id {}, with {} keys, start: {}", page_id, cnt, start);
  new_page->SetCurrentEntries(cnt);
  new_page->SetPageID(page_id);
  // index_node需要设置degree
  new_page->SetDegree(src->GetDegree());
  // 复制key [start, n_entries)
  memmove(new_page->KeyPosStart(), src->KeyPosStart() + start, cnt * sizeof(int64_t));
  printf("origin children-------------------------------\n");
  for (int i = start; i < src->GetCurrentEntries(); i++) {
    printf("%lld ", src->GetKey(i));
  }
  printf("\n----------------------\n");
  printf("new children--------------------------\n");
  for (int i = 0; i < cnt; i++) {
    printf("%lld ", new_page->GetKey(i));
  }
  printf("\n----------------------\n");
  // 复制child, [start, n_entries]
  memmove(new_page->ChildPosStart(), src->ChildPosStart() + start, (cnt + 1) * sizeof(int64_t));
  new_page->SetIsDirty(true);
  new_page->SetLeafNode(false);
  return new_page;
}

// NOTE: child should be new page_id
void BTreeNodePage::index_node_add_child(int pos, int64_t key, int child_pos, page_id_t child, BTreeNodePage* parent) {
  assert(pos >= 0);
  auto key_start = KeyPosStart();
  auto n_entry = GetCurrentEntries();
  SPDLOG_INFO("[page_id {}] key_pos={}, key={}, n_entries = {}, child_pos = {}, child_page_id={}", GetPageID(), pos, key, n_entry, child_pos, child);
  // NOTE: 这个问题居然看了好久, 居然是size的问题...
  memmove(key_start + pos + 1, key_start + pos, (n_entry - pos) * sizeof(int64_t));
  key_start[pos]= key;
  // 移动child
  auto child_start = ChildPosStart();
  // TODO: 这里有问题
  if (n_entry - pos >= 0) {
    // TODO: check
    memmove(child_start + child_pos + 1, child_start + child_pos, (n_entry - child_pos + 1) * sizeof(page_id_t));
  }
  child_start[child_pos] = child;

  SetIsDirty(true);
  // POST ASSERT
  SetCurrentEntries(n_entry + 1);
  assert(GetCurrentEntries() <= GetDegree());
  SPDLOG_INFO("[page_id {}] after add child entries {}", GetPageID(), GetCurrentEntries());
}

// NOTE: just for one page key
void BTreeNodePage::index_node_add_child(int64_t key, page_id_t child) {
  assert(!IsLeafNode());
  assert(!IsFull());
  auto key_start = KeyPosStart();
  auto key_end = key_start + GetCurrentEntries();
  auto it = std::lower_bound(key_start, key_end, key);

  auto idx = it - key_start;
  SPDLOG_INFO("new_key in the index {}", idx);
  // TODO: need test child = key_idx + 1
  index_node_add_child(idx, key, idx + 1, child, nullptr);
}

Status BTreeNodePage::remove(BufferPoolManager* buffer_pool_manager, int64_t key, BTreeNodePage** root) {
  auto it = this;
  BTreeNodePage* parent = nullptr;
  while (!it->IsLeafNode()) {
    int64_t* key_start = it->KeyPosStart();
    int n_keys = it->GetCurrentEntries();
    auto child_start = it->ChildPosStart();
    auto key_end = key_start + n_keys;
    auto found = std::lower_bound(key_start, key_end, key);
    auto pos = found - key_start;
    assert(pos <= it->GetCurrentEntries());
    parent = it;
    it = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(child_start[pos]));
    if (parent != nullptr) {
      it->SetParentPageID(parent->GetPageID());
    }
  }
  assert(it->IsLeafNode());
  return it->leaf_remove(buffer_pool_manager, key, root);
}

Status BTreeNodePage::leaf_remove(BufferPoolManager* buffer_pool_manager, int64_t key, BTreeNodePage** root) {
  assert(IsLeafNode());
  //
  auto entry_pos_start = reinterpret_cast<char *>(EntryPosStart());
  BTreeNodeIter start(entry_pos_start);
  BTreeNodeIter end(entry_pos_start + GetEntryTail());
  auto total = GetEntryTail();
  size_t offset = 0;
  bool found = false;
  auto total_size = 0;
  for(auto it = start; it != end; it++) {
    if (key == it.key()) {
      found = true;
      total_size = it.size();
      break;
    }
    offset += it.size();
  }
  if (!found) {
    SPDLOG_INFO("leaf {} not found key {}", GetPageID(), key);
    return Status::NotFound();
  }
  auto left = total - offset - total_size;
  if (left != 0) {
    memmove(entry_pos_start + offset, entry_pos_start + total_size + offset, left);
  }
  SetAvailable(GetAvailable() + total_size);
  if (total > total_size) {
    SPDLOG_INFO("page_id={} success delete key {}", GetPageID(), key);
    return Status::OK();
  }
  SPDLOG_INFO("page_id={} will be empty page", GetPageID());
  if (*root == this) {
    // root page is empty
    SPDLOG_INFO("root_page {} is empty", GetPageID());
    return Status::OK();
  }
  // empty leaf_page, will cause parent to remove child
  auto parent_id = GetParentPageID();
  assert(parent_id != INVALID_PAGE_ID);

  SPDLOG_INFO("remove_page found parent id {}", parent_id);
  auto parent = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(parent_id));
  auto key_pos = std::lower_bound(parent->KeyPosStart(), parent->KeyPosStart() + parent->GetCurrentEntries(), key);
  auto child_index = key_pos - parent->KeyPosStart();

  // NOTE: make prev and next works
  auto next_page_id = GetNextPageID();
  if (next_page_id != INVALID_PAGE_ID) {
    auto next_leaf_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(next_page_id));
    next_leaf_page->SetPrevPageID(GetPrevPageID());
  }
  auto prev_page_id = GetPrevPageID();
  if (prev_page_id != INVALID_PAGE_ID) {
    auto prev_leaf_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(prev_page_id));
    prev_leaf_page->SetPrevPageID(GetNextPageID());
  }

  if (child_index == parent->GetCurrentEntries()) {
    return parent->index_remove(buffer_pool_manager, child_index - 1, child_index, root);
  }
  return parent->index_remove(buffer_pool_manager, child_index, child_index, root);
}

// NOTE：这里的逻辑最复杂
Status BTreeNodePage::index_remove(BufferPoolManager* buffer_pool_manager, int key_idx, int child_idx, BTreeNodePage** root) {
  auto degree = GetDegree();
  auto entries = GetCurrentEntries();
  auto key_start = KeyPosStart();
  auto child_start = ChildPosStart();
  assert(entries >= 1);
  assert(child_idx <= entries);
  assert(key_idx < entries);
  SPDLOG_INFO("page_id {} to remove key_idx = {}, child_idx = {}", GetPageID(), key_idx, child_idx);
  // index_node entries >= ceil(degree / 2)
  if (entries - 1 >= (degree / 2) || *root == this) {
    if (*root == this && entries == 1) {
      // NOTE: 特殊情况
      auto new_root_page_id = GetChild(0);
      if (child_idx == 0) {
        new_root_page_id = GetChild(1);
      }
      SPDLOG_INFO("root will down to child, new_root_page_id {}", new_root_page_id);
      auto new_root_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(new_root_page_id));
      auto old_root = *root;
      assert(old_root != nullptr);
      // NOTE: remove
      buffer_pool_manager->UnPin(old_root);
      old_root->ResetMemory();
      *root = new_root_page;
      return Status::OK();
    }
    // NOTE: only root can have less than degree / 2
    // if last child, just update entries count
    auto key_move_cnt = entries - key_idx - 1;
    SPDLOG_INFO("index_page {} has {} child, no_redistribute, move key cnt: {}", GetPageID(), entries + 1, key_move_cnt);
    memmove(key_start + key_idx, key_start + (key_idx + 1), key_move_cnt * sizeof(int64_t));
    auto child_move_cnt = entries - child_idx;
    memmove(child_start + child_idx, child_start + (child_idx + 1), child_move_cnt * sizeof(page_id_t));
    SetCurrentEntries(entries - 1);
    return Status::OK();
  } else {
    // need redistribute
    // NOTE: 只需要向相邻兄弟结点借一次，父结点不会改变的场景, borrow key from parent
    // recursive remove
    auto parent_id = GetParentPageID();
    assert(parent_id != INVALID_PAGE_ID);
    SPDLOG_INFO("index_remove will cause redistribute, parent_page_id {}, current page_id {}", parent_id, GetPageID());
    auto parent_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(parent_id));
    int parent_child_idx;
    // found current page's index in parent page
    auto s = parent_page->find_child_index(GetPageID(), &parent_child_idx);
    SPDLOG_INFO("parent_child_idx = {}, child_idx = {}", parent_child_idx, child_idx);
    assert(s.ok());
    // remove current index
    if (child_idx == entries) {
      // right most child no need to delete
      SPDLOG_INFO("delete to right most child {}", child_idx);
    }
    SPDLOG_INFO("delete key_idx {} need to borrow from parent", key_idx);
    // NOTE: assume key_idx always right
    memmove(key_start + key_idx, key_start + key_idx + 1, (entries - key_idx - 1) * sizeof(int64_t));
    memmove(child_start + child_idx, child_start + child_idx + 1, (entries - child_idx) * sizeof(page_id_t));
    if (parent_child_idx == 0) {
      // left_most child, need to merge right sibling
      auto next_sibling_page_id = parent_page->GetChild(1);
      assert(next_sibling_page_id != 0 && next_sibling_page_id != INVALID_PAGE_ID);
      auto next_sibling_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(next_sibling_page_id));
      auto sibling_entries = next_sibling_page->GetCurrentEntries();
      if (need_merge(entries - 1, sibling_entries)) {
        // NOTE: just remove a child
        SPDLOG_INFO("merge two page left {}, right {}", GetPageID(), next_sibling_page_id);
        // NOTE: borrow from parent's first key
        SPDLOG_INFO("page_id {} borrow first key from parent {}, key: {}", GetPageID(), parent_page->GetPageID(), parent_page->GetKey(0));
        // NOTE: update entries count before merge
        SetCurrentEntries(entries - 1);
        s = merge(this, next_sibling_page, parent_page, 0);
        assert(s.ok());
        // TODO: delete next_sibling_page
        next_sibling_page->ResetMemory();
        return parent_page->index_remove(buffer_pool_manager, 0, 1, root);
      } else {
        // redistribute is ok, no recursive
        auto max_redistribute_cnt = sibling_entries - (MAX_DEGREE / 2);
        assert(max_redistribute_cnt > 0);
        SPDLOG_INFO("redistribute children between {} and {} is ok", GetPageID(), next_sibling_page_id);
        SetCurrentEntries(entries - 1);
        s = redistribute(this, next_sibling_page, parent_page, 0);
        assert(s.ok());
        return Status::OK();
      }

    } else if (parent_child_idx == parent_page->GetCurrentEntries()) {
      // right most child
      SPDLOG_INFO("delete right most child page {} in parent {} idx {}", GetPageID(), parent_page->GetPageID(), parent_child_idx);
      auto prev_sibling_page_id = parent_page->GetChild(parent_child_idx - 1);
      assert(prev_sibling_page_id != 0 && prev_sibling_page_id != INVALID_PAGE_ID);
      auto prev_sibling_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(prev_sibling_page_id));
      auto sibling_entries = prev_sibling_page->GetCurrentEntries();
      if (need_merge(sibling_entries, entries - 1)) {
        SPDLOG_INFO("merge two page left {}, right {}", prev_sibling_page_id, GetPageID());
        SetCurrentEntries(entries - 1);
        s = merge(prev_sibling_page, this, parent_page, parent_child_idx - 1);
        assert(s.ok());
        // current page reset memory
        ResetMemory();
        return parent_page->index_remove(buffer_pool_manager, parent_child_idx - 1, parent_child_idx, root);
      } else {
        SPDLOG_INFO("redistribute children between {} and {} is ok", prev_sibling_page_id, GetPageID());
        SetCurrentEntries(entries - 1);
        return redistribute(prev_sibling_page, this, parent_page, parent_child_idx - 1);
      }
    } else {
      // 中间节点，优先redistribute，如果不能再考虑合并，向节点数少的合并或者向节点数多的redistribute
      auto prev_page_id = parent_page->GetChild(parent_child_idx - 1);
      auto next_page_id = parent_page->GetChild(parent_child_idx + 1);
      auto prev_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(prev_page_id));
      auto next_page = reinterpret_cast<BTreeNodePage*>(buffer_pool_manager->FetchPage(next_page_id));
      auto prev_entries = prev_page->GetCurrentEntries();
      auto entries = GetCurrentEntries();
      auto next_entries = next_page->GetCurrentEntries();

      SetCurrentEntries(entries - 1);
      if (!need_merge(prev_entries, entries)) {
        if (prev_entries > next_entries) {
          // redistribute between left and current
          return redistribute(prev_page, this, parent_page, parent_child_idx - 1);
        } else {
          // redistribute between current and right
          return redistribute(this, next_page, parent_page, parent_child_idx - 1);
        }
      } else if (!need_merge(next_entries, entries)) {
        // redistribute between current and right
        return redistribute(this, next_page, parent_page, parent_child_idx - 1);
      } else if (prev_entries < next_entries) {
        // merge left and current
        s = merge(prev_page, this, parent_page, parent_child_idx - 1);
        assert(s.ok());
        return parent_page->index_remove(buffer_pool_manager, parent_child_idx - 1, parent_child_idx, root);
      } else {
        // merge current and right
        s = merge(this, next_page, parent_page, parent_child_idx - 1);
        assert(s.ok());
        return parent_page->index_remove(buffer_pool_manager, parent_child_idx - 1, parent_child_idx, root);
      }
    }
  }
  return Status::NotSupported();
}

Status BTreeNodePage::merge(BTreeNodePage *left, BTreeNodePage *right, BTreeNodePage *parent, int borrowed_key_idx) {
  assert(left != nullptr && right != nullptr && parent != nullptr);
  auto entries = left->GetCurrentEntries();
  auto left_key_start = left->KeyPosStart();
  auto left_child_start = left->ChildPosStart();
  auto right_entries = right->GetCurrentEntries();
  left_key_start[entries] = parent->GetKey(borrowed_key_idx);
  memmove(left_key_start + entries + 1, right->KeyPosStart(), sizeof(int64_t) * right_entries);
  memmove(left_child_start + entries + 1, right->ChildPosStart(), sizeof(page_id_t) * (right_entries + 1));
  left->SetCurrentEntries(entries + 1 + right_entries);
  return Status::OK();
}

// TODO: 足够通用吗
// 要分方向的
Status BTreeNodePage::redistribute(BTreeNodePage *left, BTreeNodePage *right, BTreeNodePage *parent, int key_idx) {
  assert(left != nullptr && right != nullptr && parent != nullptr);
  auto entries_left = left->GetCurrentEntries();
  auto entries_right = right->GetCurrentEntries();
  if (entries_left < entries_right) {
    // case redistribute-2
    auto redistribute_cnt = get_redistribute_cnt(entries_left, entries_right);
    SPDLOG_INFO("entry_left = {}, entry_right = {}, redistribute_cnt = {}", entries_left, entries_right, redistribute_cnt);
    // append keys to left
    auto left_key_start = left->KeyPosStart();
    auto right_key_start = right->KeyPosStart();
    auto after_redis_right_cnt = entries_right - redistribute_cnt;
    auto replaced_key = right->GetKey(redistribute_cnt - 1);
    // borrow parent's key_idx key
    // NOTE: important
    left_key_start[entries_left] = parent->GetKey(key_idx);
    memmove(left_key_start + entries_left + 1, right_key_start, (redistribute_cnt - 1) * sizeof(int64_t));

    // append children to left
    auto left_child_start = left->ChildPosStart();
    auto right_child_start = right->ChildPosStart();
    memmove(left_child_start + entries_left + 1, right_child_start, redistribute_cnt * sizeof(page_id_t));

    // move right page's data
    memmove(right_key_start, right_key_start + redistribute_cnt, sizeof(int64_t) * after_redis_right_cnt);
    memmove(right_child_start, right_child_start + redistribute_cnt, sizeof(page_id_t) * (after_redis_right_cnt + 1));

    // replace key
    parent->KeyPosStart()[key_idx] = replaced_key;
    right->SetCurrentEntries(after_redis_right_cnt);
    left->SetCurrentEntries(entries_left + redistribute_cnt);
  } else if (entries_right < entries_left) {
    // case redistribute-2
    auto redistribute_cnt = get_redistribute_cnt(entries_right, entries_left);
    SPDLOG_INFO("move left to right, parent key_idx {}", key_idx);
    SPDLOG_INFO("entry_left = {}, entry_right = {}, redistribute_cnt = {}", entries_left, entries_right, redistribute_cnt);
    // left 满了，需要向right redistribute
    auto left_key_start = left->KeyPosStart();
    auto right_key_start = right->KeyPosStart();
    auto after_redis_left_cnt = entries_left - redistribute_cnt;
    auto replaced_key = left ->GetKey(after_redis_left_cnt);
    // borrow parent's key_idx key
    // NOTE: important
    assert(redistribute_cnt >= 1);
    right_key_start[redistribute_cnt - 1] = parent->GetKey(key_idx);
    // 右结点腾出位置给左结点分配过来的结点
    memmove(right_key_start + redistribute_cnt, right_key_start, entries_right * sizeof(int64_t));

    // prepend children to right
    auto left_child_start = left->ChildPosStart();
    auto right_child_start = right->ChildPosStart();
    // 需要搬离的第一个结点不要移动
    memmove(right_key_start, left_key_start + after_redis_left_cnt + 1, (redistribute_cnt - 1) * sizeof(int64_t));

    // move right page's data
    // order matters
    // first
    memmove(right_child_start + redistribute_cnt, right_child_start, sizeof(page_id_t) * (entries_right + 1));
    // second
    memmove(right_child_start, left_child_start + after_redis_left_cnt + 1, redistribute_cnt * sizeof(page_id_t));

    // replace key
    parent->KeyPosStart()[key_idx] = replaced_key;
    left->SetCurrentEntries(after_redis_left_cnt);
    right->SetCurrentEntries(entries_right + redistribute_cnt);

  }
  return Status::OK();
}

void BTreeNodePage::debug_page(BTreeNodePage *page) {
  printf("---------------------------------------\n");
  printf("----------page_id: %2d-----------------\n", page->GetPageID());
  printf("-----------entries: %2d----------------\n", page->GetCurrentEntries());
  auto entries = page->GetCurrentEntries();
  for (int i = 0; i < entries; i++) {
    printf("   %d", page->GetKey(i));
  }
  printf("\n");
  for (int i = 0; i <= entries; i++) {
    printf("   %d", page->GetChild(i));
  }
  printf("\n----------------------------------------\n");
  fflush(stdout);
}

// 计算重新分布，right node 可一分配的entries cnt，尽量保证数量相等
int BTreeNodePage::get_redistribute_cnt(int entries_small, int entries_big) {
  assert(entries_small <= entries_big);
  int avg = entries_small + (entries_big - entries_small)  / 2;
  assert(avg >= MAX_DEGREE / 2);
  return entries_big - avg;
}

bool BTreeNodePage::need_merge(int entries_left, int entries_right) const {
  auto min_entries = MAX_DEGREE / 2;
  return entries_left + entries_right - min_entries < min_entries;
}

Status BTreeNodePage::find_child_index(int child_page_id, int *result) {
  assert(!IsLeafNode());
  auto entries = GetCurrentEntries();
  for (int i = 0; i <= entries; i++) {
    auto cld_page_id = GetChild(i);
    if (cld_page_id == child_page_id) {
      *result = i;
      return Status::OK();
    }
  }
  return Status::NotFound();
}

void BTreeNodePage::init(BTreeNodePage* dst, int degree, int n, page_id_t page_id, bool is_leaf) {
  dst->SetPageID(page_id);
  dst->SetDegree(degree);
  dst->SetCurrentEntries(n);
  dst->SetLeafNode(is_leaf);
  // leaf 节点需要设置available
  if (is_leaf) {
    dst->SetAvailable(options_.page_size - LEAF_HEADER_SIZE);
    dst->SetNextPageID(INVALID_PAGE_ID);
    dst->SetPrevPageID(INVALID_PAGE_ID);
  }
  dst->SetParentPageID(INVALID_PAGE_ID);
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

bool BTreeNodePage::keys_equals(std::initializer_list<page_id_t> results) {
  if (GetCurrentEntries() != results.size()) {
    return false;
  }
  auto it = results.begin();
  for (int i = 0; i < results.size() && it != results.end(); i++, it++) {
    if (GetKey(i) != *it) {
      return false;
    }
  }
  return true;
}
}

