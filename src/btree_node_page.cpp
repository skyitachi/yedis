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

}

