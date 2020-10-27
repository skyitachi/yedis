//
// Created by skyitachi on 2020/8/20.
//
#include <spdlog/spdlog.h>

#include <btree.hpp>
#include <buffer_pool_manager.hpp>
#include <btree_leaf_node_page.hpp>
#include <btree_meta_page.hpp>
#include <stack>


namespace yedis {

Status BTree::add(int64_t key, const Slice& value) {
  Status s;
  auto origin_root = root_;
  s = root_->add(yedis_instance_->buffer_pool_manager, key, reinterpret_cast<const byte *>(value.data()), value.size(), &root_);
  if (!s.ok()) {
    return s;
  }
  if (origin_root != root_) {
    // root有更新, 代表level + 1
    meta_->SetLevels(meta_->GetLevels() + 1);
    // 更新root page
    meta_->SetRootPageId(root_->GetPageID());
    SPDLOG_INFO("update meta info successfully");
  }
  return s;
}

Status BTree::read(int64_t key, std::string *value) {
  return root_->read(yedis_instance_->buffer_pool_manager, key, value);
}

Status BTree::init() {
  // read meta
  page_id_t meta_page_id;
  meta_ = reinterpret_cast<BTreeMetaPage*>(yedis_instance_->buffer_pool_manager->NewPage(&meta_page_id));

  meta_->SetPageID(meta_page_id);
  auto levels = meta_->GetLevels();
  if (levels == 0) {
    // initial state
    spdlog::info("init btree with empty state");
    page_id_t root_page_id;
    root_ = reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&root_page_id));
    spdlog::debug("init empty btree root_page_id: {}", root_page_id);
    root_->SetPageID(root_page_id);
    spdlog::debug("GetPageID from BTreeNodeMethod: {}", root_->GetPageID());
    meta_->SetRootPageId(root_page_id);
    meta_->SetLevels(1);
    spdlog::debug("meta_ level: {}", meta_->GetLevels());
    root_->init(get_degree(options_.page_size), root_page_id);
    return Status::OK();
  }
  auto root_page_id = meta_->GetRootPageId();
  spdlog::debug("init btree with exist tree file: meta_page_id {}, root_page_id {}", meta_page_id, root_page_id);
  root_ = reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->FetchPage(root_page_id));
  if (root_page_id != root_->GetPageID()) {
    spdlog::error("open btree failed due to non zero root page id {}", root_->GetPageID());
  }
  root_->init(root_->GetDegree(), root_page_id);

  spdlog::info("open btree successfully with page_id {}", root_page_id);
  return Status::OK();
}

Status BTree::destroy() {
  yedis_instance_->disk_manager->Destroy();
  return Status::OK();
}

page_id_t BTree::GetFirstLeafPage() {
  auto root = meta_->GetRootPageId();
  auto root_page = reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->FetchPage(root));
  if (root_page->IsLeafNode()) {
    return root;
  }
  assert(root_page->GetCurrentEntries() > 0);
  return root_page->GetChild(0);
}

std::vector<page_id_t> BTree::GetAllLeavesByPointer() {
  auto first = GetFirstLeafPage();
  assert(first != INVALID_PAGE_ID);
  auto ret = std::vector<page_id_t>();
  for (auto it = get_page(first); it != nullptr;) {
    ret.push_back(it->GetPageID());
    auto next_page_id = it->GetNextPageID();
    if (next_page_id == INVALID_PAGE_ID) {
      break;
    }
    it = get_page(next_page_id);
  }
  return ret;
}

// none recursive dfs
// 多层级的如何测试
std::vector<page_id_t> BTree::GetAllLeavesByIterate() {
  auto ret = std::vector<page_id_t>();
  std::stack<page_id_t> next;
  next.push(meta_->GetRootPageId());
  while(!next.empty()) {
    auto cur_page_id = next.top();
    next.pop();
    auto it = get_page(cur_page_id);
    auto entries = it->GetCurrentEntries();
    auto child_start = it->ChildPosStart();
    assert(entries > 0);
    auto first_child = get_page(child_start[0]);
    if (first_child->IsLeafNode()) {
      for (auto i = 0; i <= entries; i++) {
        ret.push_back(child_start[i]);
      }
    } else {
      // index node, reverse push to next
      for (auto i = entries; i >= 0; i--) {
        next.push(child_start[i]);
      }

    }
  }
  return ret;
}

BTreeNodePage* BTree::get_page(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) { return nullptr;}
  return reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->FetchPage(page_id));
}

}
