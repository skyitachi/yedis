//
// Created by skyitachi on 2020/8/20.
//
#include <spdlog/spdlog.h>

#include <btree.hpp>
#include <buffer_pool_manager.hpp>
#include <btree_leaf_node_page.hpp>
#include <btree_index_node_page.hpp>
#include <btree_meta_page.hpp>

namespace yedis {

Status BTree::add(int64_t key, const Slice& value) {
  if (root_ != nullptr) {
    // 判断应该在哪个叶子节点上
  }
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
  return root_->read(key, value);
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
    root_->init(MAX_DEGREE, root_page_id);
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

}
