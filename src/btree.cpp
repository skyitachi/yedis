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

Status BTree::add(const Slice &key, const Slice& value) {
  if (root_ != nullptr) {
    // 判断应该在哪个叶子节点上
  }
  Status s;
  if (root_->IsLeafNode()) {
    auto leaf_root = reinterpret_cast<BTreeLeafNodePage*>(root_);
    s = leaf_root->add(reinterpret_cast<const byte*>(key.data()), key.size(),
                   reinterpret_cast<const byte*>(value.data()), value.size());
  } else {
    auto index_root = reinterpret_cast<BTreeIndexNodePage*>(root_);
    s = index_root->add(reinterpret_cast<const byte*>(key.data()), key.size(),
                   reinterpret_cast<const byte*>(value.data()), value.size());
  }
  if (s.IsNoSpace()) {
    // 处理分裂
  }
}

Status BTree::read(const Slice &key, std::string *value) {
  return root_->read(reinterpret_cast<const byte*>(key.data()), value);
}

Status BTree::init() {
  // read meta
  page_id_t meta_page_id;
  meta_ = reinterpret_cast<BTreeMetaPage*>(yedis_instance_->buffer_pool_manager->NewPage(&meta_page_id));

  auto levels = meta_->GetLevels();
  auto root_page_id = meta_->GetRootPageId();
  // TODO
  root_ = static_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->FetchPage(root_page_id));
  if (levels == 1) {
    if (root_page_id != root_->GetPageID()) {
      spdlog::error("open btree failed due to non zero root page id {}", leaf_root_->GetPageID());
    }
    if (root_->GetDegree() == 0) {
      root_->init(MAX_DEGREE, root_page_id);
    } else {
      root_->init(root_->GetDegree(), root_page_id);
    }
  }

  spdlog::info("open btree successfully with page_id {}", root_page_id);

  return Status::OK();
}

Status BTree::destroy() {
  yedis_instance_->disk_manager->Destroy();
  return Status::OK();
}

}
