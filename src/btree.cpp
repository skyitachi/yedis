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
  if (index_root_ != nullptr) {
    // 判断应该在哪个叶子节点上
  }
  auto s = leaf_root_->add(reinterpret_cast<const byte*>(key.data()), key.size(),
                         reinterpret_cast<const byte*>(value.data()), value.size());
  if (s.IsNoSpace()) {
    // 处理分裂
  }
}

Status BTree::read(const Slice &key, std::string *value) {
  return leaf_root_->read(reinterpret_cast<const byte*>(key.data()), value);
}

Status BTree::init() {
  // read meta
  page_id_t meta_page_id;
  meta_ = reinterpret_cast<BTreeMetaPage*>(yedis_instance_->buffer_pool_manager->NewPage(&meta_page_id));

  auto levels = meta_->GetLevels();
  auto root_page_id_ = meta_->GetRootPageId();
  if (levels > 1) {
    index_root_ = reinterpret_cast<BTreeIndexNodePage*>(yedis_instance_->buffer_pool_manager->FetchPage(root_page_id_));
    spdlog::info("tree degree {}", leaf_root_->GetDegree());
    spdlog::info("current root node is index node");
  } else {
    page_id_t root_page_id;
    leaf_root_ = reinterpret_cast<BTreeLeafNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&root_page_id));
    if (root_page_id != leaf_root_->GetPageID()) {
      spdlog::error("open btree failed due to non zero root page id {}", leaf_root_->GetPageID());
    }
    if (leaf_root_->GetDegree() == 0) {
      leaf_root_->init(MAX_DEGREE, root_page_id);
    } else {
      leaf_root_->init(leaf_root_->GetDegree(), root_page_id);
    }
  }

  spdlog::info("open btree successfully with page_id {}", root_page_id_);

  return Status::OK();
}

Status BTree::destroy() {
  yedis_instance_->disk_manager->Destroy();
  return Status::OK();
}

}
