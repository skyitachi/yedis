//
// Created by skyitachi on 2020/8/20.
//
#include <spdlog/spdlog.h>

#include <btree.hpp>
#include <buffer_pool_manager.hpp>
#include <btree_node_page.hpp>

namespace yedis {

Status BTree::add(const Slice &key, const Slice& value) {
  return root->add(reinterpret_cast<const byte*>(key.data()), key.size(),
            reinterpret_cast<const byte*>(value.data()), value.size());
}

Status BTree::read(const Slice &key, std::string *value) {
  return root->read(reinterpret_cast<const byte*>(key.data()), value);
}

Status BTree::init() {
  page_id_t root_page_id;
  root = reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&root_page_id));
  if (root_page_id != root->GetPageID()) {
    spdlog::error("open btree failed due to non zero root page id {}", root->GetPageID());
  }
  spdlog::info("open btree successfully with page_id {}", root_page_id);
  spdlog::info("tree degree {}", root->GetDegree());

  if (root->GetDegree() == 0) {
    root->init(MAX_DEGREE, root_page_id);
  } else {
    root->init(root->GetDegree(), root_page_id);
  }
  return Status::OK();
}

Status BTree::destroy() {
  yedis_instance_->disk_manager->Destroy();
  return Status::OK();
}

}
