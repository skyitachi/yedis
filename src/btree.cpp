//
// Created by skyitachi on 2020/8/20.
//
#include <spdlog/spdlog.h>

#include <btree.hpp>
#include <buffer_pool_manager.hpp>

namespace yedis {

Status BTreeNode::add(byte *key, byte *value) {
  return Status::NotSupported();
}

Status BTree::init() {
  page_id_t root_page_id;
  root = reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&root_page_id));
  spdlog::info("open btree successfully with page_id {}", root_page_id);
  return Status::OK();
}
}
