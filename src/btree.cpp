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

Status BTree::init() {
  page_id_t root_page_id;
  root = reinterpret_cast<BTreeNodePage*>(yedis_instance_->buffer_pool_manager->NewPage(&root_page_id));
  spdlog::info("open btree successfully with page_id {}", root_page_id);
  root->init(MAX_DEGREE, root_page_id);
  return Status::OK();
}
}
