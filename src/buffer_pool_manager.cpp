//
// Created by skyitachi on 2020/8/22.
//
#include <buffer_pool_manager.hpp>

namespace yedis {
BufferPoolManager::BufferPoolManager(size_t pool_size) {
  pages_ = new Page[pool_size];
}

BufferPoolManager::~BufferPoolManager() {
  delete []pages_;
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  // TODO: just placeholder
  return &pages_[0];
}

Page* BufferPoolManager::NewPage(page_id_t *page_id) {
  // TODO: new page
}
}

