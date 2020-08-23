//
// Created by skyitachi on 2020/8/22.
//
#include <buffer_pool_manager.hpp>
#include <spdlog/spdlog.h>
namespace yedis {
BufferPoolManager::BufferPoolManager(size_t pool_size, YedisInstance* yedis_instance) {
  pages_ = new Page[pool_size];
  yedis_instance_ = yedis_instance;
}

BufferPoolManager::~BufferPoolManager() {
  delete []pages_;
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  spdlog::info("FetchPage current_index_ {}", current_index_);
  Page *next_page = &pages_[current_index_];
  yedis_instance_->disk_manager->ReadPage(page_id, next_page->GetData());
  auto next_index = (current_index_ + 1) % pool_size_;
  current_index_ = next_index;
  return next_page;
}

Page* BufferPoolManager::NewPage(page_id_t *page_id) {
  *page_id = yedis_instance_->disk_manager->AllocatePage();
  spdlog::info("NewPage page_id: {}", *page_id);
  return FetchPage(*page_id);
}
}

