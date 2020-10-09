//
// Created by skyitachi on 2020/8/22.
//
#include <buffer_pool_manager.hpp>
#include <spdlog/spdlog.h>
#include <utility>
namespace yedis {

BufferPoolManager::BufferPoolManager(size_t pool_size, YedisInstance* yedis_instance) {
  assert(pool_size != 0);
  pool_size_ = pool_size;
  pages_ = new Page[pool_size];
  yedis_instance_ = yedis_instance;
}

BufferPoolManager::~BufferPoolManager() {
  delete []pages_;
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  spdlog::info("FetchPage current_index_ {}, page_id {}", current_index_, page_id);
  auto it = records_.find(page_id);
  if (it != records_.end()) {
    // 当前page在内存中
    SPDLOG_INFO("page_id: {} is current_index {}", page_id, it->second);
    return &pages_[it->second];
  }
  records_.insert(std::make_pair(page_id, current_index_));
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

Status BufferPoolManager::Flush() {
  for (int i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()) {
      spdlog::info("page index {} is dirty", i);
      yedis_instance_->disk_manager->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    }
  }
  return Status::OK();
}
}

