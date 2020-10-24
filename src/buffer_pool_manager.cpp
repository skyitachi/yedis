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
  raw_memory_ = operator new(pool_size * sizeof(Page));
  pages_ = reinterpret_cast<Page*>(raw_memory_);
  for (int i = 0; i < pool_size; i++) {
    new(&pages_[i])Page();
  }
  yedis_instance_ = yedis_instance;
}

BufferPoolManager::BufferPoolManager(size_t pool_size, YedisInstance* yedis_instance, BTreeOptions options) {
  assert(pool_size != 0);
  pool_size_ = pool_size;
  raw_memory_ = operator new(pool_size * sizeof(Page));
  // placement new initialize
  pages_ = reinterpret_cast<Page*>(raw_memory_);
  for (int i = 0; i < pool_size; i++) {
    new(&pages_[i])Page(options);
  }
  yedis_instance_ = yedis_instance;
}

BufferPoolManager::~BufferPoolManager() {
  for (int i = 0; i < pool_size_; i++) {
    pages_[i].~Page();
  }
  operator delete(raw_memory_);
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  SPDLOG_INFO("FetchPage page_id: {}", page_id);
  auto it = records_.find(page_id);
  if (it != records_.end()) {
    // 当前page在内存中
    return &pages_[it->second];
  }
  records_.insert(std::make_pair(page_id, current_index_));
  Page *next_page = &pages_[current_index_];
  SPDLOG_INFO("before read page: {}, current_index_: {}", next_page->GetPageId(), current_index_);
  yedis_instance_->disk_manager->ReadPage(page_id, next_page->GetData());
  auto next_index = (current_index_ + 1) % pool_size_;
  current_index_ = next_index;
  next_page->SetPageID(page_id);
  SPDLOG_INFO("after read page, page_id: {}, page_size: {}", next_page->GetPageId(), next_page->getPageSize());

  return next_page;
}

Page* BufferPoolManager::NewPage(page_id_t *page_id) {
  *page_id = yedis_instance_->disk_manager->AllocatePage();
  SPDLOG_INFO("NewPage page_id: {}", *page_id);
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

