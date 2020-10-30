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
  for (int i = 0; i < pool_size; i++) {
    free_list_.push_back(new Page());
  }
  yedis_instance_ = yedis_instance;
}

BufferPoolManager::BufferPoolManager(size_t pool_size, YedisInstance* yedis_instance, BTreeOptions options) {
  assert(pool_size != 0);
  pool_size_ = pool_size;
  for (int i = 0; i < pool_size_; i++) {
    free_list_.push_back(new Page(options));
  }
  yedis_instance_ = yedis_instance;
}

BufferPoolManager::~BufferPoolManager() {
  for (auto &it: using_list_) {
    delete it;
  }
  for (auto &it: free_list_) {
    delete it;
  }
}

// lru fetch
Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  Page *next_page = nullptr;
  // fetch from lru records first
  auto it = lru_records_.find(page_id);
  if (it != lru_records_.end()) {
    // 放到队首
    next_page = *it->second;
    if (using_list_.front() == next_page) {
      SPDLOG_INFO("no need to reinsert");
      return next_page;
    }
    using_list_.erase(it->second);
    using_list_.push_front(next_page);
    return next_page;
  }
  if (!free_list_.empty()) {
    next_page = free_list_.front();
    free_list_.pop_front();
    using_list_.push_front(next_page);
    auto first = using_list_.begin();
    lru_records_.insert(std::make_pair(page_id, first));
    yedis_instance_->disk_manager->ReadPage(page_id, next_page->GetData());
    next_page->SetPageID(page_id);
    SPDLOG_INFO("using_list_ first: page_id {}", (*first)->GetPageId());
  } else if (!using_list_.empty()) {
    auto it = lru_records_.find(page_id);
    if (it == lru_records_.end()) {
      // 需要淘汰
      auto least_used_page = using_list_.back();
      using_list_.pop_back();
      lru_records_.erase(least_used_page->GetPageId());
      SPDLOG_INFO("least used page {}, out memory", least_used_page->GetPageId());
      FlushPage(least_used_page);
      next_page = least_used_page;
      using_list_.push_front(next_page);
      auto first = using_list_.begin();
      lru_records_.insert(std::make_pair(page_id, first));
      yedis_instance_->disk_manager->ReadPage(page_id, next_page->GetData());
      next_page->SetPageID(page_id);
    }
  } else {
    SPDLOG_ERROR("no enough page");
    return nullptr;
  }
  return next_page;
}

void BufferPoolManager::Pin(Page *page) {
  assert(page != nullptr);
  Pin(page->GetPageId());
}

// TODO: make sure pin and unpin logic
void BufferPoolManager::Pin(page_id_t page_id) {
  if (pinned_records_.find(page_id) != pinned_records_.end()) {
    return;
  }
  auto it = lru_records_.find(page_id);
  assert(it != lru_records_.end());
  using_list_.erase(it->second);
  lru_records_.erase(it->first);
  pinned_records_.insert(std::make_pair(it->first, *it->second));
}

void BufferPoolManager::UnPin(Page *page) {
  assert(page != nullptr);
  UnPin(page->GetPageId());
}

void BufferPoolManager::UnPin(page_id_t page_id) {
  auto it = pinned_records_.find(page_id);
  if (it == pinned_records_.end()) {
    return;
  }
  using_list_.push_front(it->second);
  lru_records_.insert(std::make_pair(page_id, using_list_.begin()));
}

// lru
Page* BufferPoolManager::NewPage(page_id_t *page_id) {
  *page_id = yedis_instance_->disk_manager->AllocatePage();
  SPDLOG_INFO("NewPage page_id: {}", *page_id);
  return FetchPage(*page_id);
}

Status BufferPoolManager::Flush() {
  for (auto &it: using_list_) {
    FlushPage(it);
  }
  for (auto &[page_id, page]: pinned_records_) {
    FlushPage(page);
  }
  return Status::OK();
}

void BufferPoolManager::FlushPage(Page *page) {
  if (page->IsDirty()) {
    SPDLOG_INFO("flush_page page_id {}", page->GetPageId());
    yedis_instance_->disk_manager->WritePage(page->GetPageId(), page->GetData());
    page->ResetMemory();
  } else {
    SPDLOG_INFO("no need to flush: {}", page->GetPageId());
  }
}
}

