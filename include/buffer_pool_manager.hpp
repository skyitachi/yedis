//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_
#define YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_

#include <unordered_map>
#include <vector>
#include <spdlog/spdlog.h>
#include <list>

#include "page.hpp"
#include "yedis.hpp"
#include "disk_manager.hpp"

#include "option.hpp"

namespace yedis {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t pool_size, YedisInstance* yedis_instance);
  BufferPoolManager(size_t pool_size, YedisInstance* yedis_instance, BTreeOptions options);

  ~BufferPoolManager();

  // no need use impl pattern
  Page *FetchPage(page_id_t page_id);

  Page *NewPage(page_id_t* page_id);

  Status Flush();

  // debug
  std::vector<page_id_t> GetAllBTreePageID() {
    auto ret = std::vector<page_id_t>();
    for (auto [page_id, _]: records_) {
      if (page_id != 0) {
        SPDLOG_INFO("memory recorded page_id {}", page_id);
        ret.push_back(page_id);
      }
    }
    return ret;
  }

  void FlushPage(Page* page);
 private:
  size_t pool_size_;
  YedisInstance* yedis_instance_;
  Page* pages_;
  int current_index_ = 0;
  std::unordered_map<page_id_t, int> records_;
  void *raw_memory_;
  std::list<Page*> free_list_;
  std::list<Page*> using_list_;
  std::unordered_map<page_id_t, std::list<Page*>::iterator> lru_records_;
};
}
#endif //YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_
