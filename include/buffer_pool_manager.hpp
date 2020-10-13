//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_
#define YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_

#include <unordered_map>

#include "page.hpp"
#include "yedis.hpp"
#include "disk_manager.hpp"

namespace yedis {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t pool_size, YedisInstance* yedis_instance);

  ~BufferPoolManager();

  // no need use impl pattern
  Page *FetchPage(page_id_t page_id);

  Page *NewPage(page_id_t* page_id);

  Status Flush();
 private:
  size_t pool_size_;
  YedisInstance* yedis_instance_;
  Page* pages_;
  int current_index_ = 0;
  std::unordered_map<page_id_t, int> records_;
};
}
#endif //YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_
