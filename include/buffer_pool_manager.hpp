//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_
#define YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_

#include "page.hpp"

namespace yedis {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t pool_size);

  ~BufferPoolManager();

  // no need use impl pattern
  Page *FetchPage(page_id_t page_id);

  Page *NewPage(page_id_t* page_id);

 private:
  size_t pool_size_;

  Page* pages_;
};
}
#endif //YEDIS_INCLUDE_BUFFER_POOL_MANAGER_HPP_
