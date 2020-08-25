//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_PAGE_HPP_
#define YEDIS_INCLUDE_PAGE_HPP_
#include <cstring>
#include <iostream>
#include "config.hpp"

namespace yedis {
  class Page {
   public:
    Page() {ResetMemory();}
    ~Page() = default;

    inline char *GetData() { return data_; }

    inline bool IsDirty() { return is_dirty_; }
    void SetIsDirty(bool is_dirty) {
      is_dirty_ = is_dirty;
    }

    page_id_t GetPageId() const {
      return page_id_;
    }

   protected:
    static constexpr size_t OFFSET_PAGE_START = 0;

    page_id_t page_id_ = INVALID_PAGE_ID;
   public:
   private:
    inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE);}
    char data_[PAGE_SIZE]{};
    bool is_dirty_ = false;
  };
}
#endif //YEDIS_INCLUDE_PAGE_HPP_
