//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_PAGE_HPP_
#define YEDIS_INCLUDE_PAGE_HPP_
#include <cstring>
#include <iostream>
#include "config.hpp"
#include "util.hpp"

#include "option.hpp"

namespace yedis {
  class Page {
   public:
    Page(const BTreeOptions& options): options_(options) {
      data_ = (char *)malloc(options.page_size * sizeof(char));
      ResetMemory();
    }
    Page(): Page(BTreeOptions{}) {}
    ~Page() = default;

    inline char *GetData() { return data_; }

    inline bool IsDirty() { return is_dirty_; }

    void SetIsDirty(bool is_dirty) {
      is_dirty_ = is_dirty;
    }

    auto GetPageId() const {
      return page_id_;
    }

    void SetPageID(page_id_t page_id) {
      page_id_ = page_id;
      EncodeFixed32(GetData(), page_id);
    }

    inline uint32_t getPageSize() {
      return options_.page_size;
    }

   protected:
    static constexpr size_t OFFSET_PAGE_START = 0;

    page_id_t page_id_ = INVALID_PAGE_ID;

    BTreeOptions options_;
   private:
    inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, options_.page_size);}
    char *data_;
    bool is_dirty_ = false;
  };
}
#endif //YEDIS_INCLUDE_PAGE_HPP_
