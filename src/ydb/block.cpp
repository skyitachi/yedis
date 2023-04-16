//
// Created by Shiping Yao on 2023/4/16.
//

#include "block.h"
#include "table_format.h"
#include "util.hpp"

namespace yedis {

Block::Block(const yedis::BlockContents &contents) {
  size_ = contents.data.size();
  data_ = contents.data.data();
  owned_ = contents.heap_allocated;
  restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
}

Block::~Block() {
  if (owned_) {
    delete data_;
  }
}

uint32_t Block::NumRestarts() const {
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

class Block::Iter : public Iterator {
};

}
