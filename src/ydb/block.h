//
// Created by Shiping Yao on 2023/4/16.
//

#ifndef YEDIS_BLOCK_H
#define YEDIS_BLOCK_H

#include "iterator.h"
#include <cstdint>

namespace yedis {
struct BlockContents;
class Comparator;

class Block{
public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

private:
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]
};
}
#endif //YEDIS_BLOCK_H
