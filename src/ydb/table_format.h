//
// Created by Shiping Yao on 2023/4/12.
//

#ifndef YEDIS_TABLE_FORMAT_H
#define YEDIS_TABLE_FORMAT_H

#include <cstdint>

#include "common/status.h"

namespace yedis {

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

class BlockHandle {
public:
  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

  BlockHandle();

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

private:
  uint64_t offset_;
  uint64_t size_;

};
}
#endif //YEDIS_TABLE_FORMAT_H
