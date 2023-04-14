//
// Created by Shiping Yao on 2023/4/12.
//

#ifndef YEDIS_TABLE_FORMAT_H
#define YEDIS_TABLE_FORMAT_H

#include <cstdint>

#include "common/status.h"

namespace yedis {

class FileHandle;
class ReadOptions;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

class BlockHandle {
public:
  // Maximum encoding length of a BlockHandle
  enum: uint8_t { kMaxEncodedLength = 10 + 10 };

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

class Footer {
public:
  enum: uint8_t { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

  Footer() = default;

  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  const BlockHandle& index_handle() const { return index_handle_; }
  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

private:
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
};

static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
};

Status ReadBlock(FileHandle* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result);

inline BlockHandle::BlockHandle(): offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}
}
#endif //YEDIS_TABLE_FORMAT_H
