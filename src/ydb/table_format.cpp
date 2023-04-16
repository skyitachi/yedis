//
// Created by Shiping Yao on 2023/4/12.
//

#include "table_format.h"
#include "util.hpp"
#include "fs.hpp"
#include "options.h"
#include "exception.h"
#include "common/checksum.h"

namespace yedis {

void BlockHandle::EncodeTo(std::string *dst) const {
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  }
  return Status::Corruption("bad block handle");
}

void Footer::EncodeTo(std::string *dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void) original_size; // disble unused variable warning.
}

Status Footer::DecodeFrom(Slice *input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadBlock(FileHandle* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  file->Read(buf, n + kBlockTrailerSize, handle.offset());
  Slice contents{buf, n + kBlockTrailerSize};
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  const char* data = contents.data();

  // compression type
  auto cType = static_cast<CompressionType>(buf[n]);
  if (cType != kNoCompression)
    return Status::Corruption("unexpected compression type");

  if (options.verify_checksums) {
    uint32_t crc = crc32::Value((uint8_t *) buf, n);
    crc = crc32::Extend(crc, reinterpret_cast<uint8_t *>(buf + n), 1);  // Extend crc to cover block type
    crc = crc32::Mask(crc);
    if (crc != DecodeFixed32(data + n + 1)) {
      return Status::Corruption("unexpected checksum");
    }
  }
  result->data = Slice(buf, n);
  result->heap_allocated = true;
  // TODO: what's the meaning of cacheable
  result->cachable = true;
  return Status::OK();
}

}
