//
// Created by Shiping Yao on 2023/3/14.
//
#include "common/status.h"
#include "fs.hpp"
#include "wal.h"
#include "log_format.h"
#include "allocator.h"
#include "util.hpp"

namespace yedis::wal {
  Writer::Writer(FileHandle &handle): handle_(handle), block_offset_(kHeaderSize) {
    Allocator& allocator = Allocator::DefaultAllocator();
    file_buffer_ = std::make_unique<FileBuffer>(allocator, FileBufferType::BLOCK, kBlockSize);
  }
  Writer::~Writer() {
    handle_.Close();
  }

  data_ptr_t Writer::data() {
    return file_buffer_->buffer + kTypeHeaderSize;
  }

  data_ptr_t Writer::header() {
    return file_buffer_->buffer;
  }
  unsigned short Writer::size() {
    return block_offset_ - kTypeHeaderSize;
  }

  int Writer::available() {
    return file_buffer_->size - block_offset_;
  }

  Status Writer::AddRecord(const Slice &slice) {
    int64_t sz = slice.size();
    int written = 0;
    bool is_begin = true;
    while (sz > 0) {
      int left = available();
      memcpy(data(), slice.data() + written, left);
      block_offset_ += left;
      written += left;
      if(sz <= left) {
        if (is_begin) {
          is_begin = false;
        }
        EncodeFixed<unsigned short>(header(), size());
        EncodeFixed<RecordType>(header() + kBlockLenSize, RecordType::kLastType);
      } else if (sz > left) {
        EncodeFixed<unsigned short>(header(), size());
        if (is_begin) {
          is_begin = false;
          EncodeFixed<RecordType>(header() + kBlockLenSize, RecordType::kFirstType);
        } else {
          EncodeFixed<RecordType>(header() + kBlockLenSize, RecordType::kMiddleType);
        }
      }
      sz -= left;
    }
    return Status::NotSupported("not implement");
  }
}