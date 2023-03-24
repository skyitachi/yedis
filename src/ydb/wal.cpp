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
  Writer::Writer(FileHandle &handle): handle_(handle), block_offset_(0) {
    Allocator& allocator = Allocator::DefaultAllocator();
    file_buffer_ = std::make_unique<FileBuffer>(allocator, FileBufferType::BLOCK, kBlockSize);
  }
  Writer::~Writer() {
    handle_.Close();
  }

  data_ptr_t Writer::data() {
    return file_buffer_->buffer + block_offset_;
  }

  unsigned short Writer::size() {
    return block_offset_;
  }

  int Writer::available() {
    return kBlockSize - block_offset_;
  }

  void Writer::reset_block_offset() {
    block_offset_ = kHeaderSize;
  }

  Status Writer::AddRecord(const Slice &slice) {
    int64_t sz = slice.size();
    bool begin = true;
    int offset = 0;
    RecordType t;
    while (offset < sz) {
      int left = available();
      bool end = false;
      memcpy(data(), slice.data() + offset, left);
      block_offset_ += left;
      if (sz <= left) {
        end = true;
      }
      offset += left;
      int leftover = kBlockSize - available();
      if (leftover < kHeaderSize) {
        // padding
        memcpy(data() + left, Slice("\0\0\0\0\0\0\0", leftover).data(), leftover);
      }
      if (begin && end) {
        t = RecordType::kFullType;
      } else if (begin) {
        t = RecordType::kFirstType;
      } else if (end) {
        t = RecordType::kLastType;
      } else {
        t = RecordType::kMiddleType;
      }
      EncodeFixed<uint16_t>(header(), left);
      EncodeFixed<RecordType>(header() + kBlockLenSize, t);
      file_buffer_->Append(handle_);

      begin = false;
      block_offset_ = kHeaderSize;
    }
    return Status::OK();
  }
}