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
    int written = 0;
    bool begin = true;
    RecordType t;
    while (sz > 0) {
      int left = available();
      bool end = sz <= left;
      int64_t w = end ? sz : left;

      if (available() <= kHeaderSize) {
        // TODO: padding
        reset_block_offset();
      }

      if (begin && end) {
        t = RecordType::kFullType;
      } else if (end) {
        t = RecordType::kLastType;
      } else if (begin) {
        t = RecordType::kFirstType;
      } else {
        t = RecordType::kMiddleType;
      }
      memcpy(data() + kHeaderSize, slice.data() + written, w);
      sz -= w;
      written += w;
      // TODO: encode crc32
      EncodeFixed<uint16_t>(data(), w);
      EncodeFixed<RecordType>(data() + kBlockLenSize, t);
      block_offset_ += w + kHeaderSize;


      // write to disk
      file_buffer_->Append(handle_);


      begin = false;

      if (end) {
        reset_block_offset();
      }
    }
    return Status::OK();
  }
}