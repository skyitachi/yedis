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

  data_ptr_t Writer::header() {
    return file_buffer_->buffer;
  }

  unsigned short Writer::size() {
    return block_offset_;
  }

  int Writer::available() {
    return kBlockSize - block_offset_ - kHeaderSize;
  }

  void Writer::reset_block_offset() {
    block_offset_ = 0;
  }

  Status Writer::AddRecord(const Slice &slice) {
    int64_t sz = slice.size();
    bool begin = true;
    int offset = 0;
    RecordType t;
    while (offset < sz) {
      int left = available();
      bool end = false;
      if ((sz - offset) <= left) {
        end = true;
        left = (sz - offset);
      }
      memcpy(data(), slice.data() + offset, left);
      offset += left;
      block_offset_ += left + kHeaderSize;
      int leftover = kBlockSize - size();
      if (leftover <= kHeaderSize) {
        // padding
        memcpy(data() + left, Slice("\0\0\0\0\0\0\0", leftover).data(), leftover);
        left += leftover;
        block_offset_ += leftover;
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
      file_buffer_->size = left + kHeaderSize;
      file_buffer_->Append(handle_);
      begin = false;
      if (block_offset_ == kBlockSize) {
        block_offset_ = 0;
      }
    }
    return Status::OK();
  }

  Status Writer::AddRecord(const std::string_view sv) {
    return AddRecord(Slice(sv));
  }

  Reader::Reader(FileHandle &handle, uint64_t initial_offset): handle_(handle), offset_(initial_offset) {}

  Reader::~Reader() {
    handle_.Close();
  }

  bool Reader::ReadRecord(Slice *record, std::string *scratch) {
    uint64_t size = kHeaderSize;
    char header_buf[kHeaderSize];
    RecordType t;
    int64_t sz;
    do {
      t = DecodeFixed<RecordType>(header_buf + kCheckSumSize + sizeof(uint16_t));
      scratch->reserve(size + kHeaderSize);
      sz = handle_.Read(header_buf, kHeaderSize, offset_);
      if (sz != kHeaderSize) {
        return false;
      }
      scratch->append(header_buf);
      uint16_t data_sz = DecodeFixed<uint16_t>(header_buf + kCheckSumSize);
      size += kHeaderSize;
      offset_ += kHeaderSize;
      sz = handle_.Read(scratch->data() + size, data_sz, offset_);
      if (sz != data_sz) {
        return false;
      }
      // TODO: verify checksum
    } while (t != RecordType::kLastType & t != RecordType::kFullType);
    *record = Slice(*scratch);
    return true;
  }
}