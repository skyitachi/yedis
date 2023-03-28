//
// Created by Shiping Yao on 2023/3/14.
//
#include <spdlog/spdlog.h>
#include <crc32c/crc32c.h>

#include "common/status.h"
#include "fs.hpp"
#include "wal.h"
#include "log_format.h"
#include "allocator.h"
#include "util.hpp"
#include "common/checksum.h"
#include "exception.h"


namespace yedis::wal {

  static void InitTypeCrc(uint32_t* type_crc) {
    for (int i = 0; i <= kMaxRecordType; i++) {
      char t = static_cast<char>(i);
      type_crc[i] = crc32::Value(reinterpret_cast<uint8_t*>(&t), 1);
    }
  }

  Writer::Writer(FileHandle &handle): handle_(handle), block_offset_(0) {
    Allocator& allocator = Allocator::DefaultAllocator();
    file_buffer_ = std::make_unique<FileBuffer>(allocator, FileBufferType::BLOCK, kBlockSize);
    InitTypeCrc(type_crc_);
  }
  Writer::~Writer() {
    handle_.Close();
  }

  data_ptr_t Writer::data() {
    return file_buffer_->buffer + kHeaderSize;
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
      int fragment_length = left;
      bool end = false;
      if ((sz - offset) <= left) {
        end = true;
        left = (sz - offset);
        fragment_length = left;
      }
      memcpy(data(), slice.data() + offset, left);
      EncodeFixed<uint16_t>(header() + kCheckSumSize, left);

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
      uint32_t checksum = crc32::Extend(type_crc_[uint8_t(t)], data(), fragment_length);
      checksum = crc32::Mask(checksum);
      EncodeFixed<uint32_t>(header(), checksum);
      EncodeFixed<RecordType>(header() + kCheckSumSize + kBlockLenSize, t);

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

  Reader::Reader(FileHandle &handle, uint64_t initial_offset): handle_(handle), offset_(initial_offset) {
    InitTypeCrc(type_crc_);
  }

  Reader::~Reader() {
    handle_.Close();
  }

  bool Reader::ReadRecord(Slice *record, std::string *scratch) {
    char header_buf[kHeaderSize];
    RecordType t;
    int64_t sz;
    int64_t size = 0;
    do {
      sz = handle_.Read(header_buf, kHeaderSize, offset_);
      if (sz != kHeaderSize) {
        return false;
      }
      t = DecodeFixed<RecordType>(header_buf + kCheckSumSize + kBlockLenSize);
      auto checksum = DecodeFixed<uint32_t>(header_buf);
      auto data_sz = DecodeFixed<uint16_t>(header_buf + kCheckSumSize);
      offset_ += kHeaderSize;

      scratch->resize(size + data_sz);
      sz = handle_.Read(scratch->data() + size, data_sz, offset_);
      if (sz != data_sz) {
        return false;
      }
      auto compute = crc32::Extend(type_crc_[uint8_t(t)], reinterpret_cast<uint8_t*>(scratch->data()) + size, data_sz);
      compute = crc32::Mask(compute);
      if (compute != checksum) {
        spdlog::info("read mismatch checksum read: {}, compute: {} data_sz: {}", checksum, compute, data_sz);
        throw IOException("checksum miss match");
      }
      offset_ += data_sz;
      size += data_sz;

      int leftover = kBlockSize - offset_ % kBlockSize;
      if (leftover <= kHeaderSize) {
        offset_ += leftover;
      }
    } while (t != RecordType::kLastType & t != RecordType::kFullType);
    *record = Slice(*scratch);
    if (offset_ == handle_.FileSize()) {
      return false;
    }
    return true;
  }
}