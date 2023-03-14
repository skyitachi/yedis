//
// Created by Shiping Yao on 2023/3/14.
//

#include "file_buffer.h"
#include "allocator.h"
#include "log_format.h"
#include "fs.hpp"
#include "util.hpp"
#include "common/checksum.h"


namespace yedis {
  FileBuffer::FileBuffer(Allocator &allocator, FileBufferType type, uint64_t user_size):
    allocator(allocator), type(type) {
    Init();
    if (user_size) {
      Resize(user_size);
    }
  }

  void FileBuffer::Init() {
    buffer = nullptr;
    size = 0;
    internal_buffer = nullptr;
    internal_size = 0;
    malloced_buffer = nullptr;
    malloced_size = 0;
  }

  // fixed buffer size
  void FileBuffer::Resize(uint64_t new_size) {
    ReallocBuffer(new_size);
    if (new_size > 0) {
      buffer = internal_buffer + wal::kHeaderSize;
      size = internal_size - wal::kHeaderSize;
    }
  }

  void FileBuffer::ReallocBuffer(uint64_t new_size) {
    if (malloced_buffer) {
      malloced_buffer = allocator.ReallocateData(malloced_buffer, malloced_size, new_size);
    } else {
      malloced_buffer = allocator.AllocateData(new_size);
    }
    if (!malloced_buffer) {
      throw std::bad_alloc();
    }
    malloced_size = new_size;
    internal_buffer = malloced_buffer;
    internal_size = malloced_size;

    buffer = nullptr;
    size = 0;
  }

  void FileBuffer::Write(FileHandle& handle, uint64_t location) {
    handle.Write(internal_buffer, internal_size, location);
  }

  void FileBuffer::Append(FileHandle &handle) {
    handle.Write(internal_buffer, internal_size);
  }

  void FileBuffer::ChecksumAndWrite(FileHandle &handle, uint64_t location) {
    uint32_t checksum = Checksum(buffer, size);
    EncodeFixed32(reinterpret_cast<char *>(internal_buffer), checksum);
    Write(handle, location);
  }
}
