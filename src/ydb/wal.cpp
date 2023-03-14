//
// Created by Shiping Yao on 2023/3/14.
//
#include "common/status.h"
#include "fs.hpp"
#include "wal.h"
#include "log_format.h"
#include "allocator.h"

namespace yedis {
namespace wal {
  Writer::Writer(FileHandle &handle): handle_(handle), block_offset_(0) {
    Allocator& allocator = Allocator::DefaultAllocator();
    file_buffer_ = std::make_unique<FileBuffer>(allocator, FileBufferType::BLOCK, kBlockSize);

  }
  Writer::~Writer() {
    handle_.Close();
  }

  Status Writer::AddRecord(const Slice &slice) {


  }
}
}