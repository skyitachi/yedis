//
// Created by Shiping Yao on 2023/3/14.
//
#include "common/status.h"
#include "fs.hpp"
#include "wal.h"
#include "log_format.h"


namespace yedis {
namespace wal {
  Writer::Writer(FileSystem& file_system, FileHandle &handle):
    file_system_(file_system), handle_(handle), block_offset_(0) {}
  Writer::~Writer() {
    handle_.Close();
  }

  Status Writer::AddRecord(const Slice &slice) {


  }
}
}