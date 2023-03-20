//
// Created by Shiping Yao on 2023/3/14.
//

#ifndef YEDIS_WAL_H
#define YEDIS_WAL_H
#include <string_view>

#include "common/status.h"
#include "file_buffer.h"

namespace yedis {
class Slice;
class FileHandle;
class FileSystem;

namespace wal {
  class Writer {
  public:
    explicit Writer(FileHandle& handle);
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    ~Writer();

    Status AddRecord(const Slice& slice);
    Status AddRecord(std::string_view slice);
  private:
    inline int available();
    data_ptr_t data();
    data_ptr_t header();
    unsigned short size();
    FileHandle& handle_;
    std::unique_ptr<FileBuffer> file_buffer_;
    int block_offset_;
  };

}
}
#endif //YEDIS_WAL_H
