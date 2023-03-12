//
// Created by skyitachi on 23-3-12.
//

#ifndef YEDIS_SRC_YDB_FS_HPP_
#define YEDIS_SRC_YDB_FS_HPP_
#include <string>

namespace yedis {
class FileSystem;

struct FileHandle {
public:
  FileHandle(FileSystem& file_system, std::string path);
  FileHandle(const FileHandle&) = delete;
  virtual ~FileHandle();
  int64_t Read(void* buffer, int64_t nr_bytes);
  int64_t Write(void *buffer, int64_t nr_bytes, int64_t offset);
  void Sync();
  void Seek(int64_t location);
 public:
  FileSystem& file_system;
  std::string path;
};

class FileSystem {
 public:
  virtual ~FileSystem();
 public:
};

}
#endif //YEDIS_SRC_YDB_FS_HPP_
