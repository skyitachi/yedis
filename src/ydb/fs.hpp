//
// Created by skyitachi on 23-3-12.
//

#ifndef YEDIS_SRC_YDB_FS_HPP_
#define YEDIS_SRC_YDB_FS_HPP_
#include <string>
#include <memory>
#include <string_view>

namespace yedis {
class FileSystem;
class DB;

struct FileHandle {
public:
  FileHandle(FileSystem& file_system, std::string path);
  FileHandle(const FileHandle&) = delete;
  virtual ~FileHandle();
  int64_t Read(void* buffer, int64_t nr_bytes);
  int64_t Write(void *buffer, int64_t nr_bytes);

  void Read(void *buffer, int64_t nr_bytes, int64_t location);
  void Write(void *buffer, int64_t nr_bytes, int64_t location);
 public:
  FileSystem& file_system;
  std::string path;
};

class FileSystem {
 public:
  virtual ~FileSystem();
 public:
  static FileSystem& GetFileSystem(DB& db);
  virtual std::unique_ptr<FileHandle> OpenFile(std::string_view path, uint8_t flags);
  virtual void Read(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location);
  virtual void Write(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location);
  virtual int64_t Read(FileHandle& handle, void *buffer, int64_t nr_bytes);
  virtual int64_t Write(FileHandle& handle, void *buffer, int64_t nr_bytes);


};

class LocalFileSystem: public FileSystem {
public:
  std::unique_ptr<FileHandle> OpenFile(std::string_view path, uint8_t flags) override;

  void Read(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location) override;

  void Write(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location) override;

  int64_t Read(FileHandle& handle, void *buffer, int64_t nr_bytes) override;

  int64_t Write(FileHandle& handle, void *buffer, int64_t nr_bytes) override;
};

}
#endif //YEDIS_SRC_YDB_FS_HPP_
