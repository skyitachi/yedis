//
// Created by skyitachi on 23-3-12.
//

#ifndef YEDIS_SRC_YDB_FS_HPP_
#define YEDIS_SRC_YDB_FS_HPP_
#include <string>
#include <memory>
#include <string_view>
#include <unistd.h>
#include <fcntl.h>

namespace yedis {
class FileSystem;
class DB;

struct FileHandle {
public:
  FileHandle(FileSystem& file_system, std::string_view path);
  FileHandle(const FileHandle&) = delete;
  virtual ~FileHandle();
  virtual void Close() = 0;
  virtual int64_t FileSize() = 0;
  int64_t Read(void* buffer, int64_t nr_bytes);
  int64_t Write(void *buffer, int64_t nr_bytes);

  int64_t Read(void *buffer, int64_t nr_bytes, int64_t location);
  int64_t Write(void *buffer, int64_t nr_bytes, int64_t location);
 public:
  FileSystem& file_system;
  std::string path;
};

class FileSystem {
  public:
  virtual ~FileSystem() = default;
 public:
//  static FileSystem& GetFileSystem(DB& db);
  virtual std::unique_ptr<FileHandle> OpenFile(std::string_view path, int flags) = 0;
  virtual int64_t GetFileSize(FileHandle& handle) = 0;
  virtual int64_t Read(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location) = 0;
  virtual int64_t Write(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location) = 0;
  virtual int64_t Read(FileHandle& handle, void *buffer, int64_t nr_bytes) = 0;
  virtual int64_t Write(FileHandle& handle, void *buffer, int64_t nr_bytes) = 0;


};

struct UnixFileHandle: public FileHandle {
public:
  UnixFileHandle(FileSystem& file_system, std::string_view path, int fd): FileHandle(file_system, path), fd(fd) {}
  ~UnixFileHandle() override {
    Close();
  }
  int fd;
  int64_t FileSize() override;
public:
  void Close() override {
    if (fd != -1) {
      ::close(fd);
      fd = -1;
    }
  }
};

class LocalFileSystem: public FileSystem {
public:
  std::unique_ptr<FileHandle> OpenFile(std::string_view path, int flags) override;

  int64_t GetFileSize(FileHandle& handle) override;

  int64_t Read(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location) override;

  int64_t Write(FileHandle& handle, void *buffer, int64_t nr_bytes, int64_t location) override;

  int64_t Read(FileHandle& handle, void *buffer, int64_t nr_bytes) override;

  int64_t Write(FileHandle& handle, void *buffer, int64_t nr_bytes) override;

  ~LocalFileSystem() override = default;
};

}
#endif //YEDIS_SRC_YDB_FS_HPP_
