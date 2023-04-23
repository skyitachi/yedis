//
// Created by skyitachi on 23-3-12.
//

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>
#include <absl/strings/substitute.h>

#include "exception.h"
#include "fs.hpp"

namespace yedis {

FileHandle::FileHandle(FileSystem &file_system, std::string_view path): file_system(file_system), path(path) {
}

FileHandle::~FileHandle() = default;

int64_t FileHandle::Read(void *buffer, int64_t nr_bytes) {
  return file_system.Read(*this, buffer, nr_bytes);
}


int64_t FileHandle::Write(void *buffer, int64_t nr_bytes) {
  return file_system.Write(*this, buffer, nr_bytes);
}


int64_t FileHandle::Read(void *buffer, int64_t nr_bytes, int64_t location) {
  return file_system.Read(*this, buffer, nr_bytes, location);
}

int64_t FileHandle::Write(void *buffer, int64_t nr_bytes, int64_t location) {
  return file_system.Write(*this, buffer, nr_bytes, location);
}

int64_t UnixFileHandle::FileSize() {
  struct stat st{};
  int ret = fstat(fd, &st);
  if (ret != 0) {
    throw IOException("stats failed");
  }
  return st.st_size;
}

std::unique_ptr<FileHandle> LocalFileSystem::OpenFile(std::string_view path, int flags) {
  int fd = ::open(path.data(), flags, 0666);
  if (fd == -1) {
    throw IOException(absl::Substitute("Cannot open file $0: $1",
                                       path.data(), strerror(errno)));
  }
  return std::make_unique<UnixFileHandle>(*this, path, fd);
}

int64_t LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, int64_t location) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_read = pread(fd, buffer, nr_bytes, location);
  if (bytes_read == -1) {
    throw IOException(absl::Substitute("Could not read from file $0: $1", handle.path, strerror(errno)));
  }
  if (bytes_read != nr_bytes) {
    throw IOException(absl::Substitute("Could not read all bytes from file $0: wanted=$1 read=$2",
                                       handle.path, nr_bytes, bytes_read));
  }
  return bytes_read;
}

int64_t LocalFileSystem::Read(FileHandle& handle, void *buffer, int64_t nr_bytes) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_read = read(fd, buffer, nr_bytes);
  if (bytes_read == -1) {
    throw IOException(absl::Substitute("Could not read from file $0: $1", handle.path, strerror(errno)));
  }
  return bytes_read;

}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, int64_t location) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_written = pwrite(fd, buffer, nr_bytes, location);
  if (bytes_written == -1) {
    throw IOException(absl::Substitute("Could not write to file $0: $1", handle.path, strerror(errno)));
  }
  if (bytes_written != nr_bytes) {
    throw IOException(absl::Substitute("Could not write all bytes to file $0: wanted=$1 read=$2",
                                       handle.path, nr_bytes, bytes_written));
  }
  return bytes_written;
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_written = write(fd, buffer, nr_bytes);
  if (bytes_written == -1) {
    throw IOException(absl::Substitute("Could not write to file $0: $1", handle.path, strerror(errno)));
  }
  return bytes_written;
}

int64_t LocalFileSystem::GetFileSize(FileHandle &handle) {
  return handle.FileSize();
}

bool LocalFileSystem::Exists(std::string_view path) {
  return std::filesystem::exists(path);
}

Status LocalFileSystem::CreateDir(std::string_view dir) {
  std::error_code ec;
  bool succ = std::filesystem::create_directories(dir, ec);
  if (succ) {
    return Status::OK();
  }
  return Status::IOError(ec.message());
}

Status LocalFileSystem::RemoveFile(const std::string &src) {
  namespace fs = std::filesystem;
  std::error_code ec;
  bool succ = fs::remove(src, ec);
  if (succ) {
    return Status::OK();
  }
  return Status::IOError(ec.message());
}

Status LocalFileSystem::RenameFile(const std::string &src, const std::string &target) {
  namespace fs = std::filesystem;
  std::error_code ec;
  fs::rename(src, target, ec);
  if (ec) {
    return Status::IOError(ec.message());
  }
  return Status::OK();
}

Status LocalFileSystem::GetChildren(const std::string &dir, std::vector<std::string> &result) {
  namespace fs = std::filesystem;
  assert(fs::is_directory(dir));
  for (const auto& entry: fs::directory_iterator(dir)) {
    if (entry.is_regular_file()) {
      std::string file_name = entry.path().relative_path().filename().string();
      result.push_back(file_name);
    }
  }
  return Status::OK();
}

}