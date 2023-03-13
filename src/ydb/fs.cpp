//
// Created by skyitachi on 23-3-12.
//

#include <fcntl.h>
#include <unistd.h>
#include <absl/strings/substitute.h>

#include "exception.h"
#include "fs.hpp"

namespace yedis {

FileHandle::FileHandle(FileSystem &file_system, std::string path): file_system(file_system), path(std::move(path)) {
}

FileHandle::~FileHandle() = default;

int64_t FileHandle::Read(void *buffer, int64_t nr_bytes) {
  return file_system.Read(*this, buffer, nr_bytes);
}


int64_t FileHandle::Write(void *buffer, int64_t nr_bytes) {
  return file_system.Write(*this, buffer, nr_bytes);
}


void FileHandle::Read(void *buffer, int64_t nr_bytes, int64_t location) {
  return file_system.Read(*this, buffer, nr_bytes, location);
}

void FileHandle::Write(void *buffer, int64_t nr_bytes, int64_t location) {
  return file_system.Write(*this, buffer, nr_bytes, location);
}


std::unique_ptr<FileHandle> LocalFileSystem::OpenFile(std::string_view path, uint8_t flags) {
  int fd = open(path.data(), flags, 0666);
  if (fd == -1) {
    throw IOException(absl::Substitute("Cannot open file $0: $1",
                                       path.data(), strerror(errno)));
  }
}

void LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, int64_t location) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_read = pread(fd, buffer, nr_bytes, location);
  if (bytes_read == -1) {
    throw IOException(absl::Substitute("Could not read from file $0: $1", handle.path, strerror(errno)));
  }
  if (bytes_read != nr_bytes) {
    throw IOException(absl::Substitute("Could not read all bytes from file $0: wanted=$1 read=$2",
                                       handle.path, nr_bytes, bytes_read));
  }
}

int64_t LocalFileSystem::Read(FileHandle& handle, void *buffer, int64_t nr_bytes) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_read = read(fd, buffer, nr_bytes);
  if (bytes_read == -1) {
    throw IOException(absl::Substitute("Could not read from file $0: $1", handle.path, strerror(errno)));
  }
  return bytes_read;

}

void LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, int64_t location) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_written = pwrite(fd, buffer, nr_bytes, location);
  if (bytes_written == -1) {
    throw IOException(absl::Substitute("Could not write to file $0: $1", handle.path, strerror(errno)));
  }
  if (bytes_written != nr_bytes) {
    throw IOException(absl::Substitute("Could not write all bytes to file $0: wanted=$1 read=$2",
                                       handle.path, nr_bytes, bytes_written));

  }
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
  int fd =((UnixFileHandle&) handle).fd;
  int64_t bytes_written = write(fd, buffer, nr_bytes);
  if (bytes_written == -1) {
    throw IOException(absl::Substitute("Could not write to file $0: $1", handle.path, strerror(errno)));
  }
  return bytes_written;
}


}