//
// Created by skyitachi on 23-3-12.
//

#include <fcntl.h>
#include <unistd.h>
#include <format>
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
}