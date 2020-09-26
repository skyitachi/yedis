//
// Created by skyitachi on 2020/8/23.
//
#include <spdlog/spdlog.h>
#include <cassert>
#include <iostream>

#include <disk_manager.hpp>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

namespace yedis {
  DiskManager::DiskManager(const std::string &db_file) {
    file_name_ = db_file;
    fd_ = open(db_file.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
      spdlog::error("{} open failed {}", db_file, strerror(errno));
      throw "open failed";
    }
  }

  void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
    lseek(fd_, page_id * PAGE_SIZE, SEEK_SET);
    read(fd_, page_data, PAGE_SIZE);
    spdlog::info("[ReadPage] success");
  }

  void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
    lseek(fd_, page_id * PAGE_SIZE, SEEK_SET);
    spdlog::info("write to disk data: page_id {}, page_size {}, db file: {}", page_id, PAGE_SIZE, file_name_);
    int written = write(fd_, page_data, PAGE_SIZE);
    if (written != PAGE_SIZE) {
      spdlog::error("write to disk error, written {} data", written);
    } else {
      spdlog::info("write success");
    }
  }

  page_id_t DiskManager::AllocatePage() {
    return next_page_id_.fetch_add(1);
  }

  void DiskManager::ShutDown() {
    close(fd_);
  }

  int DiskManager::GetFileSize(const std::string &file_name) {
    return -1;
  }

  void DiskManager::Destroy() {
    unlink(file_name_.c_str());
  }
}
