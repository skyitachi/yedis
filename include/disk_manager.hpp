//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_DISK_MANAGER_HPP_
#define YEDIS_INCLUDE_DISK_MANAGER_HPP_

#include <atomic>
#include <fstream>
#include <future>
#include <string>

#include "config.hpp"

namespace yedis {

class DiskManager {
 public:
  explicit DiskManager(const std::string& db_file);

  ~DiskManager() = default;

  void ShutDown();

  void WritePage(page_id_t page_id, const char* page_data);

  // NOTE: 根据page_id * PAGE_SIZE作为offset
  void ReadPage(page_id_t page_id, char *page_data);

  page_id_t AllocatePage();

  void Destroy();

 private:
  int GetFileSize(const std::string &file_name);

  std::string file_name_;
  int fd_;
  std::atomic<page_id_t > next_page_id_;
};
}
#endif //YEDIS_INCLUDE_DISK_MANAGER_HPP_
