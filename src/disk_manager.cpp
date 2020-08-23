//
// Created by skyitachi on 2020/8/23.
//

#include <disk_manager.hpp>
#include <cassert>

namespace yedis {
  DiskManager::DiskManager(const std::string &db_file) {
    file_name_ = db_file;
    db_io_ = std::fstream(db_file, db_io_.binary | db_io_.in | db_io_.in);
    assert(db_io_.is_open());
  }

  void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
    db_io_.seekp(page_id * PAGE_SIZE);
    db_io_.read(page_data, PAGE_SIZE);
  }

  void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
    db_io_.seekp(page_id);
    db_io_.write(page_data, PAGE_SIZE);
  }

  page_id_t DiskManager::AllocatePage() {
    return next_page_id_.fetch_add(1);
  }

  void DiskManager::ShutDown() {
    db_io_.close();
  }

  int DiskManager::GetFileSize(const std::string &file_name) {
    return -1;
  }
}
