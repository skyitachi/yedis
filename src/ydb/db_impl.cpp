//
// Created by Shiping Yao on 2023/3/12.
//
#include <iostream>

#include "db_impl.h"
#include "version_set.h"
#include "memtable.h"
#include "iterator.h"
#include "options.h"
#include "table.h"
#include "util.hpp"
#include "fs.hpp"
#include "table_builder.h"
#include "write_batch.h"
#include "write_batch_internal.h"
#include "db.h"
#include "db_format.h"

namespace yedis {


DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
  : db_name_(dbname),
    options_(raw_options),
    internal_comparator_(raw_options.comparator),
    mem_(new MemTable()),
    imm_(nullptr),
    logfile_number_(0),
    versions_(new VersionSet(db_name_, &options_, &internal_comparator_)) {
  options_.file_system = new LocalFileSystem;
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
           const Slice& value) {
  // lock by my self
  WriteBatch batch;
  batch.Put(key, value);
  std::unique_lock<std::mutex> lock(mutex_, std::defer_lock);
  lock.lock();
  std::cout << "mem size: " << mem_->ApproximateMemoryUsage() << ", write_buffer_size: " << options_.write_buffer_size << std::endl;
  // TODO: 这里要wait 压缩线程
  Status s = MakeRoomForWrite(false);
  if (!s.ok()) {
    return s;
  }
  uint64_t last_sequence = versions_->LastSequence();
  last_sequence += 1;
  lock.unlock();

  // encode like write batch
  Slice write_batch_content = WriteBatchInternal::Contents(&batch);
  s = wal_writer_->AddRecord(write_batch_content);
  if (s.ok()) {
    lock.lock();
    mem_->Add(last_sequence, ValueType::kTypeValue, key, value);
  }
  lock.unlock();
  return s;
}

// Ignore force
// mutex_ acquired
Status DBImpl::MakeRoomForWrite(bool force) {
  assert(!mutex_.try_lock());
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      s = bg_error_;
      break;
    } else if (imm_ != nullptr) {
      // NOTE: should wait here
      std::unique_lock<std::mutex> lock(mutex_, std::adopt_lock);
      std::cout << "wait here" << std::endl;
      background_work_finished_signal_.wait(lock);
      std::cout << "after wait" << std::endl;
      lock.release();
    } else if (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) {
      std::cout << "have enough space" << std::endl;
      // enough space
      break;
    } else {
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      delete wal_writer_;
      wal_handle_.reset(nullptr);
      // TODO: 这里如何使用unique_ptr管理file_handle, 应该是可以直接赋值的
      wal_handle_ =
          options_.file_system->OpenFile(LogFileName(db_name_, new_log_number), O_RDWR | O_CREAT);
      wal_writer_ = new wal::Writer(*wal_handle_);
      // NOTE: important
      logfile_number_ = new_log_number;
      imm_ = mem_;
      mem_ = new MemTable();
      mem_->Ref();
      MaybeScheduleCompaction();
    }
  }
  return s;
}

// just compact level0
// mutex_ acquired
void DBImpl::MaybeScheduleCompaction() {
  assert(!mutex_.try_lock());
  if (imm_ != nullptr) {
    background_compaction_scheduled_ = true;
    std::cout << "start background call" << std::endl;
    std::thread th(&DBImpl::BackgroundCall, this);
    // NOTE: std::move 到一个持久的bg_thread_上，不然thread会自动析构
    bg_thread_ = std::move(th);
  }
}

//void DBImpl::BGWork(void *db) {
//
//}

void DBImpl::BackgroundCall() {
  std::cout << "in the background call" << std::endl;
  std::lock_guard<std::mutex> lock_guard(mutex_);
  assert(background_compaction_scheduled_);
  if (imm_ != nullptr) {
    CompactMemTable();
    background_compaction_scheduled_ = false;
    background_work_finished_signal_.notify_all();
    std::cout << "notify all" << std::endl;
    return;
  }
}


void DBImpl::CompactMemTable() {
  std::cout << "in compact memtable" << std::endl;
  assert(!mutex_.try_lock());
  assert(imm_ != nullptr);
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  std::cout << "unref in compact memtable" << std::endl;
  base->Unref();

  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // TODO: clear, 如何清理Memtable， unique_ptr是否可行
    imm_->Unref();
    imm_ = nullptr;
    RemoveObsoleteFiles();
  }
}

Status DBImpl::WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base) {
  assert(!mutex_.try_lock());
  FileMetaData meta;

  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Status s;
  Iterator* iter = mem->NewIterator();
  {
    mutex_.unlock();
    s = BuildTable(db_name_, options_, iter, &meta);
    mutex_.lock();
  }
  if (!s.ok()) {
    return s;
  }
  delete iter;
  pending_outputs_.erase(meta.number);

  // NOTE: 本次实现只写到level 0
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
  }
  return s;
}

Status DBImpl::BuildTable(const std::string &dbname, const Options &options, Iterator *iter, FileMetaData *meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  auto file_ptr = options.file_system->OpenFile(fname, O_CREAT | O_RDWR | O_TRUNC);
  auto builder = std::make_unique<TableBuilder>(options, file_ptr.get());
  meta->smallest.DecodeFrom(iter->key());
  Slice key;
  while(iter->Valid()) {
    key = iter->key();
    builder->Add(key, iter->value());
    iter->Next();
  }
  if (!key.empty()) {
    meta->largest.DecodeFrom(key);
  }
  s = builder->Finish();
  if (!s.ok()) {
    return s;
  }
  meta->file_size = builder->FileSize();
  return s;
}

Status DBImpl::Recover(VersionEdit *edit, bool *save_manifest) {

}

void DBImpl::prepare() {
  if (!options_.file_system->Exists(db_name_)) {
    auto s = options_.file_system->CreateDir(db_name_);
    assert(s.ok());
  }
  auto fs = options_.file_system;
  auto new_log_number = versions_->NewFileNumber();
  wal_handle_ = fs->OpenFile(LogFileName(db_name_, new_log_number), O_RDWR | O_CREAT);
  wal_writer_ = new wal::Writer(*wal_handle_);

  VersionEdit edit;
//  Status s = Recover();
  edit.SetLogNumber(new_log_number);
  logfile_number_ = new_log_number;
  if (mem_ != nullptr) {
    mem_->Ref();
  }


}

void DBImpl::RemoveObsoleteFiles() {
  assert(!mutex_.try_lock());

  if (!bg_error_.ok()) {
    return;
  }

  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  options_.file_system->GetChildren(db_name_, filenames);

  uint64_t number;

  FileType ft;
  std::vector<std::string> files_to_delete;
  for(const auto& filename: filenames) {
    std::cout << "files: " << filename << std::endl;
    if (ParseFileName(filename, &number, &ft)) {
      bool keep = true;
      switch (ft) {
        case FileType::kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case FileType::kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case FileType::kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case FileType::kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case FileType::kCurrentFile:
        case FileType::kDBLockFile:
        case FileType::kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
      }
    }
  }
  mutex_.unlock();
  for(const auto& filename: files_to_delete) {
    options_.file_system->RemoveFile(db_name_ + "/" + filename);
  }
  mutex_.lock();
}

DB::~DB() = default;

Status DB::Open(const Options &options, const std::string &name, DB **dbptr) {
  *dbptr = nullptr;
  auto* impl = new DBImpl(options, name);
  *dbptr = impl;
  impl->prepare();
  return Status::OK();
}

}
