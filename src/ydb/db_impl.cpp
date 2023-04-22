//
// Created by Shiping Yao on 2023/3/12.
//
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


namespace yedis {


DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
  : db_name_(dbname),
    options_(raw_options),
    internal_comparator_(raw_options.comparator),
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

  return s;
}

// Ignore force
// mutex_ acquired
Status DBImpl::MakeRoomForWrite(bool force) {
  assert(mutex_.try_lock());
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      s = bg_error_;
      break;
    } else if (imm_ != nullptr) {
      std::unique_lock<std::mutex> lock(mutex_, std::adopt_lock);
      background_work_finished_signal_.wait(lock);
      lock.release();
    } else if (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) {
      // enough space
      break;
    } else {
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      delete wal_writer_;
      delete wal_handle_;
      // TODO: 这里如何使用unique_ptr管理file_handle
      wal_handle_ =
          options_.file_system->OpenFile(LogFileName(db_name_, new_log_number), O_RDWR | O_CREAT).get();
      wal_writer_ = new wal::Writer(*wal_handle_);
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
  assert(mutex_.try_lock());
  if (imm_ != nullptr) {
    background_compaction_scheduled_ = true;
    std::thread th([&]{ BackgroundCall(); });
  }
}

//void DBImpl::BGWork(void *db) {
//
//}

void DBImpl::BackgroundCall() {
  std::lock_guard<std::mutex> lock_guard(mutex_);
  assert(background_compaction_scheduled_);
  if (imm_ != nullptr) {
    CompactMemTable();
    background_compaction_scheduled_ = false;
    background_work_finished_signal_.notify_all();
    return;
  }
}


void DBImpl::CompactMemTable() {
  assert(mutex_.try_lock());
  assert(imm_ != nullptr);
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
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
  }
}

Status DBImpl::WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base) {
  assert(mutex_.try_lock());
  FileMetaData meta;
  // TODO:
  meta.number = versions_->NewFileNumber();
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
  // TODO: 补充下verion需要的信息
  // 本次实现只写到level 0
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

DB::~DB() = default;

Status DB::Open(const Options &options, const std::string &name, DB **dbptr) {
  *dbptr = nullptr;
  DBImpl* impl = new DBImpl(options, name);
  return Status::OK();
}

}
