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

namespace yedis {

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
           const Slice& value) {
  return Status::NotSupported("not implement");
}

void DBImpl::CompactMemTable() {
  mu_.AssertHeld();
  assert(imm_ != nullptr);
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);
    s = versions_->LogAndApply(&edit, &mu_);
  }

  if (s.ok()) {
    // TODO: clear, 如何清理Memtable， unique_ptr是否可行
    imm_->Unref();
    imm_ = nullptr;
  }
}

Status DBImpl::WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base) {
  mu_.AssertHeld();
  FileMetaData meta;
  // TODO:
  meta.number = versions_->NewFileNumber();
  Status s;
  Iterator* iter = mem->NewIterator();
  {
    mu_.Unlock();
    s = BuildTable(db_name_, options_, iter, &meta);
    mu_.Lock();
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

}
