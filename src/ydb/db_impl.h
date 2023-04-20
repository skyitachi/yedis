//
// Created by Shiping Yao on 2023/3/12.
//

#ifndef YEDIS_DB_IMPL_H
#define YEDIS_DB_IMPL_H

#include "db.h"
#include "options.h"
#include "mutex.h"
#include "wal.h"

namespace yedis {

class VersionEdit;
class Version;
class MemTable;
class Table;
struct FileMetaData;

class DBImpl: public DB {
public:
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

private:
  void CompactMemTable();
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Status BuildTable(const std::string& dbname, const Options& options, Iterator* iter, FileMetaData* meta);

  Mutex mu_;

  wal::Writer* wal_writer_;
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mu_);
  std::string db_name_;
  Options options_;
};

}

#endif //YEDIS_DB_IMPL_H
