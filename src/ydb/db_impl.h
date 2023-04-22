//
// Created by Shiping Yao on 2023/3/12.
//

#ifndef YEDIS_DB_IMPL_H
#define YEDIS_DB_IMPL_H

#include <condition_variable>

#include "db.h"
#include "options.h"
#include "wal.h"
#include "db_format.h"

namespace yedis {

class VersionEdit;
class Version;
class VersionSet;
class MemTable;
class Table;
struct FileMetaData;

class DBImpl: public DB {
public:
  DBImpl(const Options& raw_options, const std::string& dbname);
  ~DBImpl() = default;

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

private:
  void CompactMemTable();
  Status MakeRoomForWrite(bool force);
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);

  Status BuildTable(const std::string& dbname, const Options& options, Iterator* iter, FileMetaData* meta);

  std::mutex mutex_;

  FileHandle* wal_handle_;
  wal::Writer* wal_writer_;
  MemTable* mem_;
  MemTable* imm_;
  std::string db_name_;

  const Options options_;
  const InternalKeyComparator internal_comparator_;

  VersionSet* const versions_;
  // wal file_number
  uint64_t logfile_number_;
  Status bg_error_;

  std::condition_variable background_work_finished_signal_;
  bool background_compaction_scheduled_;

  void MaybeScheduleCompaction();
  void BGWork(void *db);
  void BackgroundCall();


};

}

#endif //YEDIS_DB_IMPL_H
