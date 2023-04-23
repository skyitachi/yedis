//
// Created by Shiping Yao on 2023/3/12.
//

#ifndef YEDIS_DB_IMPL_H
#define YEDIS_DB_IMPL_H

#include <condition_variable>
#include <thread>
#include <set>

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
  Status Delete(const WriteOptions& options, const Slice& key) override {
    return Status::NotSupported("no delete method");
  };

  Status Write(const WriteOptions& options, WriteBatch* updates) override {
    return Status::NotSupported("no write method");
  };

  Status Get(const ReadOptions& options, const Slice& key, std::string* value) override {
    return Status::NotSupported("no get method");
  }

  Iterator* NewIterator(const ReadOptions& options) override {
    return nullptr;
  }



private:
  friend class DB;

  void prepare();
  void CompactMemTable();
  Status Recover(VersionEdit* edit, bool *save_manifest);
  Status MakeRoomForWrite(bool force);
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);

  Status BuildTable(const std::string& dbname, const Options& options, Iterator* iter, FileMetaData* meta);

  std::mutex mutex_;

  std::unique_ptr<FileHandle> wal_handle_;
  wal::Writer* wal_writer_;
  MemTable* mem_;
  MemTable* imm_;
  std::string db_name_;

  Options options_;
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
  void RemoveObsoleteFiles();
  std::set<uint64_t> pending_outputs_;

  std::thread bg_thread_;



};

}

#endif //YEDIS_DB_IMPL_H
