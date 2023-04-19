//
// Created by Shiping Yao on 2023/3/12.
//

#ifndef YEDIS_DB_IMPL_H
#define YEDIS_DB_IMPL_H

#include <db.h>

#include "mutex.h"
#include "wal.h"

class MemTable;
namespace yedis {

class DBImpl: public DB {
public:
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

private:
  void CompactMemTable();
  Mutex mu_;

  wal::Writer* wal_writer_;
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mu_);
};

}

#endif //YEDIS_DB_IMPL_H
