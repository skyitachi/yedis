//
// Created by Shiping Yao on 2023/3/12.
//

#ifndef YEDIS_DB_IMPL_H
#define YEDIS_DB_IMPL_H

#include <db.h>
#include <mutex>

#include "wal.h"

namespace yedis {
  class DBImpl: public DB {
  public:
    Status Put(const WriteOptions& options, const Slice& key,
               const Slice& value) override;

  private:
    std::mutex mu_;

    wal::Writer* wal_writer_;

  };

}

#endif //YEDIS_DB_IMPL_H
