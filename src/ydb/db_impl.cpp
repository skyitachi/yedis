//
// Created by Shiping Yao on 2023/3/12.
//
#include "db_impl.h"

namespace yedis {

  Status DBImpl::Put(const WriteOptions& options, const Slice& key,
             const Slice& value) {
    return Status::NotSupported("not implement");
  }
}
