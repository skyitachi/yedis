//
// Created by Shiping Yao on 2023/4/11.
//

#ifndef YEDIS_TABLE_BUILDER_H
#define YEDIS_TABLE_BUILDER_H

#include <cstdint>

#include "common/status.h"
#include "options.h"

namespace yedis {
class BlockBuilder;
class BlockHandle;
class WritableFile;

class TableBuilder {

public:
  TableBuilder(const Options& options, WritableFile* file);

  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  ~TableBuilder();

  void Add(const Slice& key, const Slice& value);

  void Flush();

  Status Finish();

  void Abandon();

  uint64_t NumEntries() const;

  uint64_t FileSize() const;

private:
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  struct Rep;
  Rep* rep_;
};
}

#endif //YEDIS_TABLE_BUILDER_H
