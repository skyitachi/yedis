//
// Created by Shiping Yao on 2023/4/12.
//
#include "table_builder.h"
#include "block_builder.h"
#include "options.h"
#include "table_format.h"
#include "fs.hpp"
#include "exception.h"
#include "common/checksum.h"
#include "util.hpp"

namespace yedis {

struct TableBuilder::Rep {
  Rep(const Options& opt, FileHandle* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;

  FileHandle* file;

  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

void TableBuilder::Add(const yedis::Slice &key, const yedis::Slice &value) {
  Rep *r = rep_;
  if (r->pending_index_entry) {
    std::string index_value;
    r->pending_handle.EncodeTo(&index_value);
    r->index_block.Add(r->last_key, index_value);
    r->pending_index_entry = false;
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);


  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  // WriteBlock
  WriteBlock(&r->data_block, &r->pending_handle);
  if (r->status.ok()) {
    r->pending_index_entry = true;
  }
}

void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
  Rep* r = rep_;
  Slice raw = block->Finish();
  Slice block_content;

  CompressionType cType = r->options.compression;
  switch (cType) {
    case kNoCompression: {
      block_content = raw;
      break;
    }
    default:
      throw Exception("not support");
  }
  WriteRawBlock(block_content, cType, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice &data, CompressionType cType, BlockHandle *handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(data.size());
  int64_t written = r->file->Write((void *) data.data(), data.size());
  if (written != data.size()) {
    r->status = Status::IOError("cannot write enough data blocks");
  }
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = cType;

    uint32_t crc = crc32::Value((uint8_t *) data.data(), data.size());
    crc = crc32::Extend(crc, reinterpret_cast<uint8_t *>(trailer), 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32::Mask(crc));
    written = r->file->Write(trailer, kBlockTrailerSize);
    if (written != kBlockTrailerSize) {
      r->status = Status::IOError("cannot write trailer block");
    }
    if (r->status.ok()) {
      r->offset += data.size() + kBlockTrailerSize;
    }
  }

}

}
