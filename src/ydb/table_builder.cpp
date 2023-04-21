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
#include "filter_block.h"
#include "comparator.h"
#include "filter_policy.h"

#include <spdlog/spdlog.h>

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
        pending_index_entry(false),
        filter_block(nullptr) {
    index_block_options.block_restart_interval = 1;
    if (options.filter_policy != nullptr) {
      filter_block = new FilterBlockBuilder(options.filter_policy);
    }
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
  FilterBlockBuilder* filter_block;
};

TableBuilder::TableBuilder(const yedis::Options &options, yedis::FileHandle *file): rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);
  delete rep_->filter_block;
  delete rep_;
}

void TableBuilder::Add(const yedis::Slice &key, const yedis::Slice &value) {
  Rep *r = rep_;
  if (r->pending_index_entry) {
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string index_value;
    r->pending_handle.EncodeTo(&index_value);
    r->index_block.Add(r->last_key, index_value);
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
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

  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
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

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  if (r->status.ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), CompressionType::kNoCompression, &filter_block_handle);
  }

  if (r->status.ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  if (r->status.ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  if (r->status.ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);

    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->file->Write(footer_encoding.data(), footer_encoding.size());
    r->offset += footer_encoding.size();
  }
  return r->status;
}

}
