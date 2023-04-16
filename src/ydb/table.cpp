//
// Created by Shiping Yao on 2023/4/14.
//
#include <spdlog/spdlog.h>

#include "table.h"
#include "table_format.h"
#include "fs.hpp"
#include "slice.h"
#include "options.h"

namespace yedis {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  FileHandle* file;
  uint64_t cache_id;
//  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

  Status Table::Open(const Options &options, FileHandle *file, Table **table) {
  Status s;
  uint64_t size = file->FileSize();
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short for sstable");
  }

  char footer_space[Footer::kEncodedLength];
  file->Read(footer_space, Footer::kEncodedLength, size - Footer::kEncodedLength);
  Slice footer_input(footer_space, Footer::kEncodedLength);

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  BlockContents index_block_contents;
  ReadOptions opt;
  opt.verify_checksums = true;

  // read index_block
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
  if (!s.ok()) {
    return s;
  }



  return Status::OK();

}

Table::~Table() {
  delete rep_;
}

}
