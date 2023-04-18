//
// Created by Shiping Yao on 2023/4/14.
//
#include <spdlog/spdlog.h>

#include "table.h"
#include "table_format.h"
#include "fs.hpp"
#include "block.h"
#include "comparator.h"
#include "filter_block.h"
#include "two_level_iterator.h"

namespace yedis {

struct Table::Rep {
  ~Rep() {
//    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  FileHandle* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
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
  ReadOptions opt{};
  opt.verify_checksums = true;

  // read index_block
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
  if (s.ok()) {
    auto index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->filter_data = nullptr;
//     TODO:
//    rep->cache_id
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }
  return s;
}

Table::~Table() {
  delete rep_;
}

void Table::ReadMeta(const Footer &footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    return;
  }

  Block* meta = new Block(contents);
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.comparator->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }

  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice &filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  BlockContents block_contents;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block_contents).ok()) {
    return;
  }

  if (block_contents.heap_allocated) {
    rep_->filter_data = block_contents.data.data();
  }

  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block_contents.data);
}

// TODO: no cache support
// MUST add cache support
Iterator *Table::BlockReader(void *arg, const ReadOptions &option, const Slice &index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
//  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
//  Cache::Handle* cache_handle = nullptr;
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  if (s.ok()) {
    BlockContents contents;
    s = ReadBlock(table->rep_->file, option, handle, &contents);
    if (s.ok()) {
      block = new Block(contents);
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Status Table::InternalGet(const ReadOptions& options, const Slice &key, void *arg,
                          void (*handle_result)(void *, const Slice &, const Slice &)) {
  Status s;
  Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  if (index_iter->Valid()) {
    Slice handle_value = index_iter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok()
      &&!filter->KeyMayMatch(handle.offset(), key)) {
    } else {
      // index_block用于快速定位在哪一个block里，restarts用户在block里搜索
      Iterator* block_iter = BlockReader(this, options, index_iter->value());
      block_iter->Seek(key);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = index_iter->status();
  }
  delete index_iter;
  return s;
}


Iterator* Table::NewIterator(const ReadOptions &options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}
}
