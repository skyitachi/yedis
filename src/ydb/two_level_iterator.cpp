//
// Created by Shiping Yao on 2023/4/17.
//

#include "two_level_iterator.h"
#include "options.h"

namespace yedis {

namespace {

typedef Iterator* (*BlockFunction) (void *, const ReadOptions&, const Slice&);

class TwoLevelIterator: public Iterator {

public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void *arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;

  void SeekToFirst() override;

  void SeekToLast() override;

  void Next() override;

  void Prev() override;

private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();

  void SetDataIterator(Iterator* data_iter) {
    if (data_iter_ != nullptr) {
      SaveError(data_iter_->status());
      delete data_iter_;
    }
    data_iter_ = data_iter;
  }

  bool Valid() const override { return data_iter_ != nullptr && data_iter_->Valid(); }
  Slice key() const override {
    assert(data_iter_ != nullptr && data_iter_->Valid());
    return data_iter_->key();
  }

  Slice value() const override {
    assert(data_iter_ != nullptr && data_iter_->Valid());
    return data_iter_->value();
  }

  Status status() const override {
    if (!index_iter_->status().ok()) {
      return index_iter_->status();
    } else if (data_iter_ != nullptr && !data_iter_->status().ok()) {
      return data_iter_->status();
    } else {
      return status_;
    }
  }
  void InitDataBlock();

  BlockFunction block_function_;
  void *arg_;
  const ReadOptions options_;
  Status status_;
  std::string data_block_handle_;
  Iterator* index_iter_;
  Iterator* data_iter_;
};

TwoLevelIterator::TwoLevelIterator(yedis::Iterator *index_iter,
                                   yedis::BlockFunction block_function, void *arg,
                                   const yedis::ReadOptions &options)
   : block_function_(block_function),
     arg_(arg),
     options_(options),
     index_iter_(index_iter),
     data_iter_(nullptr) {}


// 终于发现为什么需要Iterator wrapper了
TwoLevelIterator::~TwoLevelIterator() {
  if (data_iter_ != nullptr) {
    delete data_iter_;
  }
  if (index_iter_ != nullptr) {
    delete index_iter_;
  }
};

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_->Valid()) {
    SetDataIterator(nullptr);
  } else {
    Slice handle = index_iter_->value();
    if (data_iter_ != nullptr && handle.compare(data_block_handle_) == 0) {
      // pass
    } else {
      Iterator* iter  = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

void TwoLevelIterator::Seek(const Slice &target) {
  index_iter_->Seek(target);
  InitDataBlock();
  if (data_iter_ != nullptr) data_iter_->Seek(target);
  // 处理跨data block seek的情况
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_->SeekToFirst();
  InitDataBlock();
  if (data_iter_ != nullptr) data_iter_->SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_->SeekToLast();
  InitDataBlock();
  if (data_iter_ != nullptr) data_iter_->SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  data_iter_->Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  data_iter_->Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_ == nullptr || !data_iter_->Valid()) {
    if (!index_iter_->Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_->Next();
    InitDataBlock();
    if (data_iter_ != nullptr) data_iter_->SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_ == nullptr || !data_iter_->Valid()) {
    if (!index_iter_->Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_->Prev();
    InitDataBlock();
    if (data_iter_ != nullptr) data_iter_->SeekToLast();
  }
}
}


Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(void* arg, const ReadOptions& options,
                                const Slice& index_value),
    void* arg, const ReadOptions& options) {

  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}