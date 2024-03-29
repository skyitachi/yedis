//
// Created by skyitachi on 23-4-11.
//

#include "block_builder.h"
#include "util.hpp"
#include "options.h"

namespace yedis {

BlockBuilder::BlockBuilder(const yedis::Options *options):
  options_(options), restarts_(), counter_(0), finished_(false) {
  restarts_.push_back(0);
}

void BlockBuilder::Reset() {
  restarts_.clear();
  restarts_.push_back(0);
  counter_ = 0;
  finished_ = false;
  buffer_.clear();
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t);
}

Slice BlockBuilder::Finish() {
  for(auto offset: restarts_) {
    PutFixed32(&buffer_, offset);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const yedis::Slice &key, const yedis::Slice &value) {
  int32_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    int min_length = std::min(key.size(), last_key_.size());
    while(shared < min_length && key[shared] == last_key_[shared]) {
      shared++;
    }
  } else {
    counter_ = 0;
    restarts_.push_back(buffer_.size());
  }
  int32_t non_shared = key.size() - shared;

  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  last_key_.assign(key.data(), key.size());
  counter_++;
}
}