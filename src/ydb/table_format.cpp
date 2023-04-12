//
// Created by Shiping Yao on 2023/4/12.
//

#include "table_format.h"
#include "util.hpp"

namespace yedis {

BlockHandle::BlockHandle(): offset_(0), size_(0) {}

void BlockHandle::EncodeTo(std::string *dst) const {
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  }
  return Status::Corruption("bad block handle");
}


}
