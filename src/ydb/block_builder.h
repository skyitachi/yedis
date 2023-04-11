//
// Created by skyitachi on 23-4-11.
//

#ifndef YEDIS_BLOCK_BUILDER_H
#define YEDIS_BLOCK_BUILDER_H

#include <vector>
#include "common/status.h"


namespace yedis {
struct Options;

class BlockBuilder {
public:
  explicit BlockBuilder(const Options* options);
  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  void Reset();

  void Add(const Slice& key, const Slice& value);

  Slice Finish();

  size_t CurrentSizeEstimate() const;

  bool empty() const { return buffer_.empty(); }

private:
  const Options* options_;
  std::string buffer_;
  std::vector<uint32_t> restarts_;
  int counter_;
  bool finished_;
  std::string last_key_;
};
}
#endif //YEDIS_BLOCK_BUILDER_H
