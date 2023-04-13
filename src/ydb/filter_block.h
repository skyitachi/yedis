//
// Created by skyitachi on 23-4-13.
//

#ifndef YEDIS_FILTER_BLOCK_H
#define YEDIS_FILTER_BLOCK_H
#include <cstdint>
#include <vector>

#include "common/status.h"

namespace yedis {

class FilterPolicy;


class FilterBlockBuilder {

public:

  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);

  Slice Finish();

private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;
  std::vector<size_t> start_;
  std::string result_;
  std::vector<Slice> tmp_keys_;
  std::vector<uint32_t> filter_offsets_;

};
}
#endif //YEDIS_FILTER_BLOCK_H
