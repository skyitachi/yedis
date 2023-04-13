//
// Created by skyitachi on 23-4-13.
//

#ifndef YEDIS_FILTER_POLICY_H
#define YEDIS_FILTER_POLICY_H
#include <string>

namespace yedis {
class Slice;

class FilterPolicy {
public:
  virtual ~FilterPolicy();

  virtual const char* Name() const = 0;

  virtual void CreateFilter(const Slice* keys, int n , std::string* dst) const = 0;

  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);
}
#endif //YEDIS_FILTER_POLICY_H
