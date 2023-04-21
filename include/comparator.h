//
// Created by Shiping Yao on 2023/4/6.
//

#ifndef YEDIS_COMPARATOR_H
#define YEDIS_COMPARATOR_H

#include <string>

namespace yedis {

class Slice;

class Comparator {
public:
  virtual ~Comparator() {};

  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  //
  // Names starting with "leveldb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  virtual void FindShortestSeparator(std::string* start, const Slice& limit) const = 0;

  virtual void FindShortSuccessor(std::string* key) const = 0;

};

const Comparator* BytewiseComparator();
}
#endif //YEDIS_COMPARATOR_H
