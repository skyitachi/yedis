//
// Created by Shiping Yao on 2023/4/19.
//

#ifndef YEDIS_VERSION_SET_H
#define YEDIS_VERSION_SET_H
#include <vector>

#include "slice.h"
#include "db_format.h"

namespace yedis {

struct FileMetaData {
  FileMetaData(): refs(0), allowed_seeks(1 << 30), file_size(0) {}
  int refs;
  int allowed_seeks;
  uint64_t number;
  uint64_t file_size;
  InternalKey smallest;
  InternalKey largest;
};

int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*> &files, const Slice& key);

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

class Version {

};

class VersionEdit;

class VersionSet {

private:
  int level_;
  Version* input_version_;
  VersionEdit* edit_;
  std::vector<FileMetaData* > inputs_[2];

//  std::vector<FileMetaData*> grandparents_;
  bool seen_key_;
  int64_t overlapped_bytes_;

  size_t level_ptrs_[7];

};
}
#endif //YEDIS_VERSION_SET_H
