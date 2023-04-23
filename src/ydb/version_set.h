//
// Created by Shiping Yao on 2023/4/19.
//

#ifndef YEDIS_VERSION_SET_H
#define YEDIS_VERSION_SET_H
#include <vector>
#include <set>
#include <optional>

#include "slice.h"
#include "db_format.h"
#include "option.hpp"
#include "mutex.h"
#include "common/status.h"
#include "options.h"
#include "fs.hpp"
#include "wal.h"

namespace yedis {

class VersionSet;
class Version;
class VersionEdit;

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
public:
  void Ref();
  void Unref();
private:
  friend class VersionSet;

  explicit Version(VersionSet* vset)
    : vset_(vset),
      next_(this),
      prev_(this),
      refs_(0),
      file_to_compact_(nullptr),
      file_to_compact_level_(-1),
      compaction_score_(-1),
      compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;
  ~Version() {};

  VersionSet* vset_;
  Version* next_;
  Version* prev_;
  int refs_;

  std::vector<FileMetaData*> files_[config::kNumLevels];

  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  double compaction_score_;
  int compaction_level_;

};

class VersionEdit {

public:
  void SetComparatorName(const Slice& name) {
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    last_sequence_ = seq;
  }

  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey &largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.emplace_back(level, f);
  }

  void EncodeTo(std::string *dest);
  void Clear();
  Status DecodeFrom(const Slice& src);
private:
  friend class VersionSet;

  using DeletedFileSet = std::set<std::pair<int, uint64_t>>;

  std::optional<std::string> comparator_;
  std::optional<uint64_t> log_number_;
  std::optional<uint64_t> prev_log_number_;
  std::optional<uint64_t> next_file_number_;
  std::optional<SequenceNumber> last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_file_;
  std::vector<std::pair<int, FileMetaData>> new_files_;
};

class VersionSet {
public:
  VersionSet(std::string  dbname, const Options* options, const InternalKeyComparator*);
  uint64_t NewFileNumber() { return next_file_number_++; }
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }
  uint64_t LastSequence() const { return last_sequence_; }
  void SetLastSequence(uint64_t s) { last_sequence_ = s; }
  uint64_t LogNumber() const { return log_number_; }
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  Version* current() const { return current_; }


  Status LogAndApply(VersionEdit* edit, std::mutex* mu);
  Status Recover(bool *save_manifest);
  void AppendVersion(Version* v);
  void AddLiveFiles(std::set<uint64_t>* live);


private:
  friend class Version;
  class Builder;

  const std::string db_name_;
  const Options* const options_;
  const InternalKeyComparator icmp_;
  // MANIFEST file handle
  std::unique_ptr<FileHandle> descriptor_log_;
  std::unique_ptr<wal::Writer> descriptor_log_writer_;

  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;

  Version* current_;
  Version dummy_versions_;

  size_t level_ptrs_[config::kNumLevels];

};
}
#endif //YEDIS_VERSION_SET_H
