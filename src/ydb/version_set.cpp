//
// Created by Shiping Yao on 2023/4/21.
//
#include <set>
#include <utility>

#include "version_set.h"
#include "util.hpp"
#include "fs.hpp"
#include "table.h"
#include "iterator.h"
#include "db_format.h"

namespace yedis {

static Status SetCurrentFile(FileSystem* fs, const std::string& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  std::string tmp = TempFileName(dbname, descriptor_number);
  auto handle = fs->OpenFile(tmp, O_RDWR | O_CREAT);

  std::string full_content = contents.ToString() + "\n";
  auto written = handle->Write(full_content.data(), full_content.size());
  if (written != full_content.size()) {
    return Status::Corruption("cannot write full data to disk when set current file");
  }
  Status s = fs->RenameFile(tmp, CurrentFileName(dbname));
  if (!s.ok()) {
    fs->RemoveFile(tmp);
    return s;
  }
  return s;
}

enum class Tag: uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9
};

void Version::Ref() { ++refs_; }
void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    std::cout << "free here" << std::endl;
    delete this;
  }
}

static bool NewestFile(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

Status Version::Get(const ReadOptions& options, const LookupKey &key, std::string *val) {
  // find in level 0
  Status s;
  auto internal_key = key.internal_key();
  auto user_key = key.user_key();
  auto ucmp = vset_->icmp_.user_comparator();
  std::vector<FileMetaData* > maybes;
  for(auto &f: files_[0]) {
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0
        && ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      maybes.push_back(f);
    }
  }
  std::sort(maybes.begin(), maybes.end(), NewestFile);
  for(auto* f: maybes) {
    // TableFileName()
    auto tname = TableFileName(vset_->db_name_, f->number);
    auto table_file_handle = vset_->options_->file_system->OpenFile(tname, O_RDONLY);
    Table *table;
    Options table_opt = *vset_->options_;
    table_opt.comparator = &vset_->icmp_;
    s = Table::Open(table_opt, table_file_handle.get(), &table);
    if (!s.ok()) {
      return s;
    }
    auto it = table->NewIterator(options);
    it->Seek(internal_key);
    if (!it->Valid()) {
      return it->status();
    }
    InternalKey ikey;
    ikey.DecodeFrom(it->key());
    if (ucmp->Compare(ikey.user_key(), user_key) == 0) {
      auto vt = ExtractValueType(it->key());
      if (vt == ValueType::kTypeDeletion) {
        return Status::NotFound("");
      } else if (vt == ValueType::kTypeValue) {
        val->assign(it->value().data(), it->value().size());
        return Status::OK();
      }
      return Status::Corruption("unexpect value type");
    }
  }

  return Status::NotFound("");
}

void VersionEdit::Clear() {
  comparator_ = std::nullopt;
  log_number_ = std::nullopt;
  prev_log_number_ = std::nullopt;
  next_file_number_ = std::nullopt;
  last_sequence_ = std::nullopt;
  compact_pointers_.clear();
  deleted_file_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string *dst) {
  if (comparator_.has_value()) {
    PutVarint32(dst, static_cast<uint32_t>(Tag::kComparator));
    PutLengthPrefixedSlice(dst, comparator_.value());
  }
  if (log_number_.has_value()) {
    PutVarint32(dst, static_cast<uint32_t>(Tag::kLogNumber));
    PutVarint64(dst, log_number_.value());
  }
  if (prev_log_number_.has_value()) {
    PutVarint32(dst, static_cast<uint32_t>(Tag::kPrevLogNumber));
    PutVarint64(dst, prev_log_number_.value());
  }
  if (next_file_number_.has_value()) {
    PutVarint32(dst, static_cast<uint32_t>(Tag::kNextFileNumber));
    PutVarint64(dst, next_file_number_.value());
  }

  if (last_sequence_.has_value()) {
    PutVarint32(dst, static_cast<uint32_t>(Tag::kLastSequence));
    PutVarint64(dst, last_sequence_.value());
  }

  // compactor pointer

  for(const auto& deleted_file_kvp: deleted_file_) {
    PutVarint32(dst, static_cast<uint32_t>(Tag::kDeletedFile));
    PutVarint32(dst, deleted_file_kvp.first);
    PutVarint64(dst, deleted_file_kvp.second);
  }

  for(auto & new_file : new_files_) {
    const FileMetaData& f= new_file.second;
    PutVarint32(dst, static_cast<uint32_t>(Tag::kNewFile));
    PutVarint32(dst, new_file.first);
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
}

class VersionSet::Builder {

private:
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;
    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      }
      return (f1->number < f2->number);
    };
  };
  using FileSet = std::set<FileMetaData*, BySmallestKey>;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

public:
  Builder(VersionSet* vset, Version* base): vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp{};
    cmp.internal_comparator = &vset_->icmp_;
    for(auto & level : levels_) {
      level.added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for(auto & level: levels_) {
      const FileSet* added = level.added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (auto it : *added) {
        to_unref.push_back(it);
      }
      delete added;
      for (auto f : to_unref) {
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    std::cout << "unref in builder deconstruct" << std::endl;
    base_->Unref();
  }

  void Apply(const VersionEdit* edit) {
    // TODO: compaction pointers
    // deleted files
    for(auto [level, number]: edit->deleted_file_) {
      levels_[level].deleted_files.insert(number);
    }

    // added files
    for(auto& [level, file_meta]: edit->new_files_) {
      auto* f = new FileMetaData(file_meta);
      f->refs = 1;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }

  }

  // TODO: 重点检测这里的逻辑
  void SaveTo(Version *v) {
    BySmallestKey cmp{};
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      auto& base_files = base_->files_[level];
      const auto* added_files = levels_[level].added_files;
      // 要把base_->file + added_files save 到v里
      v->files_[level].insert(v->files_[level].end(), base_files.begin(), base_files.end());
      v->files_[level].insert(v->files_[level].end(), added_files->begin(), added_files->end());
      // 从小到大
      std::sort(v->files_[level].begin(), v->files_[level].end(), cmp);
      // 忽略deleted中的file
      std::erase_if(v->files_[level], [&](FileMetaData* file_meta) {
        return levels_[level].deleted_files.count(file_meta->number) > 0;
      });
      for(auto& f: v->files_[level]) {
        f->refs++;
      }
    }
  }
};

Status VersionSet::LogAndApply(VersionEdit *edit, std::mutex *mu) {
  assert(!mu->try_lock());
  if (edit->log_number_.has_value()) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }
  if (!edit->prev_log_number_.has_value()) {
    edit->SetPrevLogNumber(prev_log_number_);
  }
  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);
  // TODO build version
  auto* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }

  // Finalize

  std::string new_manifest_file;
  Status s;
  if (!descriptor_log_) {
    new_manifest_file = DescriptorFileName(db_name_, manifest_file_number_);
    descriptor_log_ = options_->file_system->OpenFile(new_manifest_file, O_RDWR | O_CREAT);
    if (!descriptor_log_) {
      return Status::Corruption("create manifest error");
    }
    descriptor_log_writer_ = std::make_unique<wal::Writer>(*descriptor_log_);
    // TODO: WriteSnapshot?
  }

  // write edit to manifest
  {
    mu->unlock();
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_writer_->AddRecord(record);
    }
    // set current
    if (s.ok() && !new_manifest_file.empty()) {
      // NOTE: 为什么这里需要加锁
      SetCurrentFile(options_->file_system, db_name_, manifest_file_number_);
    }
    mu->lock();
  }
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_.value();
    prev_log_number_ = edit->prev_log_number_.value();
  } else {
    // TODO: error handle
  }

  return s;
}

void VersionSet::AppendVersion(Version *v) {
  assert(v != nullptr);
  assert(v->refs_ == 0);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  current_->Ref();

  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

VersionSet::VersionSet(std::string dbname, const Options *options, const InternalKeyComparator *icmp)
  : db_name_(std::move(dbname)),
    options_(options),
    icmp_(*icmp),
    dummy_versions_(this),
    next_file_number_(1),
    log_number_(0),
    prev_log_number_(0),
    manifest_file_number_(next_file_number_),
    last_sequence_(0),
    current_(nullptr),
    descriptor_log_writer_(nullptr),
    descriptor_log_(nullptr) {
  AppendVersion(new Version(this));
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}
}
