//
// Created by Shiping Yao on 2023/4/14.
//

#ifndef YEDIS_TABLE_H
#define YEDIS_TABLE_H
#include "common/status.h"
#include "options.h"

namespace yedis {

class FileHandle;
class Footer;
class Iterator;

class Table {
public:
  static Status Open(const Options& options, FileHandle* file, Table** table);

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table();
private:
  struct Rep;

  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  explicit Table(Rep* rep) : rep_(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v));

  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);

  Rep* const rep_;
};
}
#endif //YEDIS_TABLE_H
