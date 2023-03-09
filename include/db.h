//
// Created by Shiping Yao on 2023/3/9.
//

#ifndef YEDIS_DB_H
#define YEDIS_DB_H
#include <common/status.h>
#include <iterator.h>

namespace yedis {

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

class DB {
  public:
    DB() = default;

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual Status Put(const WriteOptions& options, const Slice& key,
                       const Slice& value) = 0;

    // Remove the database entry (if any) for "key".  Returns OK on
    // success, and a non-OK status on error.  It is not an error if "key"
    // did not exist in the database.
    // Note: consider setting options.sync = true.
    virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

    // Apply the specified updates to the database.
    // Returns OK on success, non-OK on failure.
    // Note: consider setting options.sync = true.
    virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

    // If the database contains an entry for "key" store the
    // corresponding value in *value and return OK.
    //
    // If there is no entry for "key" leave *value unchanged and return
    // a status for which Status::IsNotFound() returns true.
    //
    // May return some other Status on an error.
    virtual Status Get(const ReadOptions& options, const Slice& key,
                       std::string* value) = 0;

    // Return a heap-allocated iterator over the contents of the database.
    // The result of NewIterator() is initially invalid (caller must
    // call one of the Seek methods on the iterator before using it).
    //
    // Caller should delete the iterator when it is no longer needed.
    // The returned iterator should be deleted before this db is deleted.
    virtual Iterator* NewIterator(const ReadOptions& options) = 0;

    virtual ~DB();


  };
}

#endif //YEDIS_DB_H
