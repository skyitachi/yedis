//
// Created by Shiping Yao on 2023/3/9.
//

#ifndef YEDIS_WRITE_BATCH_H
#define YEDIS_WRITE_BATCH_H
#include <string>
#include <common/status.h>

namespace yedis {

  class WriteBatch {
      public:
      class Handler {
        public:
        virtual ~Handler();
        virtual void Put(const Slice& key, const Slice& value) = 0;
        virtual void Delete(const Slice& key) = 0;
      };

      WriteBatch();

      // Intentionally copyable.
      WriteBatch(const WriteBatch&) = default;
      WriteBatch& operator=(const WriteBatch&) = default;

      ~WriteBatch();

      // Store the mapping "key->value" in the database.
      void Put(const Slice& key, const Slice& value);

      // If the database contains a mapping for "key", erase it.  Else do nothing.
      void Delete(const Slice& key);

      // Clear all updates buffered in this batch.
      void Clear();

      // The size of the database changes caused by this batch.
      //
      // This number is tied to implementation details, and may change across
      // releases. It is intended for LevelDB usage metrics.
      size_t ApproximateSize() const;

      // Copies the operations in "source" to this batch.
      //
      // This runs in O(source size) time. However, the constant factor is better
      // than calling Iterate() over the source batch with a Handler that replicates
      // the operations into this batch.
      void Append(const WriteBatch& source);

      // Support for iterating over the contents of a batch.
      Status Iterate(Handler* handler) const;

      private:
      friend class WriteBatchInternal;

      std::string rep_;  // See comment in write_batch.cc for the format of rep_
  };
}

#endif //YEDIS_WRITE_BATCH_H
