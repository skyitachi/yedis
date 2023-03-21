//
// Created by Shiping Yao on 2023/3/14.
//

#ifndef YEDIS_FILE_BUFFER_H
#define YEDIS_FILE_BUFFER_H

#include "constants.h"

namespace yedis {
class Allocator;
class FileHandle;

enum class FileBufferType: uint8_t { BLOCK = 1, MANAGED_BUFFER = 2, TINY_BUFFER = 3};

class FileBuffer {

public:
  FileBuffer(Allocator& allocator, FileBufferType type, uint64_t user_size);
  Allocator& allocator;
  FileBufferType type;
  data_ptr_t buffer;
  uint64_t size;

public:
  void Write(FileHandle& handle, uint64_t location);
  void Append(FileHandle& handle);
  void Resize(uint64_t new_size);

  struct MemoryRequireMent {
    idx_t alloc_size;
    idx_t header_size;
  };

  MemoryRequireMent CalculateMemory(uint64_t user_size);
protected:
  void Init();
  void ReallocBuffer(uint64_t malloc_size);

private:
  data_ptr_t malloced_buffer;
  uint64_t malloced_size;
};

}

#endif //YEDIS_FILE_BUFFER_H
