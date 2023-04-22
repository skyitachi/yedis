//
// Created by Shiping Yao on 2023/3/14.
//

#ifndef YEDIS_ALLOCATOR_H
#define YEDIS_ALLOCATOR_H
#include <memory>

#include "constants.h"

namespace yedis {
  // reference from duckdb
class Allocator;
struct AllocatorDebugInfo;

struct PrivateAllocatorData {
  PrivateAllocatorData();
  virtual ~PrivateAllocatorData();

  std::unique_ptr<AllocatorDebugInfo> debug_info;
  size_t allocated_;
};

typedef data_ptr_t (*allocate_function_ptr_t)(PrivateAllocatorData *private_data, idx_t size);
typedef void (*free_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
typedef data_ptr_t (*reallocate_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                                idx_t size);


class AllocatedData {
public:
  AllocatedData();
  AllocatedData(Allocator &allocator, data_ptr_t pointer, idx_t allocated_size);
  ~AllocatedData();
  // disable copy constructors
  AllocatedData(const AllocatedData &other) = delete;
  AllocatedData &operator=(const AllocatedData &) = delete;
  //! enable move constructors
  AllocatedData(AllocatedData &&other) noexcept;
  AllocatedData &operator=(AllocatedData &&) noexcept;

  data_ptr_t get() {
    return pointer;
  }
  const_data_ptr_t get() const {
    return pointer;
  }
  idx_t GetSize() const {
    return allocated_size;
  }
  void Reset();

private:
  Allocator *allocator;
  data_ptr_t pointer;
  idx_t allocated_size;
};

class Allocator {
public:
  Allocator();
  Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
                       reallocate_function_ptr_t reallocate_function_p,
                       std::unique_ptr<PrivateAllocatorData> private_data);
  Allocator &operator=(Allocator &&allocator) noexcept = delete;
  ~Allocator();

  data_ptr_t AllocateData(idx_t size);
  void FreeData(data_ptr_t pointer, idx_t size);
  data_ptr_t ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t new_size);

  AllocatedData Allocate(idx_t size) {
    return AllocatedData(*this, AllocateData(size), size);
  }
  static data_ptr_t DefaultAllocate(PrivateAllocatorData *private_data, idx_t size) {
    private_data->allocated_ += size;
    return (data_ptr_t)malloc(size);
  }
  static void DefaultFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
    private_data->allocated_ -= size;
    free(pointer);
  }
  static data_ptr_t DefaultReallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                      idx_t size) {
    return (data_ptr_t)realloc(pointer, size);
  }

  PrivateAllocatorData *GetPrivateData() {
    return private_data.get();
  }

  size_t size() const {
    return private_data->allocated_;
  }

  static Allocator &DefaultAllocator();
  static std::shared_ptr<Allocator> &DefaultAllocatorReference();


private:
  allocate_function_ptr_t allocate_function;
  free_function_ptr_t free_function;
  reallocate_function_ptr_t reallocate_function;

  std::unique_ptr<PrivateAllocatorData> private_data;
};


}
#endif //YEDIS_ALLOCATOR_H
