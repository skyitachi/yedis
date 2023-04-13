//
// Created by skyitachi on 2020/8/15.
//

#ifndef YEDIS_INCLUDE_UTIL_HPP_
#define YEDIS_INCLUDE_UTIL_HPP_
#include <string>
#include <cstring>

#include "common/status.h"

namespace yedis {

char* EncodeVarint64(char* dst, uint64_t v);
char* EncodeVarint32(char* dst, uint32_t v);
int VarintLength(uint64_t v);
void EncodeFixed32(char* dst, uint32_t value);
uint32_t DecodeFixed32(const char* ptr);
uint64_t DecodeFixed64(const char* ptr);
void EncodeFixed64(char *dst, uint64_t value);
void PutFixed32(std::string* dst, uint32_t value);
void PutByte(std::string* dst, char value);
void PutVarint32(std::string* dst, uint32_t v);
void PutVarint64(std::string* dst, uint32_t v);

const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value);
const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value);
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value);

bool GetVarint64(Slice* input, uint64_t* value);

template<class T>
inline void EncodeFixed(void* dst, T value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);
  // little endian
  std::memcpy(buffer, &value, sizeof(T));
}

template<class T>
inline T DecodeFixed(void *src) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(src);
  return *reinterpret_cast<T *>(buffer);
}

template<class T>
inline void PutFixed(std::string* dst, T value) {
  char buf[sizeof(value)];
  EncodeFixed<T>(buf, value);
  dst->append(buf, sizeof(value));
}

uint32_t Hash(const char *data, size_t n, uint32_t seed);
}

#endif //YEDIS_INCLUDE_UTIL_HPP_
