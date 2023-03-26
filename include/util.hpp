//
// Created by skyitachi on 2020/8/15.
//

#ifndef YEDIS_INCLUDE_UTIL_HPP_
#define YEDIS_INCLUDE_UTIL_HPP_
#include <string>
#include <cstring>

namespace yedis {
inline void EncodeFixed32(char* dst, uint32_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  // little endian
  std::memcpy(buffer, &value, sizeof(uint32_t));
}

inline uint32_t DecodeFixed32(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  uint32_t result;
  std::memcpy(&result, buffer, sizeof(uint32_t));
  return result;
}

//　default little endian
inline void EncodeFixed64(char *dst, uint64_t value) {
  uint8_t *const buffer = reinterpret_cast<uint8_t*>(dst);
  std::memcpy(buffer, &value, sizeof(uint64_t));
}

inline uint64_t DecodeFixed64(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  uint64_t result;
  std::memcpy(&result, buffer, sizeof(uint64_t));
  return result;
}

inline void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

inline void PutByte(std::string* dst, char value) {
  char buf[1] = {value};
  dst->append(buf, 1);
}

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
}

#endif //YEDIS_INCLUDE_UTIL_HPP_
