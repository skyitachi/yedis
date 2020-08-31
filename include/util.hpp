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

inline void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

inline void PutByte(std::string* dst, char value) {
  char buf[1] = {value};
  dst->append(buf, 1);
}

}

#endif //YEDIS_INCLUDE_UTIL_HPP_
