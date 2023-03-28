//
// Created by Shiping Yao on 2023/3/14.
//

#ifndef YEDIS_CHECKSUM_H
#define YEDIS_CHECKSUM_H
#include <cstdint>

namespace yedis {
  uint32_t Checksum(uint8_t* buffer, uint64_t size);
  namespace crc32 {
    uint32_t Extend(uint32_t crc, uint8_t* buffer, uint64_t size);

    uint32_t Value(uint8_t *buffer, uint64_t size);

    uint32_t Mask(uint32_t crc);
  }
}

#endif //YEDIS_CHECKSUM_H
