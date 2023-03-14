//
// Created by Shiping Yao on 2023/3/14.
//

#include "common/checksum.h"
#include <crc32c/crc32c.h>

namespace yedis {
  uint32_t Checksum(uint8_t* buffer, uint64_t size) {
    return crc32c::Crc32c(buffer, size);
  }
}
