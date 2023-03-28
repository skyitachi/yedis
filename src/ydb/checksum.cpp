//
// Created by Shiping Yao on 2023/3/14.
//

#include "common/checksum.h"
#include <crc32c/crc32c.h>

namespace yedis {
  uint32_t Checksum(uint8_t* buffer, uint64_t size) {
    return crc32c::Crc32c(buffer, size);
  }

  namespace crc32 {
    uint32_t Extend(uint32_t crc, uint8_t* buffer, uint64_t size) {
      return crc32c_extend(crc, buffer, size);
    }

    uint32_t Value(uint8_t *buffer, uint64_t size) {
      return crc32c_value(buffer, size);
    }

    static const uint32_t kMaskDelta = 0xa282ead8ul;

    // Return a masked representation of crc.
    //
    // Motivation: it is problematic to compute the CRC of a string that
    // contains embedded CRCs.  Therefore we recommend that CRCs stored
    // somewhere (e.g., in files) should be masked before being stored.
    uint32_t Mask(uint32_t crc) {
      // Rotate right by 15 bits and add a constant.
      return ((crc >> 15) | (crc << 17)) + kMaskDelta;
    }

  }
}
