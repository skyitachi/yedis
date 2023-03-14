//
// Created by Shiping Yao on 2023/3/14.
//

#ifndef YEDIS_CHECKSUM_H
#define YEDIS_CHECKSUM_H
#include <cstdint>

namespace yedis {
  uint32_t Checksum(uint8_t* buffer, uint64_t size);
}

#endif //YEDIS_CHECKSUM_H
