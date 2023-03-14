//
// Created by Shiping Yao on 2023/3/14.
//

#ifndef YEDIS_CONSTANTS_H
#define YEDIS_CONSTANTS_H
#include <cstdint>

namespace yedis {
using idx_t = uint64_t;

using data_t = uint8_t;

typedef data_t *data_ptr_t;

typedef const data_t *const_data_ptr_t;

}

#endif //YEDIS_CONSTANTS_H
