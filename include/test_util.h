//
// Created by admin on 2020/10/15.
//

#ifndef YEDIS_INCLUDE_TEST_UTIL_H_
#define YEDIS_INCLUDE_TEST_UTIL_H_

#include "random.h"
#include "btree.hpp"

namespace yedis {
namespace test {

Slice RandomString(Random* rnd, int len, std::string *dst);
}
}
#endif //YEDIS_INCLUDE_TEST_UTIL_H_
