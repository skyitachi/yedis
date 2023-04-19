//
// Created by skyitachi on 2020/10/15.
//

#include <test_util.h>

namespace yedis {
namespace test{
Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>('a' + rnd->IntN(26));
  }
  return Slice(*dst);
}
}
}