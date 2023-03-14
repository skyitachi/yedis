//
// Created by admin on 2020/10/15.
//

#ifndef YEDIS_INCLUDE_RANDOM_H_
#define YEDIS_INCLUDE_RANDOM_H_

#include <random>
#include <limits>

namespace yedis {
class Random {
 public:
  Random() {
    e_ = std::default_random_engine(r_());
    uniform_dist_ = std::uniform_int_distribution<uint32_t>(0, M);
    int64_uniform_dist_ = std::uniform_int_distribution<int64_t>(0, std::numeric_limits<int64_t>::max());
  }

  uint32_t Next() {
    return uniform_dist_(e_);
  }

  int64_t NextInt64() {
    return int64_uniform_dist_(e_);
  }

  // return random [0, max), except for max = 0
  uint32_t IntN(uint32_t max) {
    if (max == 0) {
      return max;
    }
    return uniform_dist_(e_) % max;
  }

 private:
  static const uint32_t M = 2147483647L;
  std::random_device r_;
  std::default_random_engine e_;
  std::uniform_int_distribution<uint32_t> uniform_dist_;
  std::uniform_int_distribution<int64_t> int64_uniform_dist_;
};
}
#endif //YEDIS_INCLUDE_RANDOM_H_
