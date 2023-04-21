//
// Created by skyitachi on 2020/10/22.
//

#ifndef YEDIS_INCLUDE_OPTION_HPP_
#define YEDIS_INCLUDE_OPTION_HPP_

namespace yedis {
struct BTreeOptions {

  // page_size for btree
  uint32_t page_size = 4096;

};

namespace config {

  static const int kNumLevels = 7;

}
}
#endif //YEDIS_INCLUDE_OPTION_HPP_
