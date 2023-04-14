//
// Created by Shiping Yao on 2023/4/14.
//

#include "options.h"
#include "comparator.h"

namespace yedis {
  Options::Options() : comparator(BytewiseComparator()) {}
}
