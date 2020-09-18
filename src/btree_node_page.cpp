//
// Created by admin on 2020/9/18.
//
#include <btree_node_page.h>

namespace yedis {

void BTreeNodePage::init(int degree, page_id_t page_id) {
  if (IsLeafNode()) {
    // leaf_node init
  } else {
    // index_node_init
  }
}
}

