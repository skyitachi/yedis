//
// Created by Shiping Yao on 2023/3/14.
//

#ifndef YEDIS_LOG_FORMAT_H
#define YEDIS_LOG_FORMAT_H

namespace yedis {
namespace wal {
  enum class RecordType:uint8_t {
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4
  };

  static const int kMaxRecordType = int(RecordType::kLastType);

  static const int kBlockSize = 1 << 15;

  static const int kHeaderSize = 4 + 2 + 1;
  static const int kTypeHeaderSize = 3;
  static const int kBlockLenSize = 2;
}
}
#endif //YEDIS_LOG_FORMAT_H
