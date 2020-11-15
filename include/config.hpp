//
// Created by skyitachi on 2020/8/22.
//

#ifndef YEDIS_INCLUDE_CONFIG_HPP_
#define YEDIS_INCLUDE_CONFIG_HPP_

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace yedis {
/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
extern std::chrono::milliseconds cycle_detection_interval;

/** True if logging should be enabled, false otherwise. */
extern std::atomic<bool> enable_logging;

/** If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT. */
extern std::chrono::duration<int64_t> log_timeout;

static constexpr int INVALID_PAGE_ID = -1;                                    // invalid page id
static constexpr int INVALID_TXN_ID = -1;                                     // invalid transaction id
static constexpr int INVALID_LSN = -1;                                        // invalid log sequence number
static constexpr int HEADER_PAGE_ID = 0;                                      // the header page id
static constexpr int PAGE_SIZE = 4096;                                        // size of a data page in byte
static constexpr int BUFFER_POOL_SIZE = 10;                                   // size of buffer pool
static constexpr int LOG_BUFFER_SIZE = ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE);  // size of a log buffer in byte
static constexpr int BUCKET_SIZE = 50;                                        // size of extendible hash bucket

using frame_id_t = int32_t;    // frame id type
using page_id_t = int32_t;     // page id type
using txn_id_t = int32_t;      // transaction id type
using lsn_t = int32_t;         // log sequence number type
using slot_offset_t = size_t;  // slot offset type
using oid_t = uint16_t;

constexpr int get_degree(int page_size) {
  return (page_size / 32) / 2 / (sizeof(page_id_t) + sizeof(int64_t));
}

static constexpr int MAX_DEGREE = get_degree(PAGE_SIZE);

// page format
static constexpr int HEADER_SIZE = 21;
static constexpr int PAGE_ID_OFFSET = 0;
static constexpr int FLAG_OFFSET = 4;
static constexpr int ENTRY_COUNT_OFFSET = 5;
static constexpr int DEGREE_OFFSET = 9;
static constexpr int AVAILABLE_OFFSET = 13;
static constexpr int PARENT_OFFSET = 17;
static constexpr int KEY_POS_OFFSET = 21;

// leaf node
static constexpr int LEAF_HEADER_SIZE = 29;
static constexpr int PREV_NODE_PAGE_ID_OFFSET = 21;
static constexpr int NEXT_NODE_PAGE_ID_OFFSET = 25;
static constexpr int ENTRY_OFFSET = 29;

// buffer pool manager
static constexpr int MinPoolSize = 7;
}
#endif //YEDIS_INCLUDE_CONFIG_HPP_
