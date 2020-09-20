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

static constexpr int MAX_DEGREE = (PAGE_SIZE - 32) / 8;

// page format
static constexpr int ENTRY_COUNT_OFFSET = 4;
static constexpr int DEGREE_OFFSET = 8;
static constexpr int FLAG_OFFSET = 12;
static constexpr int PARENT_OFFSET = 13;
static constexpr int KEY_POS_OFFSET = 17;
static constexpr int ENTRY_OFFSET =  KEY_POS_OFFSET + (4 * MAX_DEGREE - 1) * sizeof(uint32_t);

}
#endif //YEDIS_INCLUDE_CONFIG_HPP_
