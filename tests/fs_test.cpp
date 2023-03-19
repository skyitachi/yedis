//
// Created by Shiping Yao on 2023/3/19.
//

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include "ydb/fs.hpp"
#include "allocator.h"
#include "ydb/file_buffer.h"
#include "ydb/log_format.h"
#include "random.h"
#include "test_util.h"

using namespace yedis;

TEST(FileSystemTest, Basic) {
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("wal.log", O_APPEND | O_CREAT | O_RDWR);
  ASSERT_TRUE(file_handle);
  std::string dest = "hello world";
  auto sz = file_handle->Write(dest.data(), dest.size());
  ASSERT_TRUE(sz == dest.size());
  char read_buf[dest.size() + 1];
  auto rd_sz = file_handle->Read(read_buf, dest.size(), 0);
  SPDLOG_INFO("write_size: {}, read_size: {}", sz, rd_sz);
  ASSERT_TRUE(sz == rd_sz);
  SPDLOG_INFO("read buf: {}", std::string(read_buf));
}

TEST(FileBufferTest, Basic) {
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("wal.log", O_APPEND | O_CREAT | O_RDWR | O_TRUNC);
  ASSERT_TRUE(file_handle);
  yedis::Allocator allocator;
  FileBuffer file_buffer(allocator, FileBufferType::BLOCK, wal::kBlockSize);
  ASSERT_EQ(file_buffer.size, wal::kBlockSize - wal::kHeaderSize);
  auto real_buf_size = wal::kBlockSize - wal::kHeaderSize;
  Random* rnd = new Random();
  std::string dest;
  test::RandomString(rnd, real_buf_size, &dest);
  memcpy(file_buffer.buffer, dest.data(), file_buffer.size);
  file_buffer.ChecksumAndWrite(*file_handle.get(), 0);

  std::string readBuffer;
  readBuffer.resize(file_buffer.size);
  auto rz = file_handle->Read(readBuffer.data(), file_buffer.size, wal::kHeaderSize);
  ASSERT_TRUE(rz == real_buf_size);
  ASSERT_TRUE(readBuffer == dest);
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
