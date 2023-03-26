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
#include "ydb/wal.h"

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
  file_buffer.Append(*file_handle.get());

  std::string readBuffer;
  readBuffer.resize(file_buffer.size);
  auto rz = file_handle->Read(readBuffer.data(), file_buffer.size, wal::kHeaderSize);
  ASSERT_TRUE(rz == real_buf_size);
  ASSERT_TRUE(readBuffer == dest);
}


TEST(WALTest, Basic) {
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("wal.log", O_APPEND | O_CREAT | O_RDWR | O_TRUNC);
  ASSERT_TRUE(file_handle);
  wal::Writer log_writer(*file_handle.get());
  Random* rnd = new Random();
  int sm_size = 100;
  std::string small;
  test::RandomString(rnd, sm_size, &small);

  int parts = wal::kBlockSize / (sm_size + wal::kHeaderSize);

  for (int i = 0; i < parts; i++) {
    auto s = log_writer.AddRecord(Slice(small));
    ASSERT_TRUE(s.ok());
  }

  auto f_sz = file_handle->FileSize();
  auto real_sz = parts * (sm_size + wal::kHeaderSize);
  ASSERT_EQ(f_sz, real_sz);
  spdlog::info("file_sz: {}", f_sz);

  {
    auto data = Slice(small);
    auto s = log_writer.AddRecord(data);
    ASSERT_TRUE(s.ok());
    f_sz = file_handle->FileSize();
    real_sz += (2 * wal::kHeaderSize) + sm_size;
    ASSERT_EQ(f_sz, real_sz);
    spdlog::info("file_sz: {}", f_sz);
  }

  auto read_file_handle = fs.OpenFile("wal.log", O_RDONLY);
  wal::Reader reader(file_handle);
}

TEST(WALTest, Padding) {
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("wal.log", O_APPEND | O_CREAT | O_RDWR | O_TRUNC);
  ASSERT_TRUE(file_handle);
  wal::Writer log_writer(*file_handle.get());
  Random* rnd = new Random();
  int sm_size = 32760;
  std::string small;
  test::RandomString(rnd, sm_size, &small);

  int parts = wal::kBlockSize / (sm_size + wal::kHeaderSize);

  for (int i = 0; i < parts; i++) {
    auto s = log_writer.AddRecord(Slice(small));
    ASSERT_TRUE(s.ok());
  }

  auto f_sz = file_handle->FileSize();
  auto real_sz = wal::kBlockSize;
  ASSERT_EQ(f_sz, real_sz);
  spdlog::info("file_sz: {}", f_sz);
}

TEST(WALTest, BigSlice) {
  LocalFileSystem fs;
  auto file_handle = fs.OpenFile("wal.log", O_APPEND | O_CREAT | O_RDWR | O_TRUNC);
  ASSERT_TRUE(file_handle);
  wal::Writer log_writer(*file_handle.get());
  Random* rnd = new Random();
  int big_size = 32768 * 2;
  std::string big;
  test::RandomString(rnd, big_size, &big);



}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
