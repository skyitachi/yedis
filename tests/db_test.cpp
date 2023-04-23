//
// Created by skyitachi on 23-4-22.
//

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "db.h"
#include "options.h"

TEST(DBTest, Basic) {
  using namespace yedis;
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 24;
  options.compression = CompressionType::kNoCompression;
  DB* db;
  Status s = DB::Open(options, "ydb_demo", &db);
  ASSERT_TRUE(s.ok());

  WriteOptions w_opt;
  s = db->Put(w_opt, "a", "b");
  ASSERT_TRUE(s.ok());

  s = db->Put(w_opt, "abc", "cdefg");
  ASSERT_TRUE(s.ok());

  s = db->Put(w_opt, "a10", "cdefg");
  ASSERT_TRUE(s.ok());
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

