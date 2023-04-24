//
// Created by skyitachi on 23-4-22.
//
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <filesystem>

#include "db.h"
#include "options.h"

TEST(DBTest, Basic) {
  using namespace yedis;
  namespace fs = std::filesystem;
  std::string db_name = "ydb_demo";
  fs::remove_all(db_name);
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 24;
  options.compression = CompressionType::kNoCompression;
  DB* db;
  Status s = DB::Open(options, db_name, &db);
  ASSERT_TRUE(s.ok());

  WriteOptions w_opt;
  s = db->Put(w_opt, "a", "b");
  ASSERT_TRUE(s.ok());

  s = db->Put(w_opt, "abc", "cdefg");
  ASSERT_TRUE(s.ok());

  // first compaction
  s = db->Put(w_opt, "a10", "cdefg");
  ASSERT_TRUE(s.ok());

  s = db->Put(w_opt, "a12", "abcdefg");
  ASSERT_TRUE(s.ok());

  s = db->Put(w_opt, "a13", "abcdefg");
  ASSERT_TRUE(s.ok());

  s = db->Put(w_opt, "a14", "abcdefg");
  ASSERT_TRUE(s.ok());

  ReadOptions ropt;
  std::string value;
  s = db->Get(ropt, "a", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "b");

  s = db->Get(ropt, "a14", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "abcdefg");

  s = db->Get(ropt, "none_key", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete db;
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

