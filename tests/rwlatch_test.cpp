#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <reader_writer_latch.h>

TEST(RWLatchTest, RWLatchTest) {
  using namespace std::chrono_literals;
  yedis::ReaderWriterLatch rw_latch;
  std::thread th1([&]{
    rw_latch.RLatch();
    SPDLOG_INFO("in thread {}", 1);
    std::this_thread::sleep_for(1s);
    rw_latch.RUnLatch();
  });

  std::thread th2([&]{
    rw_latch.RLatch();
    SPDLOG_INFO("in thread {}", 2);
    std::this_thread::sleep_for(1s);
    rw_latch.RUnLatch();
  });

  std::thread th3([&]{
    rw_latch.Latch();
    SPDLOG_INFO("got write latch");
    rw_latch.UnLatch();
  });

  std::thread th4([&] {
    rw_latch.Latch();
    SPDLOG_INFO("got write latch th4");
    rw_latch.UnLatch();
  });

  th1.join();
  th2.join();
  th3.join();
  th4.join();
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}