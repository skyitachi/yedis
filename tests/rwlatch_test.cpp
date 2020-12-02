#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <reader_writer_latch.h>

namespace yedis {
class Counter {
 public:
  Counter() = default;
  void Add(int num) {
    mutex.WLock();
    count_ += num;
    mutex.WUnLock();
  }
  int Read() {
    int res;
    mutex.RLock();
    res = count_;
    mutex.RUnLock();
    return res;
  }

 private:
  int count_{0};
  ReaderWriterLatch mutex{};
};

// NOLINTNEXTLINE
TEST(RWLatchTest, BasicTest) {
  int num_threads = 100;
  Counter counter{};
  counter.Add(5);
  std::vector<std::thread> threads;
  for (int tid = 0; tid < num_threads; tid++) {
    if (tid % 2 == 0) {
      threads.emplace_back([&counter]() { counter.Read(); });
    } else {
      threads.emplace_back([&counter]() { counter.Add(1); });
    }
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  EXPECT_EQ(counter.Read(), 55);
}

TEST(RWLatchTest, RWLatchTest) {
  using namespace std::chrono_literals;
  yedis::ReaderWriterLatch rw_latch;
  std::thread th1([&]{
    rw_latch.RLock();
    SPDLOG_INFO("in thread {}", 1);
    std::this_thread::sleep_for(1s);
    rw_latch.RUnLock();
  });

  std::thread th2([&]{
    rw_latch.RLock();
    SPDLOG_INFO("in thread {}", 2);
    std::this_thread::sleep_for(1s);
    rw_latch.RUnLock();
  });

  std::thread th3([&]{
    rw_latch.WLock();
    SPDLOG_INFO("got write latch");
    rw_latch.WUnLock();
  });

  std::thread th4([&] {
    rw_latch.WLock();
    SPDLOG_INFO("got write latch th4");
    rw_latch.WUnLock();
  });

  th1.join();
  th2.join();
  th3.join();
  th4.join();
}
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}