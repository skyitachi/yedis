//
// Created by Shiping Yao on 2023/4/19.
//
#include <spdlog/spdlog.h>
#include <gtest/gtest.h>
#include <thread>

#include <folly/MPMCQueue.h>

#include "concurrent_blocking_queue.h"

TEST(FollyConcurrentQueue, Basic) {
  folly::MPMCQueue<int> queue(10);

  std::thread producer_th([&]{
    for (int i = 0; i < 100; i++) {
      while(!queue.write(i)) {

      }
    }
  });

  std::thread consumer_th([&] {
    for (int i = 0; i < 100; i++) {
      int value;
      while(!queue.read(value)) {

      }
      std::cout << "consumed: " << value << std::endl;
    }
  });

  producer_th.join();
  consumer_th.join();
}

TEST(ConcurrentBlockingQueue, Basic) {
  using namespace yedis;
  ConcurrentBlockingQueue<int> queue;
  std::thread producer_th([&]{
    for (int i = 0; i < 100; i++) {
      queue.put(i);
    }
  });
  std::vector<int> results;

  std::thread consumer_th([&] {

    for (int i = 0; i < 100; i++) {
      int value = queue.take();
      results.push_back(value);
    }
  });

  producer_th.join();
  consumer_th.join();

  ASSERT_EQ(results.size(), 100);
  for(int i = 0; i < results.size(); i++) {
    ASSERT_EQ(i, results[i]);
  }

}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
