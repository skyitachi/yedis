//
// Created by Shiping Yao on 2023/4/19.
//
#include <spdlog/spdlog.h>
#include <gtest/gtest.h>
#include <thread>

#include <folly/MPMCQueue.h>

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

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::enable_backtrace(16);
  spdlog::set_pattern("[source %s] [function %!] [line %#] %v");

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
