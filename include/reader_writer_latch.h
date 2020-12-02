
#ifndef YEDIS_READER_WRITER_LATCH_H_
#define YEDIS_READER_WRITER_LATCH_H_

#include <mutex>
#include <condition_variable>

#include <spdlog/spdlog.h>

namespace yedis {

class ReaderWriterLatch {
  public:
    void Latch() {
      std::unique_lock<std::mutex> lk(mu_);
      SPDLOG_INFO("acquire lock and waiting: readers {}", readers_);
      writers_++;
      cond_.wait(lk, [&]{return readers_ == 0;});
      lk.release();
    }
    void UnLatch() {
      writers_--;
      if (writers_ > 0) {
        cond_.notify_one();
      }
      mu_.unlock();
    }
    void RLatch() {
      std::lock_guard<std::mutex> lock_guard(mu_);
      readers_++;
    }
    void RUnLatch() {
      std::lock_guard<std::mutex> lock_guard(mu_);
      readers_--;
      if (readers_ == 0) {
          cond_.notify_one();
      }
    }
  private:
    std::mutex mu_;
    std::condition_variable cond_;
    int readers_ = 0;
    int writers_ = 0;
};
}
#endif