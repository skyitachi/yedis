
#ifndef YEDIS_READER_WRITER_LATCH_H_
#define YEDIS_READER_WRITER_LATCH_H_

#include <mutex>
#include <condition_variable>
#include <climits>

#include <spdlog/spdlog.h>

namespace yedis {

class ReaderWriterLatch {
  public:
    void WLock() {
      std::unique_lock<std::mutex> lk(mu_);
      // why need this, implement w-w mutex
      reader_cond_.wait(lk, [&]{return !writers_entered_;});
      writers_entered_ = true;
      writer_cond_.wait(lk, [&]{ return readers_cnt_ == 0;});
    }
    void WUnLock() {
      std::lock_guard<std::mutex> lk(mu_);
      writers_entered_ = false;
      reader_cond_.notify_all();
    }
    void RLock() {
      std::unique_lock<std::mutex> lk(mu_);
      reader_cond_.wait(lk, [&]{ return !writers_entered_ && readers_cnt_ < INT_MAX;});
      readers_cnt_++;
    }
    void RUnLock() {
      std::lock_guard<std::mutex> lock_guard(mu_);
      readers_cnt_--;
      if (writers_entered_) {
        if (readers_cnt_ == 0) {
          writer_cond_.notify_one();
        }
      } else {
        if (readers_cnt_ == INT_MAX - 1) {
          reader_cond_.notify_one();
        }
      }
    }
  private:
    std::mutex mu_;
    std::condition_variable reader_cond_;
    std::condition_variable writer_cond_;
    int readers_cnt_{0};
    bool writers_entered_{false};
};
}
#endif