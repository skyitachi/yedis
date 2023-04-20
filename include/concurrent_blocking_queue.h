//
// Created by Shiping Yao on 2023/4/20.
//

#ifndef YEDIS_CONCURRENT_BLOCKING_QUEUE_H
#define YEDIS_CONCURRENT_BLOCKING_QUEUE_H

#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>

namespace yedis {

template <typename T>
class ConcurrentBlockingQueue {
public:
  void put(const T& item) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [&] { return queue_.size() < max_size_; });
    queue_.push(item);
    not_empty_.notify_one();
  }

  T take() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [&] { return !queue_.empty();});
    T item = queue_.front();
    queue_.pop();
    not_full_.notify_one();
    return item;
  }

  std::vector<T> takeAll() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [&] { return !queue_.empty();});
    std::vector<T> result;
    while(!queue_.empty()) {
      result.push_back(queue_.front());
      queue_.pop();
    }
    not_full_.notify_one();
    return result;
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }
private:
  std::queue<T> queue_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  size_t max_size_ = std::numeric_limits<size_t>::max();

};

}
#endif //YEDIS_CONCURRENT_BLOCKING_QUEUE_H
