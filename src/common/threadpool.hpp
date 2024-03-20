#pragma once

#include <deque>
#include <future>
#include <thread>
#include <vector>

namespace wing {

class ThreadPool {
 public:
  explicit ThreadPool(size_t thread_num = std::thread::hardware_concurrency()) {
    for (size_t i = 0; i < thread_num; i++) {
      pool_.emplace_back([&]() {
        while (!stop_signal_) {
          std::function<void()> F;
          {
            // Wait until there is an item that is ready to run.
            std::unique_lock lock(task_queue_mu_);
            if (task_queue_.empty()) {
              // If someone is waiting for all tasks to be completed.
              if (is_waiting_finish_ &&
                  !active_thread_num_.load(std::memory_order_relaxed)) {
                cv_wait_finish_.notify_all();
              }
            }
            if (task_queue_.empty() && !stop_signal_) {
              // wait for stop signal or new tasks.
              cv_.wait(lock,
                  [this]() { return !task_queue_.empty() || stop_signal_; });
            }

            if (!task_queue_.empty()) {
              // If we need to do work.
              F = std::move(task_queue_.front());
              task_queue_.pop_front();
            } else if (stop_signal_) {
              // If we have to stop.
              return;
            }
          }
          active_thread_num_ += 1;
          // Do work.
          F();
          active_thread_num_ -= 1;
        }
      });
    }
  }

  ~ThreadPool() {
    stop_signal_ = true;
    cv_.notify_all();
    for (auto& a : pool_) {
      a.join();
    }
  }

  // Wait for all tasks to finish.
  void WaitForAllTasks() {
    std::unique_lock lck(task_queue_mu_);
    if (task_queue_.empty() && !active_thread_num_) {
      return;
    }
    is_waiting_finish_ = true;
    cv_wait_finish_.wait(
        lck, [&]() { return task_queue_.empty() && !active_thread_num_; });
    is_waiting_finish_ = false;
  }

  size_t GetQueueLength() const { return task_queue_.size(); }

  void Push(std::function<void()>&& func) {
    // Push the task to task queue.
    std::unique_lock lck(task_queue_mu_);
    task_queue_.push_back(std::move(func));
    if (active_thread_num_.load(std::memory_order_relaxed) != pool_.size()) {
      cv_.notify_all();
    }
    return;
  }

  void Push(const std::function<void()>& func) {
    // Push the task to task queue.
    std::unique_lock lck(task_queue_mu_);
    task_queue_.push_back(func);
    if (active_thread_num_.load(std::memory_order_relaxed) != pool_.size()) {
      cv_.notify_all();
    }
    return;
  }

 private:
  std::vector<std::thread> pool_;

  /* The mutex for the task queue.*/
  std::mutex task_queue_mu_;

  /* Task queue. */
  std::deque<std::function<void()>> task_queue_;

  /* the condition variable to wake up threads. */
  std::condition_variable cv_;

  /* Number of threads that is doing task. */
  std::atomic<size_t> active_thread_num_{0};

  bool stop_signal_{false};

  /* Is someone is waiting in WaitForAllTasks() */
  bool is_waiting_finish_{false};
  std::condition_variable cv_wait_finish_;
};
}  // namespace wing
