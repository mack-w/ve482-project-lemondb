// Created by Mack on 2022-11-17

#ifndef PROJECT_THREAD_POOL_H
#define PROJECT_THREAD_POOL_H 1

#include <climits>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

using namespace std;

class ThreadPool {
private:
  vector<thread> workers;
  queue<function<void()>> tasks;
  mutex queue_mutex;
  condition_variable queue_condition;
  ptrdiff_t max_threads;
  bool running = false;

public:
  bool initPool(ptrdiff_t _max_threads);
  template <typename F, typename... Args>
  auto enqueueTask(F &&f, Args &&...args) -> std::future<decltype(f(args...))>;

public:
  ThreadPool(){};
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  ~ThreadPool();
};

template <typename F, typename... Args>
auto ThreadPool::enqueueTask(F &&f, Args &&...args)
    -> std::future<decltype(f(args...))> {
  auto task = std::make_shared<packaged_task<decltype(f(args...))()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  std::future<decltype(f(args...))> res = task->get_future();
  if (true) {
    unique_lock<mutex> lock(this->queue_mutex);
    this->tasks.emplace([task]() { (*task)(); });
  }
  this->queue_condition.notify_one();
  return res;
}

#endif // threadPool.h