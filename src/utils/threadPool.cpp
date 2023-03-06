#include "threadPool.h"

bool ThreadPool::initPool(ptrdiff_t _max_threads) {
  this->max_threads = _max_threads;
  this->running = true;
  bool success = true;
  // try {
    for (ptrdiff_t i = 0; i < max_threads; i++) {
      this->workers.emplace_back([=]() {
        while (true) {
          function<void()> task;
          {
            unique_lock<mutex> lock(this->queue_mutex);
            this->queue_condition.wait(lock, [this]() {
              return !this->running || !this->tasks.empty();
            });
            if (this->tasks.empty() || !this->running)
              return;
            task = std::move(this->tasks.front());
            this->tasks.pop();
          }
          task();
        }
      });
    }
  // } catch (...) {
  //   success = false;
  // }
  return success;
}

ThreadPool::~ThreadPool() {
  if (true) {
    lock_guard<mutex> lock(this->queue_mutex);
    this->running = false;
  }
  this->queue_condition.notify_all();
  for (ptrdiff_t i = 0; i < this->max_threads; i++)
    this->workers[i].join();
}