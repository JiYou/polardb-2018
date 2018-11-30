#include "include/engine.h"
#include "engine_race/util.h"
#include <assert.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <queue>
#include <vector>

using namespace polar_race;

class lock_q {
 public:
  void push(int x) {
    std::unique_lock<std::mutex> l(lock_);
    q_.emplace_back(x);
    cond_.notify_all();
  }
  int pop(void) {
    std::unique_lock<std::mutex> l(lock_);
    cond_.wait(l, [&] { return !q_.empty(); });
    auto ret = q_.back();
    q_.pop_back();
    return ret;
  }
 private:
  std::mutex lock_;
  std::condition_variable cond_;
  std::vector<int> q_;
};

int main(void) {
  lock_q q;
  constexpr int size = 1000000;
  std::thread thd_push([&q] {
    for (int i = 0; i < size; i++) {
      q.push(i);
    }
  });

  unsigned long long sum = 0;
  std::thread thd_pop([&q, &sum] {
    for (int i = 0; i < size; i++) {
      auto ret = q.pop();
      sum += ret;
    }
  });

  thd_push.join();
  thd_pop.join();

  std::cout << sum << std::endl;

  return 0;
}
