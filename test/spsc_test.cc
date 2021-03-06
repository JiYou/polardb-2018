#include "include/engine.h"
#include "engine_race/util.h"
#include "engine_race/engine_spsc.h"

#include <assert.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>

using namespace polar_race;

int main(void) {
  constexpr int size = 1000000;
  using spsc_queue = SPSCQueue<int>;
  spsc_queue q(size/1000+10);

  std::atomic<bool> flag{false};

  std::thread thd_push([&q] {
    for (int i = 0; i < size; i++) {
      q.push(i);
    }
  });

  unsigned long long sum = 0;
  std::thread thd_pop([&q, &sum] {
    for (int i = 0; i < size; i++) {
      while (!q.front());
      auto ret = *q.front();
      q.pop();
      sum += ret;
    }
  });

  thd_push.join();
  thd_pop.join();

  std::cout << sum << std::endl;
  return 0;
}
