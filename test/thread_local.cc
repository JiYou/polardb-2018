#include <iostream>
#include <thread>
#include <unordered_map>
#include <stdio.h>

class TestThreadLocal {
 public:
  void Read() {
    static thread_local char *buf = nullptr;
    static thread_local std::unordered_map<int, int> fd_cache_;
    constexpr int kPageSize = 4096;
    if (!buf) {
      if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
        printf("posix_memalign failed!\n");
      } else {
        printf("alloc success\n");
      }
    }
    printf("%lu\n", buf);
  }
};

int main(void) {
  TestThreadLocal t;
  auto f = [&]() {
    t.Read();
  };


  std::thread thd(f);
  std::thread thd2(f);
  std::thread thd3(f);
  thd.join();
  thd2.join();
  thd3.join();

  return 0;
}
