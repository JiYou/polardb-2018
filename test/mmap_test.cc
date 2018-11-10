#include <assert.h>
#include <stdio.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>

static const char kEnginePath[] = "/tmp/test_engine";

constexpr uint64_t k4MB = 4096 * 1024;
constexpr uint64_t k16MB = k4MB * 4;
constexpr uint64_t kMaxIndexSize{2 * k16MB * 64ull};
constexpr uint64_t kBigFileSize{279172874240ull};

#define MAP_HUGE_1GB (30 << MAP_HUGE_SHIFT)

int main() {
  std::string file = std::string(kEnginePath) + "/" + "DB";
  int fd = open(file.c_str(), O_RDWR | O_DIRECT, 0644);
  if (fd < 0) {
    printf("open file failed!\n");
    return 0;
  }

  void* ptr = mmap(NULL, kMaxIndexSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_HUGE_1GB|MAP_NONBLOCK|MAP_POPULATE, fd, 0);
  if (ptr == MAP_FAILED) {
    printf("mmap failed!\n");
    close(fd);
    return 0;
  }

  char *buf = (char*)ptr;
  printf("[] = %c\n", buf[0]);


  munmap(ptr, kMaxIndexSize);

  close(fd);
  return 0;
}
