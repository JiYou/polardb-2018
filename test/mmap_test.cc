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

void Read(char *buf, std::string *value) {
  char *a = buf + 4096;

  std::cout << "char*addr = " << static_cast<const void*>(a) << std::endl;
  std::string tmp(a, 4096);
  std::cout << "temp addr = " << static_cast<const void*>(tmp.data()) << std::endl;
  std::cout << "---swap---" << std::endl;
  value->swap(tmp);
  std::cout << "swap value addr = " << static_cast<const void*>(value->data()) << std::endl;
  std::cout << "temp addr = " << static_cast<const void*>(tmp.data()) << std::endl;
}

int main() {
  std::string file = std::string(kEnginePath) + "/" + "DB";
  int fd = open(file.c_str(), O_RDWR | O_DIRECT, 0644);
  if (fd < 0) {
    printf("open file failed!\n");
    return 0;
  }

  void* ptr = mmap(NULL, kBigFileSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_HUGE_1GB|MAP_NONBLOCK|MAP_POPULATE, fd, kMaxIndexSize);
  if (ptr == MAP_FAILED) {
    printf("mmap failed!\n");
    close(fd);
    return 0;
  }

  char *buf = (char*)ptr;
  printf("[] = %c\n", buf[4096<<12]);

  std::string value;
  std::cout << "define addr = " << static_cast<const void*>(value.data()) << std::endl;

  Read(buf, &value);

  std::cout << "called addr = " << static_cast<const void*>(value.data()) << std::endl;
  //std::cout << value << std::endl;


  munmap(ptr, kBigFileSize);

  close(fd);
  return 0;
}
