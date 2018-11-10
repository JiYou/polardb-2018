#include "engine_race/engine_aio.h"
#include "include/engine.h"
#include "include/polar_string.h"
#include "engine_race/util.h"
#include "engine_race/engine_race.h"

#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <iostream>

#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <iostream>

#define LOG(x) std::cout
#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

namespace polar_race {
// begin of namespace polar_race

const char *file_name = "/tmp/test_engine/DB";

} // end of namespace polar_race

int main(void) {
  //aio_write_example();
  std::cout << "struct iocb size = " << sizeof(struct iocb) << std::endl;

  polar_race::aio_env ev;
  char *buf = polar_race::GetAlignedBuffer(polar_race::kPageSize);

  // read 1 page.

  int fd_ = open("DATA_0", O_RDWR | O_DIRECT, 0644);

  if (fd_ < 0 && errno == ENOENT) {
    fd_ = open("DATA_0", O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd_ < 0) {
      DEBUG << "create big file failed!\n";
      return 0;
    }
    // if open success.
    if (posix_fallocate(fd_, 0, 100 * 1024 * 1024)) {
      DEBUG << "posix_fallocate failed\n";
      return 0;
    }
  }

  ev.SetFD(fd_);

  auto single_write = [&]() {
    char buf[4096];
    for (int i = 0; i < 4096; i++) {
      buf[i] = i % 26 + 'a';
    }
    ev.Clear();
    ev.PrepareWrite(0, buf, 4096);
    ev.Submit();
    ev.WaitOver();
  };
  single_write();

  return 0;

  auto read_test = [&](uint64_t offset, uint64_t size) {
    ev.PrepareRead(offset, buf, size);
    ev.Submit();
    ev.WaitOver();
    ev.Clear();
    for (int i = 0; i < 10; i++) {
      std::cout << buf[i];
    }
    std::cout << std::endl;
  };

  read_test(0, polar_race::kPageSize);

  uint64_t sum_char = 0;
  char backup[4096];
  for (uint32_t i = 0; i < polar_race::kPageSize; i++) {
    sum_char += buf[i];
    backup[i] = buf[i];
  }

  read_test(1024, 1024);


  // read 1KB, 1KB, 2KB into buf.
  auto two_read_request = [&]() {
    char *a = buf, *b = buf + 1024;
    char *c = buf + 2048;
    ev.PrepareRead(0, a, 1024); // read first 1KB
    ev.PrepareRead(1024, b, 1024); // read second 1KB
    ev.PrepareRead(2048, c, 2048); // read 3,4 1KB
    ev.Submit();
    ev.WaitOver();
    ev.Clear();
  };

  memset(buf, 0, polar_race::kPageSize);
  two_read_request();

  for (uint32_t i = 0; i < polar_race::kPageSize; i++) {
    sum_char -= buf[i];
    if (buf[i] != backup[i]) {
      std::cout << "meet error" << std::endl;
    }
  }
  if (sum_char) {
    std::cout << "meet error" << std::endl;
  } else {
    std::cout << "read success" << std::endl;
  }

  char *write_content = polar_race::GetAlignedBuffer(polar_race::kPageSize);
  memcpy(write_content, backup, polar_race::kPageSize);
  // read 1KB, 1KB, 2KB into buf.
  auto write_request = [&]() {
    char *a = buf, *b = buf + 1024;
    char *c = buf + 2048;
    ev.PrepareWrite(polar_race::kPageSize, write_content, polar_race::kPageSize);
    ev.Submit();
    ev.WaitOver();
    ev.Clear();
  };
  write_request();

  close(fd_);
  free(buf);
  return 0;
}
