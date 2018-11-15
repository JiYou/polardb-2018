#include "include/engine.h"

#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";

using namespace polar_race;

int main() {
  struct disk_index {
    uint64_t key;
    uint32_t file_no;
    uint32_t file_offset;
  };
  int fd = open("/tmp/test_engine/DB", O_RDONLY, 0644);
  // read 16 bytes every time.
  struct disk_index di;
  int cnt = 0;
  while (true) {
    ++cnt;
    di.file_no = di.file_offset = 0;
    if (read(fd, &di, 16) != 16) {
      break;
    }
    if (di.file_no == 0xffff && di.file_offset == 0xffff) {
      continue;
    }

    if (di.file_no == 0 && di.file_offset == 0 && di.key == 0) {
      break;
    }
    cnt++;
    std::cout << di.key << " = " << di.file_no << ":" << di.file_offset << std::endl;
  }
  std::cout << "read over: cnt = " << cnt << std::endl;
  close(fd);
  return 0;
}
