#include "include/engine.h"
#include "engine_race/util.h"

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
  {

    struct disk_index {
      char key[8];
      uint64_t pos;
    };
    int fd = open("/tmp/test_engine/DB", O_RDONLY, 0644);
    // read 16 bytes every time.
    struct disk_index di;
    int cnt = 0;
    while (true) {
      ++cnt;
      di.pos = 0;
      if (read(fd, &di, 16) != 16) {
        break;
      }
      constexpr uint64_t kIndexSkipType{18446744073709551615ull};
      if (di.pos == kIndexSkipType) continue;
      if (di.pos == 0) break;
      printf("%8s", di.key);
      std::cout << " " << di.pos << std::endl;
    }
    std::cout << "read over: cnt = " << cnt << std::endl;
    close(fd);
    return 0;
  }
}
