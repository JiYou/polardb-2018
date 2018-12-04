#include "include/engine.h"
#include "engine_race/engine_race.h"

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

int main(int argc, char **argv) {
  int fd = open(argv[1], O_RDONLY, 0644);
  // read 16 bytes every time.
  int cnt = -1;
  while (true) {
    ++cnt;
    struct disk_index di;
    if (read(fd, &di, sizeof(struct disk_index)) != sizeof(di)) {
      break;
    }
    if (!di.is_valid()) {
      break;
    }
    printf("%lu %d %d\n", di.get_key(), di.get_file_number(), di.get_offset());
  }

  std::cout << "read over: cnt = " << cnt << std::endl;
  close(fd);
  return 0;
}
