#include "engine_race/engine_aio.h"
#include "include/engine.h"
#include "include/polar_string.h"
#include "engine_race/util.h"

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
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <iostream>

using namespace polar_race;

int main(void)
{
  int fd, n;
/*
  fd = open("/tmp/dd", O_CREAT | O_WRONLY|O_NONBLOCK|O_DIRECT, 0644);
  if(fd<0) {
    std::cout << "open file meet error\n";
    exit(1);
  }
  char *buf = GetAlignedBuffer(17825792ull);
  for (uint64_t i = 0; i < 17825792ull; i++) {
    buf[i] = i%26 + 'a';
  }
  if (write(fd, buf, 17825792ull) != 17825792ull) {
    std::cout << "write file error\n";
  }
  close(fd);
*/
  char *buf = GetAlignedBuffer(17825792ull);
  int read_size = 16777216ull;
  memset(buf, 0, read_size);
  fd = open("/tmp/dd", O_RDONLY|O_DIRECT|O_NONBLOCK, 0644);

  auto read_file = [&]() -> int {
    int bytes = 0;
    while ((bytes = read(fd, buf, read_size)) < 0) {
      if (errno != EAGAIN) {
        DEBUG << "read filer meet error\n";
        return 0;
      }
    }
    return bytes;
  };

  std::cout << "1 " << read_file() << std::endl;
  std::cout << "2 " << read_file() << std::endl;
  std::cout << "3 " << read_file() << std::endl;

  close(fd);
  return 0;
}
