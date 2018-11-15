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
  fd = open("/tmp/dd", O_WRONLY|O_NONBLOCK|O_DIRECT, 0644);
  if(fd<0) {
    exit(1);
  }

  char *buf = GetAlignedBuffer(kPageSize);
  for (uint64_t i = 0; i < kPageSize; i++) {
    buf[i] = 'a';
  }
  write(fd, buf, kPageSize);

  close(fd);
  return 0;
}
