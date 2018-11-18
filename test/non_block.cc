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

  char *buf = GetAlignedBuffer(16777216);
/*
  for (uint64_t i = 0; i < 16777216; i++) {
    buf[i] = 'a';
  }
  write(fd, buf, 16777216);
*/
  close(fd);

  memset(buf, 0, kPageSize);
  fd = open("/tmp/dd", O_RDONLY|O_DIRECT|O_NONBLOCK, 0644);
  while (read(fd, buf, kPageSize) < 0) {
    if (errno == EAGAIN) {
      for (int i = 0; i < 10; i ++) {
        printf("%c", buf[i]);
      }
      printf("\n");
    } else {
      printf("read error\n");
    }
  }
  printf("read over\n");
  close(fd);
  return 0;
}
