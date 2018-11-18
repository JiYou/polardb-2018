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
  char *buf = GetAlignedBuffer(16777216);
  memset(buf, 0, 16777216ull);
  fd = open("/tmp/write.dat", O_RDONLY|O_DIRECT|O_NONBLOCK, 0644);
  while (read(fd, buf, 16777216ull) < 0) {
    if (errno == EAGAIN) {
      for (int i = 0; i < 10; i ++) {
        printf("%c", buf[i]);
      }
      printf("\n");
    } else {
      printf("read error\n");
    }
  }
  for (int i = 0; i < 10; i ++) {
    printf("%c", buf[i]);
  }
  printf("read over\n");
  close(fd);
  return 0;
}
