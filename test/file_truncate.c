#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


int main(void) {
  int fd = open("/tmp/dd", O_CREAT | O_RDWR, S_IWRITE | S_IREAD);
  ftruncate(fd, 1024*1024*100);
  lseek(fd, 10, SEEK_SET);
  write(fd, "aa", 2);
  close(fd);
  return 0;
}
