#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


int main(void) {
  int fd = open("/tmp/dd", O_CREAT | O_RDWR, S_IWRITE | S_IREAD);
  if (ftruncate(fd, 1024*1024*100) < 0) {
    printf("error of truncate()\n");
  }
  lseek(fd, 10, SEEK_SET);
  if (write(fd, "aa", 2) != 2) {
    printf("error write data!\n");
  }
  close(fd);
  return 0;
}
