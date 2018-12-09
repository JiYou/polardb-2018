#pragma once

#include <stdio.h>  // printf
#include <stdlib.h> // exit
#include <fcntl.h>
#include <unistd.h> // pipe
#include <string.h> // strlen
#include <pthread.h> // pthread_create

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>

#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>

#include <atomic>
#include <cassert>
#include <stdexcept>
#include <type_traits>

namespace polar_race {
class chan {
 public:
  void write() {
    if (-1 == fd[1]) {
      return;
    }
    if (::write(fd[1], &c, sizeof(c)) != sizeof(c)) {
      ; // add error msg ?
    }
  }
  void read() {
    if (-1 == fd[0]) {
      return;
    }
    if (::read(fd[0], &c, sizeof(c)) != sizeof(c)) {
      ; // add error msg?
    }
  }
  chan() { }
  void init() {
    if (pipe(fd) < 0) {
      std::cout << "create pipe meet error\n";
    }
  }
  ~chan() {
    if (fd[0] > 0) {
      close(fd[0]);
    }
    if (fd[1] > 0) {
      close(fd[1]);
    }
    fd[0] = fd[1] = -1;
  }
 private:
  int c = 'A';
  int fd[2] = {-1};
};

} // namespace polar_race
