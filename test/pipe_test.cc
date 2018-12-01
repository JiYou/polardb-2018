#include <stdio.h>  // printf
#include <stdlib.h> // exit
#include <unistd.h> // pipe
#include <string.h> // strlen
#include <pthread.h> // pthread_create

#include <thread>
#include <iostream>

using namespace std;

#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"(): "<<" [" <<strerror(errno) << "] "
/*
class chan {
 public:
  void send() {
    if (write(fd[1], &c, sizeof(c)) != sizeof(c)) {
      DEBUG << "write fd[0] meet error\n";
    }
  }
  void recv() {
    if (read(fd[0], &c, sizeof(c)) != sizeof(c)) {
      DEBUG << "read fd[1] meet error\n";
    }
  }
  chan() { }
  void init() {
    if (pipe(fd) < 0) {
      std::cout << "create pipe meet error\n";
    }
    DEBUG << "fd[0] = " << fd[0] << std::endl;
    DEBUG << "fd[1] = " << fd[1] << std::endl;
  }
  ~chan() {
    if (fd[0] > 0) {
      close(fd[0]);
    }
    if (fd[1] > 0) {
      close(fd[1]);
    }
  }
 private:
  int c = 0;
  int fd[2] = {-1};
};
*/

class chan {
 public:
  void write() {
    if (::write(fd[1], &c, sizeof(c)) != sizeof(c)) {
      std::cout << "write fd[0] meet error\n";
    }
  }
  void read() {
    if (::read(fd[0], &c, sizeof(c)) != sizeof(c)) {
      std::cout << "read fd[1] meet error\n";
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
  }
 private:
  int c = 'A';
  int fd[2] = {-1};
};
int main() {
  chan ch;
  ch.init();
  std::thread thd_read([&] { ch.read(); });
  std::thread thd_write([&] { ch.write(); });
  thd_read.join();
  thd_write.join();
  return 0;
}
