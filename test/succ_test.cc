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

#define LOG(x) std::cout

namespace polar_race {
// begin of namespace polar_race

const char *file_name = "DATA_0";

struct aio_env {

  void SetFD(int fd_) {
    fd = fd_;
  }

  aio_env() {
    // prepare the io context.
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(kMaxIOEvent, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;

    memset(&iocb, 0, sizeof(iocb));
    iocbs = (struct iocb**) malloc(sizeof(struct iocb*) * kMaxIOEvent);
    for (int i = 0; i < kMaxIOEvent; i++) {
      iocbs[i] = iocb + i;
      iocb[i].aio_lio_opcode = IO_CMD_PREAD;
      iocb[i].aio_reqprio = 0;
      iocb[i].u.c.nbytes = kPageSize;
      // iocb->u.c.offset = offset;
      // iocb->aio_fildes = fd;
      // iocb->u.c.buf = buf;
    }
  }

  void PrepareRead(uint64_t offset, char *out, uint32_t size) {
    // prepare the io
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PREAD;
    index++;
  }

  void PrepareWrite(uint64_t offset, char *out, uint32_t size) {
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PWRITE;
    index++;
  }

  void Clear() {
    index = 0;
  }

  RetCode Submit() {
    if ((io_submit(ctx, index, iocbs)) != index) {
      DEBUG << "io_submit meet error, " << std::endl;;
      printf("io_submit error\n");
      return kIOError;
    }
    return kSucc;
  }

  void WaitOver() {
    // after submit, need to wait all read over.
    while (io_getevents(ctx, index, index,
                        events, &(timeout)) != index) {
      /**/
    }
  }

  ~aio_env() {
    io_destroy(ctx);
  }

  int fd = 0;
  int index = 0;
  io_context_t ctx;
  struct iocb iocb[kMaxIOEvent];
  // struct iocb* iocbs[kMaxIOEvent];
  struct iocb** iocbs = nullptr;
  struct io_event events[kMaxIOEvent];
  struct timespec timeout;
};
} // end of namespace polar_race

int main(void) {
  //aio_write_example();
  std::cout << sizeof(struct iocb) << std::endl;

  polar_race::aio_env ev;
  char *buf = polar_race::GetAlignedBuffer(polar_race::kPageSize * 1024 * 4);

  printf("addr = %lu\n", buf);

  for (int i = 0; i < 100; i ++) {
    buf[i] = 'c';
  }

  // write 1 page.
  int fd = open(polar_race::file_name, O_RDWR | O_DIRECT, 0644);
  if (fd < 0) {
    DEBUG << "open file failed!\n";
  }

  ev.SetFD(fd);
  ev.Clear();
  ev.PrepareWrite(0 /*offset*/, buf, polar_race::kPageSize);
  ev.Submit();
  ev.WaitOver();

  // read 1 page.
  ev.SetFD(fd);
  ev.Clear();
  ev.PrepareRead(0 /*offset*/, buf, polar_race::kPageSize);
  ev.Submit();
  ev.WaitOver();
  for (int i = 0; i < 10; i++) {
    std::cout << "[] = " << buf[i] << std::endl;
  }


  close(fd);
  free(buf);
  return 0;
}
