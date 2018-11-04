#include "engine_race/libaio.h"
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
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <iostream>

#define LOG(x) std::cout
#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

namespace polar_race {
// begin of namespace polar_race

const char *file_name = "DATA_0";

struct aio_env {
  aio_env() {
    // prepare the io context.
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(kSingleRequest, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;

    iocbs = &iocb;
    memset(&iocb, 0, sizeof(iocb));
    // iocb->aio_fildes = fd;
    iocb.aio_lio_opcode = IO_CMD_PREAD;
    iocb.aio_reqprio = 0;
    // iocb->u.c.buf = buf;
    iocb.u.c.nbytes = kPageSize;
    // iocb->u.c.offset = offset;
  }

  void Prepare(int fd, uint64_t offset, char *out, uint32_t size) {
    // prepare the io
    iocb.aio_fildes = fd;
    iocb.u.c.offset = offset;
    iocb.u.c.buf = out;
    iocb.u.c.nbytes = size;
  }

  RetCode Submit() {
    if ((io_submit(ctx, kSingleRequest, &iocbs)) != kSingleRequest) {
      DEBUG << "io_submit meet error, " << std::endl;;
      printf("io_submit error\n");
      return kIOError;
    }
    return kSucc;
  }

  void WaitOver() {
    // after submit, need to wait all read over.
    while (io_getevents(ctx, kSingleRequest, kSingleRequest,
                      &(events), &(timeout)) != kSingleRequest) {
      /**/
    }
  }

  ~aio_env() {
    io_destroy(ctx);
  }

  io_context_t ctx;
  struct iocb iocb;
  struct iocb* iocbs;
  struct io_event events;
  struct timespec timeout;
};

} // end of namespace polar_race

int main(void) {
  //aio_write_example();
  std::cout << sizeof(struct iocb) << std::endl;

  polar_race::aio_env ev;
  char *buf = polar_race::GetAlignedBuffer(polar_race::kPageSize);

  // read 1 page.
  int fd = open(polar_race::file_name, O_RDWR | O_DIRECT, 0644);
  if (fd < 0) {
    DEBUG << "open file failed!\n";
  }

  ev.Prepare(fd, 0 /*offset*/, buf, polar_race::kPageSize);
  ev.Submit();
  ev.WaitOver();

  for (int i = 0; i < 10; i++) {
    std::cout << "[] = " << buf[i] << std::endl;
  }

  close(fd);
  free(buf);
  return 0;
}
