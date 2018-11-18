#include "engine_race/engine_aio.h"
#include "include/engine.h"
#include "include/polar_string.h"
#include "engine_race/util.h"
#include "engine_race/engine_race.h"

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

namespace polar_race {
// begin of namespace polar_race

const char *file_name = "/tmp/test_engine/DB";

struct aio_env_two {
  static constexpr int two_event = 2;
  aio_env_two() {
    // prepare the io context.
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(two_event, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;

    memset(&iocb, 0, sizeof(iocb));
    iocbs = (struct iocb**) malloc(sizeof(struct iocb*) * two_event);
    for (int i = 0; i < two_event; i++) {
      iocbs[i] = iocb + i;
      //iocb[i].aio_lio_opcode = IO_CMD_PREAD;
      iocb[i].aio_reqprio = 0;
      // iocb[i].u.c.nbytes = kPageSize;
      // iocb->u.c.offset = offset;
      // iocb->aio_fildes = fd;
      // iocb->u.c.buf = buf;
    }

    index_buf = GetAlignedBuffer(k1KB);  // 1KB for write index.
    if (!index_buf) {
      DEBUG << "aligned memory for aio_2 index failed\n";
    }
    data_buf = GetAlignedBuffer(kPageSize); // 4KB for write data.
    if (!data_buf) {
      DEBUG << "ailgned memory for aio_2 data failed\n";
    }
  }

  void PrepareRead(int fd, uint64_t offset, char *out, uint32_t size, wait_item* item=nullptr) {
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PREAD;
    iocb[index].data = item;
    index++;
  }

  void PrepareWrite(int fd, uint64_t offset, char *out, uint32_t size, wait_item *item=nullptr) {
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PWRITE;
    iocb[index].data = item;
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
    int write_over_cnt = 0;
    while (write_over_cnt != index) {
      int num_events = io_getevents(ctx, 1, index, events, &timeout);
      write_over_cnt += num_events;
    }
  }

  ~aio_env_two() {
    io_destroy(ctx);
    free(index_buf);
    free(data_buf);
  }

  char *index_buf = nullptr;
  char *data_buf = nullptr;
  int index = 0;
  io_context_t ctx;
  struct iocb iocb[two_event];
  struct iocb** iocbs = nullptr;
  struct io_event events[two_event];
  struct timespec timeout;
};


// it just set env for aio.
// Usage:
// struct aio_env io_;
// io_.SetFD(fd);
// io_.PrepareRead();
// io_.PrepareRead();
// io_.Submit();
// io_.WaitOver();
// io_.Clear();
// this is just used in read read queue operations.
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
      //iocb[i].aio_lio_opcode = IO_CMD_PREAD;
      iocb[i].aio_reqprio = 0;
      // iocb[i].u.c.nbytes = kPageSize;
      // iocb->u.c.offset = offset;
      // iocb->aio_fildes = fd;
      // iocb->u.c.buf = buf;
    }
  }

  void PrepareRead(uint64_t offset, char *out, uint32_t size, wait_item* item=nullptr) {
    // align with 4 kb
    assert ((((uint64_t)out) & 4095) == 0);
    assert ((size & 1023) == 0);
    // prepare the io
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PREAD;
    iocb[index].data = item;
    index++;
  }

  void PrepareWrite(uint64_t offset, char *out, uint32_t size, wait_item *item=nullptr) {
    assert ((((uint64_t)out) & 4095) == 0);
    assert ((size & 1023) == 0);

    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PWRITE;
    iocb[index].data = item;
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
    int write_over_cnt = 0;
    while (write_over_cnt != index) {
      constexpr int min_number = 1;
      int num_events = io_getevents(ctx, min_number, index, events, &timeout);
      assert (num_events >= 0);
      for (int i = 0; i < num_events; i++) {
        wait_item *feed_back = reinterpret_cast<wait_item*>(events[i].data);
        if (feed_back) {
          feed_back->feed_back();
        }
      }
      // need to call for (i = 0; i < num_events; i++) events[i].call_back();
      write_over_cnt += num_events;
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
  std::cout << "struct iocb size = " << sizeof(struct iocb) << std::endl;

  polar_race::aio_env ev;
  char *buf = polar_race::GetAlignedBuffer(polar_race::kPageSize);

  // read 1 page.

  int fd_ = open("DATA_0", O_RDWR | O_DIRECT, 0644);

  if (fd_ < 0 && errno == ENOENT) {
    fd_ = open("DATA_0", O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd_ < 0) {
      DEBUG << "create big file failed!\n";
      return 0;
    }
    // if open success.
    if (posix_fallocate(fd_, 0, 100 * 1024 * 1024)) {
      DEBUG << "posix_fallocate failed\n";
      return 0;
    }
  }

  ev.SetFD(fd_);

  auto single_write = [&]() {
    char buf[4096];
    for (int i = 0; i < 4096; i++) {
      buf[i] = i % 26 + 'a';
    }
    ev.Clear();
    ev.PrepareWrite(0, buf, 4096);
    ev.Submit();
    ev.WaitOver();
  };
  single_write();

  return 0;

  auto read_test = [&](uint64_t offset, uint64_t size) {
    ev.PrepareRead(offset, buf, size);
    ev.Submit();
    ev.WaitOver();
    ev.Clear();
    for (int i = 0; i < 10; i++) {
      std::cout << buf[i];
    }
    std::cout << std::endl;
  };

  read_test(0, polar_race::kPageSize);

  uint64_t sum_char = 0;
  char backup[4096];
  for (uint32_t i = 0; i < polar_race::kPageSize; i++) {
    sum_char += buf[i];
    backup[i] = buf[i];
  }

  read_test(1024, 1024);


  // read 1KB, 1KB, 2KB into buf.
  auto two_read_request = [&]() {
    char *a = buf, *b = buf + 1024;
    char *c = buf + 2048;
    ev.PrepareRead(0, a, 1024); // read first 1KB
    ev.PrepareRead(1024, b, 1024); // read second 1KB
    ev.PrepareRead(2048, c, 2048); // read 3,4 1KB
    ev.Submit();
    ev.WaitOver();
    ev.Clear();
  };

  memset(buf, 0, polar_race::kPageSize);
  two_read_request();

  for (uint32_t i = 0; i < polar_race::kPageSize; i++) {
    sum_char -= buf[i];
    if (buf[i] != backup[i]) {
      std::cout << "meet error" << std::endl;
    }
  }
  if (sum_char) {
    std::cout << "meet error" << std::endl;
  } else {
    std::cout << "read success" << std::endl;
  }

  char *write_content = polar_race::GetAlignedBuffer(polar_race::kPageSize);
  memcpy(write_content, backup, polar_race::kPageSize);
  // read 1KB, 1KB, 2KB into buf.
  auto write_request = [&]() {
    char *a = buf, *b = buf + 1024;
    char *c = buf + 2048;
    ev.PrepareWrite(polar_race::kPageSize, write_content, polar_race::kPageSize);
    ev.Submit();
    ev.WaitOver();
    ev.Clear();
  };
  write_request();

  close(fd_);
  free(buf);
  return 0;
}
