#include "engine_race/libaio.h"
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
#include <thread>
#include <atomic>
#include <chrono>

#define LOG(x) std::cout
#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

constexpr int kMaxAIOEvents = 64;
constexpr int kMaxReadPosition = 64;
const char *file_name = "DATA_0";
constexpr int kMaxFileSize = 1000; // 1000 blocks with 4KB a block.
constexpr int kPageSize = 4096;

static inline void io_set_callback(struct iocb *iocb, io_callback_t cb)
{
  iocb->data = (void *)cb;
}

static inline void io_prep_pread(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IO_CMD_PREAD;
  iocb->aio_reqprio = 0;
  iocb->u.c.buf = buf;
  iocb->u.c.nbytes = count;
  iocb->u.c.offset = offset;
}

static inline void io_prep_pwrite(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IO_CMD_PWRITE;
  iocb->aio_reqprio = 0;
  iocb->u.c.buf = buf;
  iocb->u.c.nbytes = count;
  iocb->u.c.offset = offset;
}

/*
 * aio write example.
 * submit 1000 AIO request to single file to generate 4MB file.
 * - allocate 1000 4KB pages.
 * - prepare 1000 io request.
 * - submit the 1000 io request.
 * - wait the write over.
 */
int aio_write_example() {
  int fd = -1;

  fd = open(file_name, O_RDWR | O_DIRECT | O_CREAT, 0644);
  if (fd < 0) {
    DEBUG << "Error opening file" << std::endl;
  }

  LOG(INFO) << "Allocating enough space for the sum" << std::endl;
  if (fallocate(fd, 0, 0, kPageSize * kMaxFileSize) < 0) {
    DEBUG << "Error in fallocate" << std::endl;
  }

  // alloc the memory
  char *pages[kMaxFileSize];
  for (int i = 0; i < kMaxFileSize; i++) {
    char *buf = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
      perror("posix_memalign failed!\n");
      return -1;
    }
    pages[i] = buf;
    memset(buf, (i%26) + 'a', kPageSize);
  }

  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  LOG(INFO) << "Setting up the io context" << std::endl;
  if (io_setup(kMaxFileSize, &ctx) < 0) {
    DEBUG << "Error in io_setup" << std::endl;
  }

  struct iocb **ops = (struct iocb**) malloc(sizeof(struct iocb*) * kMaxFileSize);
  // begin to prepare every write request.
  for (int i = 0; i < kMaxFileSize; i++) {
    ops[i] = (struct iocb*) malloc(sizeof(struct iocb));
    io_prep_pwrite(ops[i], fd, pages[i], kPageSize,  i * kPageSize);
    std::cout << "align prep_pwrite = " << ops[i]->u.c.nbytes << std::endl;
    // TODO. may set some ops[i].data = some callback calss obj.
  }

  int ret = 0;
  if ((ret = io_submit(ctx, kMaxFileSize, ops)) != kMaxFileSize) {
    io_destroy(ctx);
    DEBUG << "io_submit meet error, ret = " << ret << std::endl;;
    printf("io_submit error\n");
    return -1;
  }

  // after submit, need to wait all write over.
  int write_over_cnt = 0;
  struct io_event *events = (struct io_event*)
        malloc(sizeof(struct io_event) * kMaxFileSize);

  struct timespec timeout;
  timeout.tv_sec = 0;
  timeout.tv_nsec = 1;

  while (write_over_cnt != kMaxFileSize) {
    constexpr int min_number = 1;
    constexpr int max_number = kMaxFileSize;
    int num_events = io_getevents(ctx, min_number, max_number, events, &timeout);
    // need to call for (i = 0; i < num_events; i++) events[i].call_back();
    write_over_cnt += num_events;
  }
  free(events);

  // clean resource
  for (int i = 0; i < kMaxFileSize; i++) {
    free(ops[i]);
  }
  free(ops);
  ops = nullptr;
  io_destroy(ctx);
  close(fd);
  for (int i = 0; i < kMaxFileSize; i++) {
    char *buf = pages[i];
    free(buf);
    pages[i] = nullptr;
  }
}

/*
 * TO: test is thread safe for multi-thread using the same fd?
 * aio read example.
 * submit 1000 AIO request to single file to generate 4MB file.
 * - allocate 1000 4KB pages.
 * - prepare 1000 io request.
 * - submit the 1000 io request.
 * - wait the write over.
 */

void read_page_thead(int fd, char *buf, int index) {
    constexpr int kSingleRequest = 1;
    // prepare the io context.
    io_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(1, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    struct iocb iocb;
    struct iocb* iocbs = &iocb;
    io_prep_pread(&iocb, fd, buf, kPageSize, index * kPageSize);
    // TODO. may set some ops[i].data = some callback calss obj.

    int ret = 0;
    if ((ret = io_submit(ctx, kSingleRequest, &iocbs)) != kSingleRequest) {
      io_destroy(ctx);
      DEBUG << "io_submit meet error, ret = " << ret << std::endl;;
      printf("io_submit error\n");
      return;
    }

    // after submit, need to wait all read over.
    int write_over_cnt = 0;
    struct io_event events;

    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 1;

    while (write_over_cnt != kSingleRequest) {
      constexpr int min_number = 1;
      constexpr int max_number = kSingleRequest;
      int num_events = io_getevents(ctx, min_number, max_number, &events, &timeout);
      // need to call for (i = 0; i < num_events; i++) events[i].call_back();
      write_over_cnt += num_events;
    }
    io_destroy(ctx);

}
int aio_read_example() {
  int fd = -1;

  std::atomic<int> cnt {0};

  // use the same fd.
  fd = open(file_name, O_RDONLY | O_DIRECT, 0644);
  if (fd < 0) {
    DEBUG << "Error opening file" << std::endl;
  }

  // alloc the memory
  // would generate 1000 theads and read to the related page.
  char *pages[kMaxFileSize];
  for (int i = 0; i < kMaxFileSize; i++) {
    char *buf = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
      perror("posix_memalign failed!\n");
      return -1;
    }
    memset(buf, 0, sizeof(buf));
    pages[i] = buf;
  }

  auto f = [&fd, &pages, &cnt](int idx) {
    read_page_thead(fd, pages[idx], idx);
    // after read, then check the content.
    char x = (idx%26) + 'a';
    for (int i = 0; i < kPageSize; i++) {
      if (pages[idx][i] != x) {
        DEBUG << "ERROR: char not the same!\n";
      }
    }
    cnt++;
  };

  for (int i = 0; i < kMaxFileSize; i++) {
    std::thread thd(f, i);
    thd.detach();
  }

  while (cnt != kMaxFileSize) {
    std::this_thread::sleep_for(std::chrono::nanoseconds(10000));
  }

  close(fd);
  for (int i = 0; i < kMaxFileSize; i++) {
    char *buf = pages[i];
    free(buf);
    pages[i] = nullptr;
  }
}

int main(void) {
  aio_write_example();
  aio_read_example();
  return 0;
}
