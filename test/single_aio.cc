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

#define LOG(x) std::cout
#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

constexpr int kMaxAIOEvents = 64;
constexpr int kMaxReadPosition = 64;
const char *file_name = "DATA_0";
constexpr int kMaxFileSize = 1000; // 1000 blocks with 4KB a block.
constexpr int kPageSize = 4096;

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

    // init the buf content
    char k = 0;
    for (int j = 0; j < kPageSize; j++) {
      pages[i][j] = static_cast<char>(k+'a');
      k = (k + 1) % 26;
    }
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
 * aio read example.
 * submit 1000 AIO request to single file to generate 4MB file.
 * - allocate 1000 4KB pages.
 * - prepare 1000 io request.
 * - submit the 1000 io request.
 * - wait the write over.
 */
int aio_read_example() {
  int fd = -1;

  fd = open(file_name, O_RDONLY | O_DIRECT, 0644);
  if (fd < 0) {
    DEBUG << "Error opening file" << std::endl;
  }

  // alloc the memory
  char *buf = nullptr;
  if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
    perror("posix_memalign failed!\n");
    return -1;
  }
  memset(buf, 0, sizeof(buf));

  // prepare the io context.
  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  LOG(INFO) << "Setting up the io context" << std::endl;
  if (io_setup(1, &ctx) < 0) {
    DEBUG << "Error in io_setup" << std::endl;
  }

  struct iocb **ops = (struct iocb**) malloc(sizeof(struct iocb*) * 1);
  // begin to prepare every write request.
  for (int i = 0; i < 1; i++) {
    ops[i] = (struct iocb*) malloc(sizeof(struct iocb));
    io_prep_pread(ops[i], fd, buf, kPageSize, 245760);
    std::cout << "align prep_pread = " << ops[i]->u.c.nbytes << std::endl;
    // TODO. may set some ops[i].data = some callback calss obj.
  }

  int ret = 0;
  if ((ret = io_submit(ctx, 1, ops)) != 1) {
    io_destroy(ctx);
    DEBUG << "io_submit meet error, ret = " << ret << std::endl;;
    printf("io_submit error\n");
    return -1;
  }

  // after submit, need to wait all read over.
  int write_over_cnt = 0;
  struct io_event *events = (struct io_event*)
        malloc(sizeof(struct io_event) * kMaxFileSize);

  struct timespec timeout;
  timeout.tv_sec = 0;
  timeout.tv_nsec = 1;

  while (write_over_cnt != 1) {
    constexpr int min_number = 1;
    constexpr int max_number = 1;
    int num_events = io_getevents(ctx, min_number, max_number, events, &timeout);
    // need to call for (i = 0; i < num_events; i++) events[i].call_back();
    write_over_cnt += num_events;
  }
  free(events);

  // clean resource
  for (int i = 0; i < 1; i++) {
    free(ops[i]);
  }
  free(ops);
  ops = nullptr;
  io_destroy(ctx);
  close(fd);
  DEBUG << "buf[0] = " << buf[0] << std::endl;
  free(buf);
}

int main(void) {
  aio_write_example();
  aio_read_example();
  return 0;
}
