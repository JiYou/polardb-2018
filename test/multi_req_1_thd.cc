/*
 *
 */
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
const char *file_name = "/tmp/write.dat";
constexpr int kMaxRequestCount = 64;
constexpr int kPageSize = 4096;

/*
 * submit 1000 AIO request to single file to generate 4MB file.
 * - prepare  64 io request.
 * - submit the 64 io request.
 * - wait the write over.
 */
int aio_read_example() {
  int fd = -1;

  fd = open(file_name, O_RDONLY | O_DIRECT, 0644);
  if (fd < 0) {
    DEBUG << "Error opening file" << std::endl;
  }

  // alloc the memory
  char *pages[kMaxRequestCount];
  for (int i = 0; i < kMaxRequestCount; i++) {
    char *buf = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
      perror("posix_memalign failed!\n");
      return -1;
    }
    memset(buf, 0, sizeof(buf));
    pages[i] = buf;
  }

  // prepare the io context.
  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  LOG(INFO) << "Setting up the io context" << std::endl;
  if (io_setup(kMaxRequestCount, &ctx) < 0) {
    DEBUG << "Error in io_setup" << std::endl;
  }

  struct iocb **ops = (struct iocb**) malloc(sizeof(struct iocb*) * kMaxRequestCount);
  // begin to prepare every write request.
  for (int i = 0; i < kMaxRequestCount; i++) {
    ops[i] = (struct iocb*) malloc(sizeof(struct iocb));
  }

  // after submit, need to wait all read over.
  int write_over_cnt = 0;
  struct io_event *events = (struct io_event*)
        malloc(sizeof(struct io_event) * kMaxRequestCount);

  struct timespec timeout;
  timeout.tv_sec = 0;
  timeout.tv_nsec = 0;

  for (int run = 0; run < 10000; run++) {
    // begin to prepare every write request.
    for (int i = 0; i < kMaxRequestCount; i++) {
      auto idx = (static_cast<uint32_t>(random()) % 2048);
      io_prep_pread(ops[i], fd, pages[i], kPageSize, idx * kPageSize);
      // TODO. may set some ops[i].data = some callback calss obj.
    }

    int ret = 0;
    if ((ret = io_submit(ctx, kMaxRequestCount, ops)) != kMaxRequestCount) {
      io_destroy(ctx);
      DEBUG << "io_submit meet error, ret = " << ret << std::endl;;
      printf("io_submit error\n");
      return -1;
    }

    while (write_over_cnt != kMaxRequestCount) {
      constexpr int min_number = 1;
      constexpr int max_number = kMaxRequestCount;
      int num_events = io_getevents(ctx, min_number, max_number, events, &timeout);
      // need to call for (i = 0; i < num_events; i++) events[i].call_back();
      write_over_cnt += num_events;
    }
  }
  free(events);

  // clean resource
  for (int i = 0; i < kMaxRequestCount; i++) {
    free(ops[i]);
  }
  free(ops);
  ops = nullptr;
  io_destroy(ctx);
  close(fd);
  for (int i = 0; i < kMaxRequestCount; i++) {
    char *buf = pages[i];
    free(buf);
    pages[i] = nullptr;
  }
}

int main(void) {
  aio_read_example();
  return 0;
}
