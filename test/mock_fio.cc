
/*
 * Try to write libaio like fio.
 * FIO: random read:  **Result** = 87268.80iops 340.89MB/s 64threads. io-depth=1
 * - 64 threads
 * - io-depth=1
 * - submit every 1 read IO every time.
 */
#include "engine_race/libaio.h"


#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

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
#include <vector>

#define LOG(x) std::cout
#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

const char *file_name = "/tmp/write.dat";
constexpr int kMaxThread = 64; // 1000 blocks with 4KB a block.
constexpr int kPageSize = 4096;

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
}

int aio_read_example() {
  int fd = -1;
  // use the same fd.
  std::cout << "begin to open file: = " << file_name << std::endl;
  fd = open(file_name, O_RDONLY | O_DIRECT, 0644);
  if (fd < 0) {
    DEBUG << "Error opening file" << std::endl;
  }

  // alloc the memory
  // would generate 1000 theads and read to the related page.
  char *pages[kMaxThread];
  for (int i = 0; i < kMaxThread; i++) {
    char *buf = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
      perror("posix_memalign failed!\n");
      return -1;
    }
    memset(buf, 0, sizeof(buf));
    pages[i] = buf;
  }


  constexpr int kSingleRequest = 1;
  // prepare the io context.
  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  if (io_setup(1, &ctx) < 0) {
    DEBUG << "Error in io_setup" << std::endl;
  }

  auto f = [&fd, &pages, &ctx](int i) {
    struct iocb iocb;
    struct iocb* iocbs = &iocb;
    struct io_event events;

    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;


    // read 10000 times.
    for (int x = 0; x < 100000; x++) {
      auto idx = (static_cast<uint32_t>(random()) % 2048);

      io_prep_pread(&iocb, fd, pages[i], kPageSize, idx * kPageSize);
      int ret = 0;
      if ((ret = io_submit(ctx, kSingleRequest, &iocbs)) != kSingleRequest) {
        io_destroy(ctx);
        DEBUG << "io_submit meet error, ret = " << ret << std::endl;;
        printf("io_submit error\n");
        return;
      }

      // after submit, need to wait all read over.
      int write_over_cnt = 0;
      while (write_over_cnt != kSingleRequest) {
        constexpr int min_number = 1;
        constexpr int max_number = kSingleRequest;
        int num_events = io_getevents(ctx, min_number, max_number, &events, &timeout);
        // need to call for (i = 0; i < num_events; i++) events[i].call_back();
        write_over_cnt += num_events;
      }
    }
  };

  std::vector<std::thread> vt;
  for (int i = 0; i < 64; i++) {
    vt.emplace_back(std::thread(f,i));
  }

  for (int i = 0; i < 64; i++) {
    vt[i].join();
  }


  io_destroy(ctx);
  close(fd);
  for (int i = 0; i < kMaxThread; i++) {
    char *buf = pages[i];
    free(buf);
    pages[i] = nullptr;
  }
}

int main(void) {
  std::cout << "call aio_read_example()" << std::endl;
  aio_read_example();
  return 0;
}
