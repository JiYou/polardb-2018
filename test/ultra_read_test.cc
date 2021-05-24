
#include "engine_race/engine_race.h"
#include "engine_race/util.h"

#include <assert.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <bits/stdc++.h>

using namespace std;
using namespace polar_race;

enum
{
  kReadOp = 1,
  kWriteOp = 2,
  kBlockSize = 512,
};

static void
exit_with_help()
{
  printf("Usage: ultra_disk_test [option] disk_full_path\n"
         "Options:\n"
         "-r: read test\n"
         "-w: write test\n"
         "-l size: write size(bytes) in every write\n"
         "         the size must be %d aligned\n"
         "-v size: overlapped size(bytes) with previous write/read\n"
         "         the size must be %d bytes aligned\n"
         "-t time: run how many seconds\n"
         "example:\n"
         "    ./ultra_disk_test -r -l 4096 -v %d /dev/sdd\n"
         "    ./ultra_disk_test -w -l 4096 -v 4096 /dev/sdd\n",
         kBlockSize,
         kBlockSize,
         kBlockSize);
  exit(-1);
}

int
GetLockFileFlag(void)
{
  return O_RDWR | O_CREAT;
}

uint32_t
GetLockFileMode(void)
{
  return 0644;
}

uint32_t
GetDeviceFileMode(void)
{
  return 0644;
}

int
GetDeviceFileFlag(void)
{
  return O_RDWR | O_NOATIME | O_DIRECT | O_SYNC;
}

RetCode
Open(const char* file_name, int flag, uint32_t mode, int* fd)
{
  assert(file_name);
  assert(fd);
  *fd = ::open(file_name, flag, mode);
  if (*fd == -1) {
    return kInvalidArgument;
  }
  return kSucc;
}

RetCode
Close(int fd)
{
  auto ret = ::close(fd);
  if (ret == -1) {
    return kInvalidArgument;
  }
  return kSucc;
}

RetCode
Fcntl(int fd, int cmd, void* lock)
{
  struct flock* f = (struct flock*)lock;
  auto ret = fcntl(fd, F_SETLK, f);
  return ret == 0 ? kSucc : kIOError;
}

RetCode
IOCtl(int fd, uint64_t request, uint64_t* size, int* result)
{
  auto ret = ioctl(fd, BLKGETSIZE64, size);
  if (result) {
    *result = ret;
  }
  return ret != -1 ? kSucc : kIOError;
}

RetCode
Stat(const char* file_name, void* stat_buf)
{
  auto ret = ::stat(file_name, (struct stat*)stat_buf);
  return ret == 0 ? kSucc : kIOError;
}

RetCode
GetFileLength(const char* file_name, int64_t* file_size_result)
{
  struct stat stat_buf;

  int rc = Stat(file_name, &stat_buf);
  auto file_size = rc == 0 ? stat_buf.st_size : -1;
  uint64_t size = 0;

  // or maybe this file is a device, then stat() not works well.
  int fd = -1;
  auto ret = Open(file_name, O_RDONLY, GetDeviceFileMode(), &fd);
  if (ret != kSucc || fd == -1) {
    goto open_device_failed;
  }

  if (IOCtl(fd, BLKGETSIZE64, &size, &rc /*ignored*/) != kSucc) {
    goto open_device_failed;
  }

  Close(fd);
  if (file_size_result) {
    *file_size_result = size > file_size ? size : file_size;
  }
  return kSucc;

open_device_failed:
  if (file_size == -1) {
    return kIOError;
  }
  if (file_size_result) {
    *file_size_result = file_size;
  }
  return kSucc;
}

// index stands for microsecond
static uint64_t time_stat_microsecond[1000000];
static uint64_t max_io_time_nanosecond = 0;
static uint64_t min_io_nanosecond = INT64_MAX;

/**
 * Function:
 *  Used to print out the results of disk performance
 *
 * Arguments:
 *  - IOPS: total iops during the time.
 *  - total_time (nanoseconds): total running time
 *  - unit_size (bytes): every read/write_size
 *
 * output: Just like fio
 *    |  1.00th=[ 1020],  5.00th=[ 1237], 10.00th=[ 1401], 20.00th=[ 7373],
 *    | 30.00th=[ 7701], 40.00th=[ 7832], 50.00th=[ 7898], 60.00th=[ 7963],
 *    | 70.00th=[ 8094], 80.00th=[ 8160], 90.00th=[ 8291], 95.00th=[ 8356],
 *    | 99.00th=[ 8455], 99.50th=[ 8586], 99.90th=[ 8848], 99.95th=[ 8979],
 *    | 99.99th=[ 9110]
 */
static void
output_result(int op_type,
              uint64_t iops,
              uint64_t total_time,
              uint64_t uint_size)
{
  printf("min_time = %.3f (ms)\n", (double)min_io_nanosecond / 1000.0 / 1000.0);
  printf("max_io_time_nanosecond = %.3f (ms)\n",
         (double)max_io_time_nanosecond / 1000.0 / 1000.0);
  printf("total_iops = %lu\n", iops);
  printf("total_time = %.3f (ms)\n", double(total_time) / 1000.0 / 1000.0);

  static double per[] = { 1,  5,  10, 20, 30,   40,   50,    60,   70,
                          80, 90, 95, 99, 99.5, 99.9, 99.95, 99.99 };

  double per_ret_ms[sizeof(per) / sizeof(*per)];

  if (op_type == kReadOp) {
    std::cout << "Op = "
              << "Read" << std::endl;
  } else {
    std::cout << "Op = "
              << "Write" << std::endl;
  }

  // iops per second
  std::cout << "IOPS = " << (iops * 1000 * 1000 * 1000) / total_time
            << " iops/s" << std::endl;

  // BW (bytes/second)
  std::cout << "BW = "
            << (double)(iops * uint_size * 1000 * 1000 * 1000) /
                 (double)total_time / 1024 / 1024
            << " MB/s" << std::endl;

  double cnt = 0;
  double total_item = iops;

  int per_idx = 0;

  // compute ops percentile
  for (uint64_t i = min_io_nanosecond / 1000;
       i <= max_io_time_nanosecond / 1000;
       i++) {
    // want percentile
    if (cnt / total_item * 100.0 <= per[per_idx]) {
      per_ret_ms[per_idx] = (double)i / 1000.0;
    } else {
      per_idx++;
    }
    cnt += time_stat_microsecond[i];
  }

  for (int i = 0; i < sizeof(per) / sizeof(*per); i++) {
    printf("percentile %.2lf %%= %.3lf (ms)\n", per[i], per_ret_ms[i]);
  }
}

int
main(int argc, char** argv)
{
  const char* disk_path = nullptr;
  int op_type = -1;
  int op_size = -1;
  int overlap_size = 0;
  uint64_t run_time = 120;

  // parse options
  for (int i = 1; i < argc; i++) {

    // this is disk path
    if (argv[i][0] != '-') {
      disk_path = argv[i];
      continue;
    }

    switch (argv[i][1]) {
      // is read op!
      case 'r':
        op_type = kReadOp;
        break;

      // is write op!
      case 'w':
        op_type = kWriteOp;
        break;

      // write len
      case 'l':
        i++;
        op_size = stoi(argv[i]);
        if (op_size & (kBlockSize - 1) || op_size == 0) {
          fprintf(stderr, "write size must aligned with %d\n", kBlockSize);
          exit_with_help();
        }
        break;

      // overlap size
      case 'v':
        i++;
        overlap_size = stoi(argv[i]);
        if (overlap_size & (kBlockSize - 1)) {
          fprintf(stderr, "overlap_size must aligned with %d\n", kBlockSize);
          exit_with_help();
        }
        break;

      // run time
      case 't':
        i++;
        run_time = stoi(argv[i]);
        if (run_time > 1000) {
          fprintf(stderr, "run too long time = %lu seconds\n", run_time);
          exit_with_help();
        }
        break;
      default:
        exit_with_help();
        break;
    }
  }

  if (-1 == op_size) {
    fprintf(stderr, "-l op_size is mandatory\n");
    exit_with_help();
  }

  if (-1 == op_type) {
    fprintf(stderr, "-r/-w is mandatory\n");
    exit_with_help();
  }

  if (nullptr == disk_path) {
    fprintf(stderr, "disk_path is mandatory\n");
    exit_with_help();
  }

  int64_t disk_size = 0;
  if (kSucc != GetFileLength(disk_path, &disk_size)) {
    fprintf(stderr, "get %s disk length failed\n", disk_path);
  }

  disk_size = disk_size & (~(kBlockSize - 1));
  std::cout << "disk_size = " << disk_size / 1024 / 1024 << " MB" << std::endl;

  int fd = open(disk_path, O_RDWR | O_NOATIME | O_DIRECT | O_SYNC, 0644);
  if (fd == -1) {
    fprintf(stderr, "open file %s failed\n", disk_path);
    return -1;
  }

  bool is_read = op_type == kReadOp;
  struct aio_env_single aio(fd, is_read, false /*no_alloc*/);

  // setup buffer
  char* buf = polar_race::GetAlignedBuffer(op_size + overlap_size);
  memset(buf, 0, op_size + overlap_size);

  auto start_time = std::chrono::system_clock::now();
  auto get_diff_ns = [](const decltype(start_time)& start,
                        const decltype(start_time) end) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    // 1,000 nanoseconds – one microsecond
    // 1,000 microseconds - one ms
  };

  uint64_t time_cost_nanosecond = 0;
  uint64_t io_cnt = 0;
  const uint64_t begin_pos = overlap_size;
  uint64_t write_pos = begin_pos;

  while (true) {
    auto start_submit_time = std::chrono::system_clock::now();

    aio.Prepare(write_pos - overlap_size, buf, op_size + overlap_size);
    aio.Submit();

    auto start_commit_time = std::chrono::system_clock::now();

    write_pos += op_size;
    if (write_pos >= disk_size) {
      write_pos = begin_pos;
    }
    auto ret = aio.WaitOver();

    auto end_commit_time = std::chrono::system_clock::now();

    io_cnt++;

    // nanosecond -> microsecond
    uint64_t current_io_nanosecond =
      get_diff_ns(start_submit_time, end_commit_time).count();
    max_io_time_nanosecond = max(max_io_time_nanosecond, current_io_nanosecond);
    min_io_nanosecond = min(min_io_nanosecond, current_io_nanosecond);
    time_stat_microsecond[current_io_nanosecond / 1000]++;

    time_cost_nanosecond = get_diff_ns(start_time, end_commit_time).count();
    if (time_cost_nanosecond >= run_time * 1000 * 1000 * 1000) {
      break;
    }

  }

  output_result(op_type, io_cnt, time_cost_nanosecond, overlap_size + op_size);

  close(fd);

  return 0;
}
