
#include "engine_race/engine_race.h"
#include "engine_race/util.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <scsi/scsi_ioctl.h>
#include <scsi/sg.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#ifndef major
#include <sys/types.h>
#endif
#include <assert.h>
#include <byteswap.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <linux/major.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/poll.h>

#include <bits/stdc++.h>

#ifndef RAW_MAJOR
#define RAW_MAJOR 255 /*unlikely value */
#endif

// /sys/block/sdb/queue/max_hw_sectors_kb = 512 这个会限制每次能传输的数据的大小

using namespace std;
using namespace polar_race;

enum {
  kReadOp = 1,
  kWriteOp = 2,
  kBlockSize = 512,
};

static void Sleep(int ms) {
  if (ms <= 0) {
    return;
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

static void exit_with_help() {
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
         kBlockSize, kBlockSize, kBlockSize);
  exit(-1);
}

int GetLockFileFlag(void) { return O_RDWR | O_CREAT; }

uint32_t GetLockFileMode(void) { return 0644; }

uint32_t GetDeviceFileMode(void) { return 0644; }

int GetDeviceFileFlag(void) { return O_RDWR | O_NOATIME | O_DIRECT | O_SYNC; }

RetCode Open(const char *file_name, int flag, uint32_t mode, int *fd) {
  assert(file_name);
  assert(fd);
  *fd = ::open(file_name, flag, mode);
  if (*fd == -1) {
    return kInvalidArgument;
  }
  return kSucc;
}

RetCode Close(int fd) {
  auto ret = ::close(fd);
  if (ret == -1) {
    return kInvalidArgument;
  }
  return kSucc;
}

RetCode Fcntl(int fd, int cmd, void *lock) {
  struct flock *f = (struct flock *)lock;
  auto ret = fcntl(fd, F_SETLK, f);
  return ret == 0 ? kSucc : kIOError;
}

RetCode IOCtl(int fd, uint64_t request, uint64_t *size, int *result) {
  auto ret = ioctl(fd, BLKGETSIZE64, size);
  if (result) {
    *result = ret;
  }
  return ret != -1 ? kSucc : kIOError;
}

RetCode Stat(const char *file_name, void *stat_buf) {
  auto ret = ::stat(file_name, (struct stat *)stat_buf);
  return ret == 0 ? kSucc : kIOError;
}

RetCode GetFileLength(const char *file_name, int64_t *file_size_result) {
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
static void output_result(int op_type, uint64_t iops, uint64_t total_time,
                          uint64_t uint_size) {
  printf("min_time = %.3f (ms)\n", (double)min_io_nanosecond / 1000.0 / 1000.0);
  printf("max_time = %.3f (ms)\n",
         (double)max_io_time_nanosecond / 1000.0 / 1000.0);
  printf("total_iops = %lu\n", iops);
  printf("total_time = %.3f (ms)\n", double(total_time) / 1000.0 / 1000.0);

  static double per[] = {1,  5,  10, 20, 30,   40,   50,    60,   70,
                         80, 90, 95, 99, 99.5, 99.9, 99.95, 99.99};

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
            << (double)((double)iops * (double)uint_size * 1000 * 1000 * 1000) /
                   (double)total_time / 1024 / 1024
            << " MB/s" << std::endl;

  double cnt = 0;
  double total_item = iops;

  int per_idx = 0;

  // compute ops percentile
  for (uint64_t i = min_io_nanosecond / 1000;
       i <= max_io_time_nanosecond / 1000; i++) {
    // want percentile
    if (cnt / total_item * 100.0 <= per[per_idx]) {
      per_ret_ms[per_idx] = (double)i / 1000.0;

      if (per_idx + 1 < sizeof(per_ret_ms) / sizeof(*per_ret_ms)) {
        per_ret_ms[per_idx + 1] = (double)i / 1000.0;
      }

    } else {
      per_idx++;
    }
    cnt += time_stat_microsecond[i];
  }

  for (int i = 0; i < sizeof(per) / sizeof(*per); i++) {
    printf("percentile %.2lf %%= %.3lf (ms)\n", per[i], per_ret_ms[i]);
  }
}

namespace SCSI {

enum {
  FT_OTHER = 0,
  FT_SG = 1,
  FT_RAW = 2,

  DEF_TIMEOUT = 60000,

  S_RW_LEN = 10,
  SGP_READ10 = 0x28,
  SGP_WRITE10 = 0x2a,

  SENSE_BUFF_LEN = 32,
};

// SCSI的命令 
uint8_t cmd[S_RW_LEN];

uint8_t sb[SENSE_BUFF_LEN];

static inline uint32_t sg_get_unaligned_be32(const void *p) {
  return ((const uint8_t *)p)[0] << 24 | ((const uint8_t *)p)[1] << 16 |
         ((const uint8_t *)p)[2] << 8 | ((const uint8_t *)p)[3];
}

static inline void sg_put_unaligned_be16(uint16_t val, void *p)
{
  ((uint8_t *)p)[0] = (uint8_t)(val >> 8);
  ((uint8_t *)p)[1] = (uint8_t)val;
}

static inline void sg_put_unaligned_be32(uint32_t val, void *p)
{
  sg_put_unaligned_be16(val >> 16, p);
  sg_put_unaligned_be16(val, (uint8_t *)p + 2);
}

static int sg_prepare(int fd, int sz) {
  int res;
  int t;
  struct sg_scsi_id info;

  res = ioctl(fd, SG_GET_VERSION_NUM, &t);
  if ((res < 0) || (t < 30000)) {
    fprintf(stderr, "sgq_dd: sg driver prior to 3.x.y\n");
    return -1;
  }

  // 设置驱动预留的内存的大小
  res = ioctl(fd, SG_SET_RESERVED_SIZE, &sz);
  if (res < 0) {
    perror("sgq_dd: SG_SET_RESERVED_SIZE error");
  }

  res = ioctl(fd, SG_GET_SCSI_ID, &info);
  if (res < 0) {
    perror("sgq_dd: SG_SET_SCSI_ID error");
    return -1;
  }

  return info.scsi_type;
}

static int dd_filetype(const char *filename) {
  struct stat st;

  if (stat(filename, &st) < 0) {
    return FT_OTHER;
  }

  if (S_ISCHR(st.st_mode)) {
    if (RAW_MAJOR == major(st.st_rdev))
      return FT_RAW;
    else if (SCSI_GENERIC_MAJOR == major(st.st_rdev))
      return FT_SG;
  }
  return FT_OTHER;
}

// 这里只是发指令去拿一个设备的大小
static int read_capacity(int sg_fd, int *num_sect, int *sect_sz) {
  int res = -1;
  uint8_t rc_cdb[10] = {0x25, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  uint8_t rcBuff[64];
  uint8_t sense_b[64];
  sg_io_hdr_t io_hdr;

  memset(&io_hdr, 0, sizeof(sg_io_hdr_t));

  io_hdr.interface_id = 'S';
  io_hdr.cmd_len = sizeof(rc_cdb);
  io_hdr.mx_sb_len = sizeof(sense_b);
  io_hdr.dxfer_direction = SG_DXFER_FROM_DEV;
  io_hdr.dxfer_len = sizeof(rcBuff);
  io_hdr.dxferp = rcBuff;
  io_hdr.cmdp = rc_cdb;
  io_hdr.sbp = sense_b;
  io_hdr.timeout = DEF_TIMEOUT;

  if (ioctl(sg_fd, SG_IO, &io_hdr) < 0) {
    perror("read_capacity (SG_IO) error");
    return -1;
  }

  // res = sg_err_category3(&io_hdr);
  // assert(res == 0);

  *num_sect = 1 + sg_get_unaligned_be32(rcBuff + 0);
  *sect_sz = sg_get_unaligned_be32(rcBuff + 4);

  std::cout << *num_sect << " : " << *sect_sz << std::endl;

  return 0;
}

static int
sg_build_scsi_cdb(uint8_t *cdbp,
                  int cdb_sz,
                  unsigned int blocks,
                  int64_t start_block,
                  bool is_verify,
                  bool write_true,
                  bool fua,
                  bool dpo)
{
  // 读的编码
  int rd_opcode[] = {0x8, 0x28/*只用这个*/, 0xa8, 0x88};
  // 写的编码
  int wr_opcode[] = {0xa, 0x2a/*只用这个*/, 0xaa, 0x8a};

  memset(cdbp, 0, cdb_sz);
  cdbp[0] = write_true ? SGP_WRITE10 : SGP_READ10;
  cdbp[1] = 0x2;

  sg_put_unaligned_be32(start_block, cdbp + 2);
  sg_put_unaligned_be16(blocks, cdbp + 7);

  return 0;
}

/**
 *
 * 函数功能：
 *
 *  调用SCSI命令写一个设备!
 *  Does a SCSI WRITE or VERIFY
 *  (if do_verify set) on OFILE.
 *
 * 输入参数：
 *
 *    infd:       文件句柄
 *    wrkPos:     内存位置
 *    blocks:     要读/写的blocks的数目
 *    to_block:   从哪里开始读/写
 *    bs:         每个block的大小
 *    scsi_cdbsz: cmd缓冲区的大小
 *    fua:        fua要用吗？
 *    dpo:        device cache要不要被调度出去!
 *    &diop:      diop值有可能被改变
 *
 * 返回值：
 *
 * 0:             successful
 * other:         failed
 *
 **/
static int
sg_write(int sg_fd,
         uint8_t *buff,
         int blocks,
         int64_t to_block,
         int bs,
         int cdbsz,
         bool is_write,
         bool fua,
         bool dpo,
         bool *diop)
{
  int res = 0;
  sg_io_hdr_t hp;
  memset(&hp, 0, sizeof(hp));

  sg_build_scsi_cdb(cmd,
                    10,
                    blocks,
                    to_block,
                    false,
                    is_write,
                    false,
                    false);

  hp.interface_id = 'S';
  hp.cmd_len = sizeof(cmd);
  hp.cmdp = cmd;
  hp.dxfer_direction = is_write ? SG_DXFER_TO_DEV : SG_DXFER_FROM_DEV;
  hp.dxfer_len = bs * blocks;
  hp.dxferp = buff;

  // This is the maximum size that can be written back to the 'sbp'
  // pointer when a sense_buffer is output which is usually in
  // an error situation. The actual number written out is given by
  // 'sb_len_wr'. In all cases 'sb_len_wr' <= 'mx_sb_len' .
  // The type of mx_sb_len is unsigned char.
  hp.mx_sb_len = sizeof(sb);
  hp.sbp = sb;
  hp.timeout = DEF_TIMEOUT;
  hp.usr_ptr = NULL;
  hp.pack_id = to_block;

  if (diop && *diop) {
    hp.flags |= SG_FLAG_DIRECT_IO;
  }

  while (((res = write(sg_fd, &hp, sizeof(sg_io_hdr_t))) < 0) &&
         (EINTR == errno))
    ;
  
  printf("res = %d\n", res);
  assert(res >= 0);

  return 0;
}

static int sg_write_over(int sg_fd,
                         bool is_write,
                         int to_blocks) {
  int res;
  sg_io_hdr_t io_hdr;
  sg_io_hdr_t *hp;

  memset(&io_hdr, 0, sizeof(sg_io_hdr_t));
  io_hdr.interface_id = 'S';
  io_hdr.dxfer_direction = is_write ? SG_DXFER_TO_DEV : SG_DXFER_FROM_DEV;
  io_hdr.pack_id = to_blocks;

  while (((res = read(sg_fd, &io_hdr, sizeof(sg_io_hdr_t))) < 0) &&
         (EINTR == errno))
    ;

  assert(res >= 0);

  return 0;
}

} // end namespace SCSI

int main(int argc, char **argv) {
  const char *disk_path = nullptr;
  int op_type = -1;
  int op_size = -1;
  int overlap_size = 0;
  uint64_t run_time = 120;

  printf("CMD: ");
  for (int i = 0; i < argc; i++) {
    printf("%s ", argv[i]);
  }
  printf("\n");

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

  bool is_read = op_type == kReadOp;

  // setup buffer
  uint8_t *buf =
      (uint8_t *)polar_race::GetAlignedBuffer(op_size + overlap_size);
  for (int i = 0; i < op_size + overlap_size; i++) {
    buf[i] = i % 26 + 'a';
  }

  auto start_time = std::chrono::system_clock::now();
  auto get_diff_ns = [](const decltype(start_time) &start,
                        const decltype(start_time) &end) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    // 1,000 nanoseconds – one microsecond
    // 1,000 microseconds - one ms
  };

  uint64_t time_cost_nanosecond = 0;
  uint64_t io_cnt = 0;
  const uint64_t begin_pos = overlap_size;
  uint64_t write_pos = begin_pos;

  constexpr uint64_t total_write_test = 10000;

  uint64_t time_cost[total_write_test];

  // 打开文件
  // 这里打开文件
  int fd = open(disk_path, O_RDWR);
  if (fd == -1) {
    fprintf(stderr, "open file %s failed\n", disk_path);
    return -1;
  }

  int disk_sectors = 0;
  int sector_size_in_bytes = 0;

  // SCSI pre check
  {
    auto file_type = SCSI::dd_filetype(disk_path);
    assert(file_type == SCSI::FT_SG);

    auto sg_type = SCSI::sg_prepare(fd, op_size);
    assert(sg_type >= 0);

    // 拿到磁盘的大小!
    // sector_size_in_bytes 是最小可以操作的sector size.
    assert(!SCSI::read_capacity(fd, &disk_sectors, &sector_size_in_bytes));
    uint64_t total_size_in_bytes = (uint64_t)disk_sectors *
                                   (uint64_t)sector_size_in_bytes;
    disk_size = total_size_in_bytes & (~(kBlockSize - 1));

    std::cout << "disk_size = " << disk_size / 1024 / 1024 << " MB" << std::endl;
  }

  struct pollfd fds[1];
  {
    // 准备poll需要的数据
    fds[0].fd = fd;
    fds[0].events = POLLIN;
  }

  for (int i = 0; i < total_write_test; i++) {
    auto start = std::chrono::system_clock::now();

    bool direct_io = true;

    // DO IO HERE
    {

      if (i == 0 || fds[0].revents & POLLIN) {
        bool direct_io = false;
        auto ret = SCSI::sg_write(fd,
                                  buf,
                                  (op_size + overlap_size) / kBlockSize /*block per write*/,
                                  (write_pos - overlap_size) / kBlockSize /*write_to_block_pos*/,
                                  kBlockSize/*block_size*/,
                                  10/*SCSI cmd length*/,
                                  true/*is_write*/,
                                  false/*FUA*/,
                                  false/*dpo*/,
                                  &direct_io);
        printf("+");
        assert(ret == 0);

        fds[0].fd = fd;
        fds[0].events = POLLIN;
      }

      int res = 0;
      while (((res = poll(fds, 1, 0)) < 0) &&
         (EINTR == errno))
      ;

      assert(res >= 0);

      SCSI::sg_write_over(fd, true, (write_pos - overlap_size) / kBlockSize);
    }

    write_pos += op_size;
    if (write_pos >= disk_size) {
      write_pos = begin_pos;
    }

    auto end = std::chrono::system_clock::now();

    uint64_t current_io_nanosecond = get_diff_ns(start, end).count();
    max_io_time_nanosecond = max(max_io_time_nanosecond, current_io_nanosecond);
    min_io_nanosecond = min(min_io_nanosecond, current_io_nanosecond);

    time_cost[i] = current_io_nanosecond;
    time_stat_microsecond[time_cost[i] / 1000]++;

    // Sleep(3);
  }
  close(fd);

  time_cost_nanosecond = 0;
  for (int i = 0; i < total_write_test; i++) {
    time_cost_nanosecond += time_cost[i];
  }

  output_result(op_type, total_write_test, time_cost_nanosecond,
                overlap_size + op_size);

  return 0;
}
