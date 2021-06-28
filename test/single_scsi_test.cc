
#include "engine_race/engine_race.h"
#include "engine_race/util.h"

#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <scsi/sg.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <scsi/scsi_ioctl.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#ifndef major
#include <sys/types.h>
#endif
#include <linux/major.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <byteswap.h>
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

#ifndef RAW_MAJOR
#define RAW_MAJOR 255 /*unlikely value */
#endif

// /sys/block/sdb/queue/max_hw_sectors_kb = 512 这个会限制每次能传输的数据的大小

using namespace std;
using namespace polar_race;

enum
{
  kReadOp = 1,
  kWriteOp = 2,
  kBlockSize = 512,
};

static void
Sleep(int ms)
{
    if (ms <= 0) {
      return;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

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
  printf("max_time = %.3f (ms)\n",
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
            << (double)((double)iops * (double)uint_size * 1000 * 1000 * 1000) /
                 (double)total_time / 1024 / 1024
            << " MB/s" << std::endl;

  double cnt = 0;
  double total_item = iops;

  int per_idx = 0;

  // compute ops percentile
  for (uint64_t i = min_io_nanosecond/ 1000;
       i <= max_io_time_nanosecond / 1000;
       i++) {
    // want percentile
    if (cnt / total_item * 100.0 <= per[per_idx]) {
      per_ret_ms[per_idx] = (double)i / 1000.0;

      if (per_idx + 1 < sizeof(per_ret_ms) / sizeof(*per_ret_ms)) {
        per_ret_ms[per_idx + 1] = (double) i / 1000.0;
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

/**
 * Utilities can use these exit status values for syntax errors and
 * file (device node) problems (e.g. not found or permissions).
 * command line syntax problem
 **/
#define SG_LIB_SYNTAX_ERROR 1

#define SENSE_BUFF_LEN 64 /* Arbitrary, could be larger */
#define DEF_TIMEOUT 40000 /* 40,000 millisecs == 40 seconds */

#define FT_OTHER 1  /* filetype other than sg and ... */
#define FT_SG 2     /* filetype is sg char device */
#define FT_RAW 4    /* filetype is raw char device */
#define FT_BLOCK 8  /* filetype is block device */
#define FT_ERROR 64 /* couldn't "stat" file */

constexpr uint32_t KiB = 1024;
constexpr uint32_t MiB = KiB * KiB;
constexpr uint32_t k4KiB = KiB << 2;

inline void Sleep(int ms) {
    if (ms <= 0) {
      return;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

static std::pair<int, std::string>
file_type_list[] = {
  {FT_OTHER, "FT_OTHER"},
  {FT_SG, "FT_SG"},
  {FT_RAW, "FT_RAW"},
  {FT_BLOCK, "FT_BLOCK"},
  {FT_ERROR, "FT_ERROR"},
};

// 去拿文件的类型
static int get_file_type(const char *filename) {
  struct stat st;

  if (stat(filename, &st) < 0) {
    return FT_ERROR;
  }

  if (S_ISCHR(st.st_mode)) {
    if (RAW_MAJOR == major(st.st_rdev)) {
      return FT_RAW;
    } else if (SCSI_GENERIC_MAJOR == major(st.st_rdev)) {
      return FT_SG;
    }
  } else if (S_ISBLK(st.st_mode)) {
    return FT_BLOCK;
  }
  return FT_OTHER;
}

// 输出文件的类型
static void print_file_type(int flag) {
  const int N = sizeof(file_type_list) / sizeof(*file_type_list);
  for (int i = 0; i < N; i++) {
    const int mask = file_type_list[i].first;
    if (flag & mask) {
      printf("%s | ", file_type_list[i].second.c_str());
    }
  }
  printf("\n");
}


// 文件操作
static const char *file_name = "/dev/sg2";
int fd = -1;


// 写入之后是否要检查
static bool do_verify = false;


#define VERIFY10 0x2f
#define VERIFY12 0xaf
#define VERIFY16 0x8f


static inline void sg_put_unaligned_be16(uint16_t val, void *p) {
        uint16_t u = bswap_16(val);
        memcpy(p, &u, 2);
}

static inline void sg_put_unaligned_be32(uint32_t val, void *p) {
        uint32_t u = bswap_32(val);
        memcpy(p, &u, 4);
}

// 开始构建SCSI命令!
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
    int sz_ind;
    int rd_opcode[] = {0x8, 0x28, 0xa8, 0x88};
    int ve_opcode[] = {0xff /* no VERIFY(6) */,
                       VERIFY10,
                       VERIFY12,
                       VERIFY16};
    int wr_opcode[] = {0xa, 0x2a, 0xaa, 0x8a};

    memset(cdbp, 0, cdb_sz);

    // 先把检查打开
    assert(is_verify == true);
    // 我们先考虑为10的时候的处理
    assert(cdb_sz == 10);

    cdbp[1] = 0x2;

    is_verify = false;
        if (dpo)
      cdbp[1] |= 0x10;
    if (fua)
      cdbp[1] |= 0x8;

    sz_ind = 1;
    if (is_verify && write_true) {
      cdbp[0] = ve_opcode[sz_ind];
    } else {
      cdbp[0] = (uint8_t)(write_true ? wr_opcode[sz_ind] :
        rd_opcode[sz_ind]);
    }

    sg_put_unaligned_be32(start_block, cdbp + 2);
    sg_put_unaligned_be16(blocks, cdbp + 7);

    if (blocks & (~0xffff)) {
      printf("for 10 byte commands, maximum number of blocks is "
                "%d\n", 0xffff);
      return SG_LIB_SYNTAX_ERROR;
    }

    return 0;
}

/**
 * coe:
 * 0: exit on error (def)
 * 1: continue on sg error (zero fill)
 * 2: also try read_long on unrecovered reads,
 * 3: and set the CORRCT bit on the read long
 **/
static int coe = 0;

/* failed but coe set */
constexpr int G_DD_BYPASS = 999;

uint8_t wrCmd[1024];
uint8_t senseBuff[1024];
int pack_id_count;

/**
 *
 * 函数功能：
 *
 *  调用SCSI命令写一个设备!
 *  Does a SCSI WRITE or VERIFY (if do_verify set) on OFILE.
 *
 * 输入参数：
 *
 *    infd:       文件句柄
 *    wrkPos:     内存位置
 *    blocks:     要读的blocks的数目
 *    skip:       从哪里开始读?
 *    bs:         block的大小
 *    scsi_cdbsz: cmd缓冲区的大小
 *    fua:        fua要用吗？
 *    dpo:        device cache要不要被调度出去!
 *    &diop:      diop值有可能被改变
 *
 * 返回值：
 *
 * 0:                           successful
 * SG_LIB_SYNTAX_ERROR:         unable to build cdb,
 * SG_LIB_CAT_NOT_READY:
 * SG_LIB_CAT_UNIT_ATTENTION:
 * SG_LIB_CAT_MEDIUM_HARD:
 * SG_LIB_CAT_ABORTED_COMMAND:
 * -2:                          recoverable (ENOMEM),
 * -1:                          unrecoverable error + others
 * SG_DD_BYPASS:                failed but coe set.
 *
 **/
static int
sg_write(int sg_fd,
         uint8_t *buff,
         int blocks,
         int64_t to_block,
         int bs,
         int cdbsz,
         bool fua,
         bool dpo,
         bool *diop)
{
    bool info_valid;
    int res;
    uint64_t io_addr = 0;
    struct sg_io_hdr io_hdr;

    assert(0 == sg_build_scsi_cdb(wrCmd,
                          cdbsz,
                          blocks,
                          to_block,
                          true/*verify*/,
                          true/*write_true*/,
                          fua,
                          dpo));
    memset(&io_hdr, 0, sizeof(struct sg_io_hdr));

    io_hdr.interface_id = 'S';
    io_hdr.cmd_len = cdbsz;
    io_hdr.cmdp = wrCmd;
    io_hdr.dxfer_direction = SG_DXFER_TO_DEV;
    io_hdr.dxfer_len = bs * blocks;
    io_hdr.dxferp = buff;
    io_hdr.mx_sb_len = SENSE_BUFF_LEN;
    io_hdr.sbp = senseBuff;
    io_hdr.timeout = DEF_TIMEOUT;
    io_hdr.pack_id = pack_id_count++;

    if (diop && *diop) {
        io_hdr.flags |= SG_FLAG_DIRECT_IO;
    }

    while (((res = ioctl(sg_fd, SG_IO, &io_hdr)) < 0) &&
           ((EINTR == errno) || (EAGAIN == errno) || (EBUSY == errno))) {
        /*Nothing*/;
    }

    assert(res == 0);

//    if (diop && *diop &&
//        ((io_hdr.info & SG_INFO_DIRECT_IO_MASK)
//          != SG_INFO_DIRECT_IO)) {
//      printf("write NOT over!\n");
//      *diop = false;
//    }

    return 0;
}


} // end namespace SCSI

int
main(int argc, char** argv)
{
  const char* disk_path = nullptr;
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
  uint8_t* buf = (uint8_t*)polar_race::GetAlignedBuffer(op_size + overlap_size);
  for (int i = 0; i < op_size + overlap_size; i++) {
    buf[i] = i % 26 + 'a';
  }

  auto start_time = std::chrono::system_clock::now();
  auto get_diff_ns = [](const decltype(start_time)& start,
                        const decltype(start_time)& end) {
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

  for (int i = 0; i < total_write_test; i++) {
    auto start = std::chrono::system_clock::now();

    //aio.Prepare(write_pos - overlap_size, buf, op_size + overlap_size);
    // aio.Submit();
    // auto ret = aio.WaitOver();
    bool direct_io = true;
    auto ret = SCSI::sg_write(fd,
                              buf,
                             (op_size + overlap_size) / kBlockSize    /*block per write*/,
                              (write_pos - overlap_size) / kBlockSize /*write_to_block_pos*/,
                              kBlockSize/*block_size*/,
                              10/*SCSI cmd length*/,
                              false/*FUA*/,
                              false/*dpo*/,
                              &direct_io);


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

    Sleep(3);
  }
  close(fd);

  time_cost_nanosecond = 0;
  for (int i = 0; i < total_write_test; i++) {
    time_cost_nanosecond += time_cost[i];
  }


  output_result(op_type, total_write_test, time_cost_nanosecond, overlap_size + op_size);

  return 0;
}
