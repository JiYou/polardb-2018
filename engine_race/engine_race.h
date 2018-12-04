// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include "include/engine.h"
#include "spin_lock.h"
#include "util.h"
#include "engine_aio.h"
#include "engine_spsc.h"

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>

#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>

#include <bitset>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <deque>
#include <vector>
#include <atomic>
#include <string>
#include <queue>
#include <map>

namespace polar_race {

#pragma pack(push, 1)
// try to just use 256 files.
// the file number is from [0, 256)
// 0 is not included.
// offset is devided by 4K
// 1024*1024*1024/4096
// 262144 need 18bits.
// so, here need 24 bits.
// 8 + 3 = 11 bytes.
// 3bytes is hard to deal
// so, here just use 12 bytes.
class disk_index {
 private:
  uint64_t key_ = 0;
  uint32_t offset_ = 0;
 public:
  disk_index(uint64_t k, uint32_t o) : key_(k), offset_(o) {
  }

  disk_index() { }

  uint64_t get_key() const {
    return key_;
  }

  uint32_t get_offset() const {
    return offset_;
  }

  const uint64_t *get_key_ptr() const {
    return &key_;
  }

  void set_key(uint64_t key) {
    key_ = key;
  }

  void set_offset(uint64_t offset) {
    offset_ = offset;
  }

  // NOTE: return value [0, 256);
  uint16_t get_file_number() const {
    return (key_ >> 56);
  }

  bool operator < (const uint64_t k) const {
    return key_ < k;
  }

  bool operator < (const disk_index &x) const {
    return key_ < x.key_;
  }

  bool operator == (const disk_index &k) const {
    return key_ == k.key_ &&
      offset_ == k.offset_;
  }

  bool operator != (const disk_index &k) const {
    return key_ != k.key_ ||
      offset_ != k.offset_;
  }

  bool is_valid() const {
    return !(key_ == 0 && offset_ == 0);
  }
};
#pragma pack(pop)

class HashTreeTable {
 public:
  RetCode GetNoLock(uint64_t key, uint32_t *file_no, uint32_t *file_offset); // no_lock
  RetCode SetNoLock(uint64_t key, uint32_t file_offset, spinlock *ar); // no_lock

  // NOTE: no lock here. Do not call it anywhere.
  // after load all the entries from disk,
  // for speed up the lookup, need to sort it.
  // after some set operations->append to the
  // hash shard vector.
  void Sort();

  // print Hash Mean StdDev size of hash shard.
  void PrintMeanStdDev();

  // merge sucket sort and write all kv into single file.
  void Save(const char *file_name);

 public:
  void Init() {
    hash_.resize(kMaxBucketSize);
  }

  HashTreeTable() { }
  ~HashTreeTable() { }

 public:
  HashTreeTable(const HashTreeTable &) = delete;
  HashTreeTable(const HashTreeTable &&) = delete;
  HashTreeTable &operator=(const HashTreeTable&) = delete;
  HashTreeTable &operator=(const HashTreeTable&&) = delete;
 private:
  std::vector<std::vector<struct disk_index>> hash_;
 private:
  uint32_t compute_pos(uint64_t key);
  RetCode find(std::vector<struct disk_index> &vs, uint64_t key, struct disk_index**ptr);
};

// aio just for read operations.
struct aio_env_single {
  aio_env_single(int fd_=-1, bool read=true, bool alloc=true) {
    fd = fd_;
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(kSingleRequest, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;

    memset(&iocb, 0, sizeof(iocb));
    iocbs = &iocb;
    if (read) {
      iocb.aio_lio_opcode = IO_CMD_PREAD;
    } else {
      iocb.aio_lio_opcode = IO_CMD_PWRITE;
    }
    iocb.aio_reqprio = 0;
    iocb.aio_fildes = fd;
    iocb.u.c.buf = buf;
    iocb.u.c.nbytes = kPageSize;

    if (alloc) {
      buf = GetAlignedBuffer(kPageSize);
      if (!buf) {
        DEBUG << "alloc memory failed\n";
      }
    }
  }

  void SetFD(int fd_) {
    fd = fd_;
    iocb.aio_fildes = fd;
  }

  void Prepare(uint64_t offset) {
    iocb.u.c.offset = offset;
    iocb.u.c.buf = buf; // must set here !
  }

  void Prepare(uint64_t offset, char *out, uint64_t size) {
    iocb.u.c.offset = offset;
    iocb.u.c.buf = out;
    iocb.u.c.nbytes = size;
  }

  RetCode Submit() {
    if ((io_submit(ctx, kSingleRequest, &iocbs)) != kSingleRequest) {
      DEBUG << "io_submit meet error, " << std::endl;;
      return kIOError;
    }
    return kSucc;
  }

  void WaitOver() {
    // after submit, need to wait all read over.
    while (io_getevents(ctx, kSingleRequest, kSingleRequest,
                        &events, &(timeout)) != kSingleRequest) {
      /**/
    }
  }

  ~aio_env_single() {
    io_destroy(ctx);

    if (buf) {
      free(buf);
    }
  }

  int fd = -1;
  char *buf = nullptr;
  io_context_t ctx;
  struct iocb iocb;
  struct iocb* iocbs;
  struct io_event events;
  struct timespec timeout;
};

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir)
    :  dir_(dir) {
  }

  virtual ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

 private:
  const char *AllIndexFile() { return all_index_file_.c_str(); }
  std::string all_index_file_;
  std::string file_name_;

 private:
  void RangeEntry();
  RetCode SlowRead(const PolarString &l, const PolarString &u, Visitor &visitor);

 private:
  uint64_t pin_cpu() {
    auto thread_pid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    uint64_t m_thread_id = thread_id_++;
    CPU_SET(m_thread_id % max_cpu_cnt_, &cpuset);
    int rc = pthread_setaffinity_np(thread_pid, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    return m_thread_id;
  }
 private:
  // for write/read/range
  bool has_open_data_fd_ = false;
  int data_fd_[kThreadShardNumber] = {-1};
  // in the write process the initial value is set to 0.
  int data_fd_len_[kThreadShardNumber] = {0};
  spinlock *write_lock_ = nullptr;
  void close_data_fd() {
    if (!has_open_data_fd_) {
      return;
    }
    for (int i = 0; i < (int)kThreadShardNumber; i++) {
      if (data_fd_[i] > 0) {
        close(data_fd_[i]);
        data_fd_[i] = -1;
      }
    }
  }

  void open_data_fd_read() {
    if (has_open_data_fd_) {
      close_data_fd();
    }
    char path[kPathLength];
    const std::string data_dir = file_name_ + kDataDirName;
    for (int i = 0; i < (int)kThreadShardNumber; i++) {
      sprintf(path, "%s/%d", data_dir.c_str(), i);
      int fd = open(path, O_RDONLY | O_NOATIME | O_DIRECT, 0644);
      if (fd < 0) {
        DEBUG << "open " << path << " meet error\n";
        // DO not exit, may this file not exits.
      }
      data_fd_[i] = fd;
    }
    has_open_data_fd_ = true;
  }

  void open_data_fd_range() {
    if (has_open_data_fd_) {
      close_data_fd();
    }
    char path[kPathLength];
    const std::string data_dir = file_name_ + kDataDirName;
    for (int i = 0; i < (int)kThreadShardNumber; i++) {
      sprintf(path, "%s/%d", data_dir.c_str(), i);
      data_fd_len_[i] = get_file_length(path);
      int fd = open(path, O_RDONLY | O_NOATIME | O_DIRECT, 0644);
      if (fd < 0) {
        DEBUG << "open " << path << " meet error\n";
        // DO not exit, may this file not exits.
      }
      data_fd_[i] = fd;
    }
    has_open_data_fd_ = true;
  }

  void open_data_fd_write() {
    char path[kPathLength];
    const std::string data_dir = file_name_ + kDataDirName;
    for (int i = 0; i < (int)kThreadShardNumber; i++) {
      sprintf(path, "%s/%d", data_dir.c_str(), i);
      int fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK | O_NOATIME, 0644);
      if (fd < 0) {
        DEBUG << "open " << path << " meet error\n";
        // DO not exit, may this file not exits.
      }
      data_fd_[i] = fd;
    }
    posix_fallocate(data_fd_[0], 0, kPageSize);
    lseek(data_fd_[0], 0, SEEK_END);
    // change the start position.
    data_fd_len_[0] = kPageSize;
    has_open_data_fd_ = true;
  }

 // Read Stage
 private:
  void init_read();
  HashTreeTable hash_; // for the read index.


 // Range Stage
 // TODO here is just for 64 threads: call these threads visit-thread.
 // there are other 2 thread.
 // - read_index thread.
 // - read_data thread
 private:

  void ReadIndexEntry();
  void ReadDataEntry();

  int read_file(int fd, char *buffer, size_t size) {
    int bytes = 0;
    while ((bytes = read(fd, buffer, size)) < 0) {
      if (errno != EAGAIN) {
        return 0;
      }
    }
    return bytes;
  }
  int get_file_length(const char *path) {
    struct stat stat_buf;
    int rc = stat(path, &stat_buf);
    if (rc) {
      return -1;
    }
    return stat_buf.st_size;
  }

  uint64_t buf_size_ = 0;
  char *index_buf_ = nullptr;
  char *data_buf_ = nullptr;

  chan index_chan_[kMaxThreadNumber];
  // 64 visit thread -> ReadIndex thread
  // ask it to read index.
  void ask_to_read_index(int thread_id) {
    index_chan_[thread_id].write();
  }
  // called by ReadIndex thread
  // if 64 threads as me to read, then read.
  void is_ok_to_read_index() {
    for (uint64_t i = 0; i < kMaxThreadNumber; i++) {
      auto &q = index_chan_[i];
      q.read();
    }
  }

  chan data_chan_[kMaxThreadNumber];
  void ask_to_read_data(int thread_id) {
    data_chan_[thread_id].write();
  }
  void is_ok_to_read_data() {
    for (uint64_t i = 0; i < kMaxThreadNumber; i++) {
      auto &q = data_chan_[i];
      q.read();
    }
  }

  // read-index thread -> 64 visit-thread
  // you can visit key now.
  chan visit_index_chan_[kMaxThreadNumber];
  void ask_to_visit_index() {
    for (uint64_t i = 0; i < kMaxThreadNumber; i++) {
      auto &q = visit_index_chan_[i];
      q.write();
    }
  }
  // 64 visit-thread -> read-index thread
  // is ok to visit the index?
  void is_ok_to_visit_index(int thread_id) {
    visit_index_chan_[thread_id].read();
  }

  // read-data thread -> 64 visit-thread
  // you can visit key now.
  chan visit_data_chan_[kMaxThreadNumber];
  void ask_to_visit_data() {
    for (uint64_t i = 0; i < kMaxThreadNumber; i++) {
      auto &q = visit_data_chan_[i];
      q.write();
    }
  }
  // 64 visit-thread -> read-data thread
  // is ok to visit the data?
  void is_ok_to_visit_data(int thread_id) {
    visit_data_chan_[thread_id].read();
  }

 private:
  std::string dir_;
  FileLock* db_lock_ = nullptr;

  // -1 init stage
  // 0 write
  // 1 read
  // 2 range
  int stage_ = kInitStage;

  // use to pin cpu on core.
  uint64_t max_cpu_cnt_ = 0;
  // tag for the thread id,
  // just increase.
  std::atomic<uint64_t> thread_id_{0};

  // time counter
  decltype(std::chrono::system_clock::now()) begin_;
  decltype(std::chrono::system_clock::now()) end_;
};

}  // namespace polar_race
