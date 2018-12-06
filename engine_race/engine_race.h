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

  uint64_t get_file_number() const {
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

  unsigned long WaitOver() {
    // after submit, need to wait all read over.
    while (io_getevents(ctx, kSingleRequest, kSingleRequest,
                        &events, &(timeout)) != kSingleRequest) {
      /**/
    }
    return events.res;
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

class HashTreeTable {
 public:
  RetCode GetNoLock(uint64_t key, uint64_t *file_no, uint32_t *file_offset); // no_lock
  RetCode SetNoLock(uint64_t key, uint32_t file_offset, spinlock *ar); // no_lock

  // NOTE: no lock here.
  // after sort then begin to write into file.
  void Sort(const char *file_name);
  void WaitWriteOver() {
    if (has_save_) {
      if (write_aio_) {
        write_aio_->WaitOver();
        delete write_aio_;
        write_aio_ = nullptr;
        if (all_index_fd_ > 0) {
          close(all_index_fd_);
        }
      }
    }
    has_save_ = false;
  }

 public:
  void Init(const uint32_t *hash_bucket_counter) {
    this->hash_bucket_counter_ = hash_bucket_counter;

    const uint64_t alloc_size = sizeof(struct disk_index*) * (kMaxBucketSize+1);
    bucket_iter_ = (struct disk_index**) malloc (alloc_size);
    bucket_start_pos_ = (struct disk_index**) malloc(alloc_size);
    if (!bucket_iter_ || !bucket_start_pos_) {
      DEBUG << "alloc memory failed for bucket_iter bucket_start_pos\n";
      exit(-1);
    }

    // get total number of items.
    total_item_ = 0;
    for (int i = 0; i < (int)kMaxBucketSize; i++) {
      bucket_start_pos_[i] = bucket_iter_[i] = hash_ + total_item_;
      total_item_ += hash_bucket_counter_[i];
    }
    bucket_start_pos_[kMaxBucketSize] = hash_ + total_item_;

    if (total_item_ == 64000000ul) {
      remove_dup_ = false;
    }

    // begin to malloc memory.
    // can use aio to write the file
    hash_ = (struct disk_index*)GetAlignedBuffer(mem_size());
    if (!hash_) {
      DEBUG << "malloc memory for all the item failed\n";
      exit(-1);
    }
  }

  HashTreeTable() { }
  ~HashTreeTable() {
    WaitWriteOver();
    if (hash_) {
      free(hash_);
    }
    if (bucket_iter_) {
      free(bucket_iter_);
    }
    if (bucket_start_pos_) {
      free(bucket_start_pos_);
    }
  }

 public:
  HashTreeTable(const HashTreeTable &) = delete;
  HashTreeTable(const HashTreeTable &&) = delete;
  HashTreeTable &operator=(const HashTreeTable&) = delete;
  HashTreeTable &operator=(const HashTreeTable&&) = delete;
 private:
  uint64_t total_item_ = 0;
  uint64_t mem_size() const {
    return total_item_ * sizeof(struct disk_index);
  }
 private:
  const uint32_t *hash_bucket_counter_ = nullptr;
  struct disk_index *hash_ = nullptr;
  // record the start position of every bucket.
  struct disk_index **bucket_iter_ = nullptr;
  struct disk_index **bucket_start_pos_ = nullptr;
  bool remove_dup_ = true;
  bool has_save_ = false;
  struct aio_env_single *write_aio_ = nullptr;
  int all_index_fd_ = -1;
 private:
  uint32_t compute_pos(uint64_t key);
  RetCode find(uint64_t key, struct disk_index**ptr);
};

template<int numberOfEvent>
struct aio_env_range {
  aio_env_range() {
    // prepare the io context.
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(numberOfEvent, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;

    memset(&iocb, 0, sizeof(iocb));
    iocbs = (struct iocb**) malloc(sizeof(struct iocb*) * numberOfEvent);
    for (int i = 0; i < numberOfEvent; i++) {
      iocbs[i] = iocb + i;
      iocb[i].aio_lio_opcode = IO_CMD_PREAD;
      iocb[i].aio_reqprio = 0;
      iocb[i].u.c.nbytes = kPageSize;
      // iocb->u.c.offset = offset;
      // iocb->aio_fildes = fd;
      // iocb->u.c.buf = buf;
    }
  }

  void PrepareRead(int fd, uint64_t offset, char *out, uint32_t size) {
    // prepare the io
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PREAD;
    index++;
  }

  void PrepareWrite(int fd, uint64_t offset, char *out, uint32_t size) {
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size;
    iocb[index].aio_lio_opcode = IO_CMD_PWRITE;
    index++;
  }

  void Clear() {
    index = 0;
  }

  int Size() const {
    return index;
  }

  bool Full() {
    return index == numberOfEvent;
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
      write_over_cnt += num_events;
    }
  }

  ~aio_env_range() {
    free(iocbs);
    io_destroy(ctx);
  }

  int fd = 0;
  int index = 0;
  io_context_t ctx;
  struct iocb iocb[numberOfEvent];
  // struct iocb* iocbs[numberOfEvent];
  struct iocb** iocbs = nullptr;
  struct io_event events[numberOfEvent];
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

      if (0 == i) {
        std::string str(kPageSize, '.');
        if (write(data_fd_[0], str.c_str(), kPageSize) != kPageSize) {
          DEBUG << "write file 0 first 4K meet error\n";
          exit(-1);
        }
        data_fd_len_[0] = kPageSize;
      }
    }
    has_open_data_fd_ = true;
  }

 // Read Stage
 private:
  void init_read();
  HashTreeTable hash_; // for the read index.
  uint32_t *hash_bucket_counter_ = nullptr;
  void build_hash_counter() {
    // open all the hash_counte files
    char path[kPathLength];
    hash_bucket_counter_ = (uint32_t*) malloc(kMaxHashFileSize);
    if (!hash_bucket_counter_) {
      DEBUG << "malloc for hash_counter meet error\n";
      exit(-1);
    }

    char *buf = GetAlignedBuffer(kMaxHashFileSize * kMaxThreadNumber); // 64KB * 64thread
    struct aio_env_range<kMaxThreadNumber> read_aio;
    read_aio.Clear();
    for (int i = 0; i < (int)kMaxThreadNumber; i++) {
      sprintf(path, "%sindex/%d_hash", file_name_.c_str(), i);
      int fd = open(path, O_DIRECT | O_NOATIME | O_RDONLY, 0644);
      if (fd < 0) {
        // if it's single thread, the file maybe not exist.
        continue;
      }
      read_aio.PrepareRead(fd, 0, buf + i * kMaxHashFileSize, kMaxHashFileSize);
    }
    read_aio.Submit();
    read_aio.WaitOver();

    memset(hash_bucket_counter_, 0, kMaxHashFileSize);
    for (int i = 0; i < (int)kMaxThreadNumber; i++) {
      uint32_t *single_thread_counter = reinterpret_cast<uint32_t*>(
              buf + i * kMaxHashFileSize);
      for (uint32_t j = 0; j < kMaxBucketSize; j++) {
        hash_bucket_counter_[j] += single_thread_counter[j];
      }
    }
    free(buf);
  }

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
