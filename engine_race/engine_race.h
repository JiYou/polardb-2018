// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include "include/engine.h"
#include "spin_lock.h"
#include "util.h"
#include "engine_aio.h"

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
// totaly 16 bytes.
struct disk_index {
  uint64_t key = 0;
  uint32_t file_no = 0;
  uint32_t file_offset = 0;
  disk_index(uint64_t k, uint32_t fn, uint32_t ff) :
    key(k), file_no(fn), file_offset(ff) {
  }
  disk_index() { }
  bool operator < (const uint64_t k) const {
    return key < k;
  }
  bool operator < (const disk_index &x) const {
    return key < x.key;
  }
  bool operator == (const disk_index &k) const {
    return key == k.key && file_no == k.file_no && file_offset == file_offset;
  }
  bool operator != (const disk_index &k) {
    return key != k.key || file_no != k.file_no || file_offset != file_offset;
  }

  bool is_valid() const {
    return !(file_no == 0xffff && file_offset == 0xffff);
  }
};
#pragma pack(pop)

class HashTreeTable {
 public:
  RetCode GetNoLock(const std::string &key, uint32_t *file_no, uint32_t *file_offset); // no_lock
  RetCode SetNoLock(const std::string &key, uint32_t file_no, uint32_t file_offset); // no_lock
  RetCode GetNoLock(const char* key, uint32_t *file_no, uint32_t *file_offset); // no_lock
  RetCode SetNoLock(const char* key, uint32_t file_no, uint32_t file_offset); // no_lock
  RetCode SetNoLock(const uint64_t key, uint32_t file_no, uint32_t file_offset); // no_lock

  // NOTE: no lock here. Do not call it anywhere.
  // after load all the entries from disk,
  // for speed up the lookup, need to sort it.
  // after some set operations->append to the
  // hash shard vector.
  // Such as: bucket[i] has do push_back()
  // then has_sort_[i] must be false;
  // then can not use binary_search to find the item.
  void Sort();

  // print Hash Mean StdDev size of hash shard.
  void PrintMeanStdDev();

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
  aio_env_single(int fd_, bool read=true, bool alloc=true) {
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

  void Prepare16MB(uint64_t offset, char *out) {
    iocb.u.c.offset = offset;
    iocb.u.c.buf = out;
    iocb.u.c.nbytes = k16MB;
  }

  void Prepare100MB(uint64_t offset, char *out) {
    iocb.u.c.offset = offset;
    iocb.u.c.buf = out;
    iocb.u.c.nbytes = kMaxFileSize;
  }


  RetCode Submit() {
    write_over = false;
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
    write_over = true;
  }

  ~aio_env_single() {
    io_destroy(ctx);

    if (buf) {
      free(buf);
    }
  }

  int fd = -1;
  char *buf = nullptr;
  bool write_over = true;
  io_context_t ctx;
  struct iocb iocb;
  struct iocb* iocbs;
  struct io_event events;
  struct timespec timeout;
};

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
    data_buf = GetAlignedBuffer(k256KB); // 256KB for write data.
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

template<typename KVItem>
class Queue {
  public:
    Queue(size_t cap): cap_(cap) { }

    // just push the pointer of write_item into queue.
    // the address of write_item maybe local variable
    // in the stack, so the caller must wait before
    // it return from stack-function.
    void Push(KVItem *w) {
      // check the queue is full or not.
      std::unique_lock<std::mutex> l(qlock_);
      // check full or not.
      produce_.wait(l, [&] { return q_.size() != cap_; });
      q_.push_back(w);
      consume_.notify_all();
    }

    /*
     * because the read is random, wait maybe not that effective.
     * but for write, if more data flushed by one time.
     * it maybe faster that flush 2 times.
     * So, if find the items are smaller, wait for some nano seconds.
     */
    void Pop(std::vector<KVItem*> *vs, bool is_write=true) {
        std::unique_lock<std::mutex> lck(qlock_);
        consume_.wait(lck, [&] {return !q_.empty(); });
        vs->swap(q_);
        q_.clear();
        produce_.notify_all();
    }
  private:
    std::vector<KVItem*> q_;
    std::mutex qlock_;
    std::condition_variable produce_;
    std::condition_variable consume_;
    size_t cap_ = kMaxQueueSize;
};

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir,
                      size_t write_queue_size=kMaxQueueSize)
    : dir_(dir),
    db_lock_(nullptr),
    write_queue_(write_queue_size) {
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
  void BuildHashTable();
  void WriteEntry();
  void start_write_thread();
  std::string file_name_;

 private:
  std::string dir_;
  FileLock* db_lock_;

  // control of the write_queue.
  std::atomic<bool> stop_{false};
  Queue<write_item> write_queue_;

  // hash tree table.
  HashTreeTable hash_;

  // data offset, started from 1GB. add the left 4KB in
  // build hash tree table.!
  uint64_t max_data_offset_ = kMaxIndexSize;
  // index offset, start from 0 pos.
  // add the left 16 bytes in
  // build_hash_tree table.
  uint64_t max_index_offset_ = 0;

  // use to pin cpu on core.
  uint64_t max_cpu_cnt_ = 0;
  std::atomic<uint64_t> cpu_id_{0};
  std::atomic<uint64_t> kv_cnt_{0};

  // open all the data files.
  std::vector<int> data_fds_;

  // time counter
  decltype(std::chrono::system_clock::now()) begin_;
  decltype(std::chrono::system_clock::now()) end_;
};

}  // namespace polar_race
