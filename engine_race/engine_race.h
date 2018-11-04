// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include "include/engine.h"
#include "spin_lock.h"
#include "util.h"
#include "libaio.h"

#include <assert.h>
#include <stdint.h>
#include <pthread.h>

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
// totaly 16 bytes. devided by 4096, easy for read.
struct disk_index {
  char key[kMaxKeyLen];  // 8 byte
  uint32_t offset;       // 4 byte
  uint32_t valid;        // 4 byte
};
#pragma pack(pop)

class HashTreeTable {
 public:
  // Just these 2 function with lock
  RetCode Get(const std::string &key, uint64_t *l); // with_lock
  RetCode Set(const std::string &key, uint64_t l); // with_lock
  RetCode Get(const char* key, uint64_t *l); // with_lock
  RetCode Set(const char* key, uint64_t l); // with_lock

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
  HashTreeTable(): hash_lock_(kMaxBucketSize) {
    hash_.resize(kMaxBucketSize);
  }
  ~HashTreeTable() { }

 private:
  void LockHashShard(uint32_t index);
  void UnlockHashShard(uint32_t index);

 public:
  HashTreeTable(const HashTreeTable &) = delete;
  HashTreeTable(const HashTreeTable &&) = delete;
  HashTreeTable &operator=(const HashTreeTable&) = delete;
  HashTreeTable &operator=(const HashTreeTable&&) = delete;
 private:
  // uint64 & uint32_t would taken 16 bytes, if not align with bytes.
  #pragma pack(push, 1)
  struct kv_info {
    uint64_t key;
    uint32_t offset_4k_; // just the 4k offset in big file.
                         // if you want get the right pos, need to <<= 12;
    kv_info(uint64_t k, uint32_t v): key(k), offset_4k_(v) { }
    kv_info(): key(0), offset_4k_(0) { }
    bool operator < (const uint64_t k) const {
      return key < k;
    }
    bool operator < (const kv_info &x) const {
      return key < x.key;
    }
    bool operator == (const kv_info &k) const {
      return key == k.key && offset_4k_ == k.offset_4k_;
    }
    bool operator != (const kv_info &k) {
      return key != k.key || offset_4k_ != k.offset_4k_;
    }
  };
  #pragma pack(pop)
  std::vector<std::vector<kv_info>> hash_;
  std::vector<spinlock> hash_lock_;
  std::bitset<kMaxBucketSize> has_sort_;
  // the starting write pos of 4K data.
  uint64_t next_write_pos_ = 0;
 private:
  uint32_t compute_pos(uint64_t key);
  RetCode find(std::vector<kv_info> &vs, bool sorted, uint64_t key, kv_info **ptr);
};

// This env just support read 1 request a time.
struct aio_env {
  aio_env(bool inbuf=true) {
    // !!! must align, do not use value->data() directly.
    if (inbuf) {
      buf = GetAlignedBuffer(k4MB);
    }

    // prepare the io context.
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(1, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;

    iocbs = &iocb;

    memset(&iocb, 0, sizeof(iocb));
    // iocb->aio_fildes = fd;
    iocb.aio_lio_opcode = IO_CMD_PREAD;
    iocb.aio_reqprio = 0;
    iocb.u.c.buf = buf;
    // iocb->u.c.buf = buf;
    iocb.u.c.nbytes = k4MB;
    // iocb->u.c.offset = offset;
  }

  void Prepare(int fd, uint64_t offset, char *out=nullptr, uint32_t size=k4MB) {
    // prepare the io
    iocb.aio_fildes = fd;
    iocb.u.c.offset = offset;
    if (out) {
      iocb.u.c.buf = out;
    }
    if (size != k4MB) {
      iocb.u.c.nbytes = size;
    }
  }

  RetCode Submit() {
    if ((io_submit(ctx, kSingleRequest, &iocbs)) != kSingleRequest) {
      DEBUG << "io_submit meet error, " << std::endl;;
      printf("io_submit error\n");
      return kIOError;
    }
    return kSucc;
  }

  RetCode WaitOver() {
    // after submit, need to wait all read over.
    while (io_getevents(ctx, kSingleRequest, kSingleRequest,
                      &(events), &(timeout)) != kSingleRequest) {
      /**/
    }
    return kSucc;
  }

  ~aio_env() {
    io_destroy(ctx);
    free(buf);
  }

  io_context_t ctx;
  char *buf = nullptr;
  struct iocb iocb;
  struct iocb* iocbs = nullptr;
  struct io_event events;
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
        // wait for more write here.
        qlock_.lock();
        if (q_.size() < kMaxFlushItem && is_write) {
          qlock_.unlock();
          // do something here.
          // is some reader blocked on the request?
          std::this_thread::sleep_for(std::chrono::nanoseconds(4));
        } else {
          qlock_.unlock();
        }

        std::unique_lock<std::mutex> lck(qlock_);
        consume_.wait(lck, [&] {return !q_.empty() ; });
        vs->clear();
        // get all the items.
        std::copy(q_.begin(), q_.end(), std::back_inserter((*vs)));
        q_.clear();
        produce_.notify_all();
    }
  private:
    std::deque<KVItem*> q_;
    std::mutex qlock_;
    std::condition_variable produce_;
    std::condition_variable consume_;
    size_t cap_ = kMaxQueueSize;
};

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

#ifdef READ_QUEUE
  explicit EngineRace(const std::string& dir,
                      size_t write_queue_size=kMaxQueueSize,
                      size_t read_queue_size=kMaxQueueSize)
    : dir_(dir),
    db_lock_(nullptr),
    write_queue_(write_queue_size),
    read_queue_(read_queue_size) {
  }
#else
  explicit EngineRace(const std::string& dir,
                      size_t write_queue_size=kMaxQueueSize)
    : dir_(dir),
    db_lock_(nullptr),
    write_queue_(write_queue_size) {
  }
#endif

  ~EngineRace();

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

#ifdef READ_QUEUE
  void ReadEntry();
#endif

  void start();

 private:
  std::string dir_;
  FileLock* db_lock_;
  int fd_ = -1;
  char *write_data_buf_ = nullptr; //  256KB
  char *index_buf_ = nullptr; // 4KB
  char *read_data_buf_ = nullptr; // 4MB
  std::atomic<bool> stop_{false};
  Queue<write_item> write_queue_;
  HashTreeTable hash_;

  // started from 1GB. add the left 4KB in
  // build hash tree table.!
  uint64_t max_data_offset_ = kMaxIndexSize - kPageSize;
#ifdef READ_QUEUE
  Queue<read_item> read_queue_;
#endif

#ifdef PERF_COUNT

  // perfcounters for the read items.
  //
  // How many interval of items to print stat time.
  std::atomic<uint64_t> print_interval_{10000};
  std::atomic<uint64_t> item_cnt_{0};
  // all time is in nonoseconds.
  // record the hash lookup time.
  std::atomic<uint64_t> hash_time_cnt_{0};
  // record the total disk_io read time.
  std::atomic<uint64_t> io_time_read_cnt_{0};
  // record the total disk_io write time.
  std::atomic<uint64_t> io_time_write_cnt_{0};
#endif
};

}  // namespace polar_race
