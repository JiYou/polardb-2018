// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include "include/engine.h"
#include "spin_lock.h"
#include "util.h"
#include "libaio.h"

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
// totaly 16 bytes. devided by 4096, easy for read.
struct disk_index {
  char key[kMaxKeyLen];      // 8 byte
  uint32_t offset_4k_;       // 4 byte
  uint32_t valid;            // 4 byte
  void SetKey(uint64_t *k) {
    uint64_t *a = reinterpret_cast<uint64_t*>(key);
    *a = *k;
  }
};
#pragma pack(pop)

class HashTreeTable {
 public:
  // note, l_bytes means the location in big file
  // related to bytes, not 4KB
  RetCode Get(const std::string &key, uint64_t *l_bytes); // with_lock
  RetCode Set(const std::string &key, uint64_t l_bytes); // with_lock
  RetCode Get(const char* key, uint64_t *l_bytes); // with_lock
  RetCode Set(const char* key, uint64_t l_bytes); // with_lock

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

// this struct does not manage file & buffer
// it just set env for aio.
// Usage:
// struct aio_env io_;
// io_.SetFD(fd);
// io_.PrepareRead();
// io_.PrepareRead();
// io_.Submit();
// io_.WaitOver();
// io_.Clear();

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
      iocb[i].aio_lio_opcode = IO_CMD_PREAD;
      iocb[i].aio_reqprio = 0;
      iocb[i].u.c.nbytes = kPageSize;
      // iocb->u.c.offset = offset;
      // iocb->aio_fildes = fd;
      // iocb->u.c.buf = buf;
    }
  }

  void PrepareRead(uint64_t offset, char *out, uint32_t size, wait_item* item=nullptr) {
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
    for (int i = 0; i < 20; i++) {
      std::cout << "data[] = " << out[i] << std::endl;
    }
    std::cout << "JIYOU = " << size << std::endl;
    iocb[index].aio_fildes = fd;
    iocb[index].u.c.offset = offset;
    iocb[index].u.c.buf = out;
    iocb[index].u.c.nbytes = size < kPageSize ? kPageSize : size; // FIX TODO
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
    std::cout << "index = " << index << std::endl;
    while (write_over_cnt != index) {
      constexpr int min_number = 1;
      int num_events = io_getevents(ctx, min_number, index, events, &timeout);
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
        // wait for more write here.
        qlock_.lock();
        if (q_.size() < kMaxThreadNumber && is_write) {
          qlock_.unlock();
          // do something here.
          // is some reader blocked on the request?
          std::this_thread::sleep_for(std::chrono::nanoseconds(1));
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

  // point to the big file.
  int fd_ = -1;
  // read aio, NOTE: no thread safe.
  struct aio_env write_aio_;
  // write aio: NOTE: no thread safe.
  struct aio_env read_aio_;

  char *write_data_buf_ = nullptr; //  256KB
  char *index_buf_ = nullptr; // 4KB
  // char *read_data_buf_ = nullptr; // 4MB

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
