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
    return !(file_no == 0xffffffff && file_offset == 0xffffffff);
  }

  void set_file_no(int thread_id, int fn) {
    file_no = (thread_id<<16) | fn;
  }

  int get_thrad_id() const {
    return file_no >> 16;
  }

  int get_file_no() const {
    return file_no & 0xffff;
  }
};
#pragma pack(pop)

class HashTreeTable {
 public:
  RetCode GetNoLock(const std::string &key, uint32_t *file_no, uint32_t *file_offset); // no_lock
  RetCode GetNoLock(const char* key, uint32_t *file_no, uint32_t *file_offset); // no_lock
  RetCode SetNoLock(const char* key, uint32_t file_no, uint32_t file_offset, spinlock *ar); // no_lock

  // NOTE: no lock here. Do not call it anywhere.
  // after load all the entries from disk,
  // for speed up the lookup, need to sort it.
  // after some set operations->append to the
  // hash shard vector.
  void Sort();

  // print Hash Mean StdDev size of hash shard.
  void PrintMeanStdDev();

  // for range scan, return all the items
  // in a single vector.
  std::vector<struct disk_index> &GetAll() { return all_; }

  // save all the index files into single file.
  void Save(const char *file_name);
  bool Load(const char *file_name);

  // copy to all vector.
  // return: true have copy
  //         false, not copy.
  bool CopyToAll();

 public:
  void Init(bool is_hash) {
    if (is_hash) {
      hash_.resize(kMaxBucketSize);
    } else {
      std::vector<std::vector<struct disk_index>>().swap(hash_);
      all_.clear();
    }
    is_hash_ = is_hash;
  }

  HashTreeTable() { }
  ~HashTreeTable() { }

 public:
  HashTreeTable(const HashTreeTable &) = delete;
  HashTreeTable(const HashTreeTable &&) = delete;
  HashTreeTable &operator=(const HashTreeTable&) = delete;
  HashTreeTable &operator=(const HashTreeTable&&) = delete;
 private:
  bool is_hash_ = true;
  std::vector<struct disk_index> all_;
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

template<typename KVItem>
class Queue {
  public:
    Queue(size_t cap): cap_(cap) { }

    // just push the pointer of write_item into queue.
    // the address of write_item maybe local variable
    // in the stack, so the caller must wait before
    // it return from stack-function.
    void Push(KVItem w) {
      // check the queue is full or not.
      std::unique_lock<std::mutex> l(qlock_);
      // check full or not.
      produce_.wait(l, [&] { return q_.size() != cap_; });
      q_.push_back(w);
      consume_.notify_all();
    }

    uint64_t Size() {
      std::unique_lock<std::mutex> lck(qlock_);
      return q_.size();
    }

    /*
     * because the read is random, wait maybe not that effective.
     * but for write, if more data flushed by one time.
     * it maybe faster that flush 2 times.
     * So, if find the items are smaller, wait for some nano seconds.
     */
    void Pop(std::vector<KVItem> *vs, bool is_write=true) {
        if (is_write && !has_wait) {
          for (int i = 0; i < 1024 && Size() < kMaxThreadNumber; i++) {
            DEBUG << "i = " << i << "Q size = " << Size() << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
          }
          has_wait = true;
        }
        std::unique_lock<std::mutex> lck(qlock_);
        consume_.wait(lck, [&] {return !q_.empty(); });
        vs->swap(q_);
        q_.clear();
        produce_.notify_all();
    }

  void SetNoWait() {
    has_wait = true;
  }
  private:
    bool has_wait = false;
    std::vector<KVItem> q_;
    std::mutex qlock_;
    std::condition_variable produce_;
    std::condition_variable consume_;
    size_t cap_ = kMaxQueueSize;
};

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir)
    : dir_(dir), q_(kMaxThreadNumber * 2) {
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
  void RangeEntry();
  void BuildHashTable(bool is_hash);
  const char *AllIndexFile() { return all_index_file_.c_str(); }
  std::string all_index_file_;
  std::string file_name_;

 private:
  std::string dir_;
  FileLock* db_lock_ = nullptr;

  // -1 init stage
  // 0 write
  // 1 read
  // 2 range
  int stage_ = kInitStage;

  // hash tree table.
  HashTreeTable hash_;

  // use to pin cpu on core.
  uint64_t max_cpu_cnt_ = 0;
  // tag for the thread id,
  // just increase.
  std::atomic<uint64_t> thread_id_{0};

  // open all the data files.
  std::vector<std::vector<int>> data_fds_;

  std::atomic<bool> stop_{false};

  // queue for the range scan
  Queue<visitor_item*> q_;

  // time counter
  decltype(std::chrono::system_clock::now()) begin_;
  decltype(std::chrono::system_clock::now()) end_;
};

}  // namespace polar_race
