// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include "engine_race/splin_lock.h"
#include "include/engine.h"
#include "engine_race/util.h"
#include "engine_race/door_plate.h"
#include "engine_race/data_store.h"

#include <assert.h>
#include <stdint.h>

#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <deque>
#include <vector>
#include <atomic>

#include <pthread.h>
#include <string>
#include <queue>
#include <map>

namespace polar_race {

static constexpr size_t kMaxQueueSize = 256; // 4K * 4Kitem ~= 16MB
static constexpr size_t kMaxFlushItem = 64;   // because there are 64 threads r/w.

struct write_item {
  const PolarString *key = nullptr;
  const PolarString *value = nullptr;

  RetCode ret_code = kSucc;
  bool is_done = false;
  std::mutex lock_;
  std::condition_variable cond_;

  write_item(const PolarString *pkey, const PolarString *pvalue) :
    key(pkey), value(pvalue) {
  }

  void wait_done() {
    std::unique_lock<std::mutex> l(lock_);
    cond_.wait(l, [&] { return is_done; } );
  }
};

struct read_item {
  const PolarString *key = nullptr;
  std::string *value = nullptr;

  RetCode ret_code = kSucc;
  bool is_done = false;
  std::mutex lock_;
  std::condition_variable cond_;

  read_item(const PolarString *pkey, std::string *pvalue) :
    key(pkey), value(pvalue) {
  }

  void wait_done() {
    std::unique_lock<std::mutex> l(lock_);
    cond_.wait(l, [&] { return is_done; } );
  }
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

  explicit EngineRace(const std::string& dir,
                      size_t write_queue_size=kMaxQueueSize,
                      size_t read_queue_size=kMaxQueueSize)
    : mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(NULL),
    plate_(dir),
    store_(dir),
    write_queue_(write_queue_size),
    read_queue_(read_queue_size) {
  }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

 private:
  void WriteEntry();
  void ReadEntry();
  void start();

 private:
  pthread_mutex_t mu_;
  FileLock* db_lock_;
  DoorPlate plate_;
  DataStore store_;

  std::atomic<bool> stop_{false};
  Queue<write_item> write_queue_;
  Queue<read_item> read_queue_;
};

}  // namespace polar_race
