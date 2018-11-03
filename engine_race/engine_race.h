// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include "engine_race/spin_lock.h"
#include "include/engine.h"
#include "engine_race/util.h"
#include "engine_race/door_plate.h"
#include "engine_race/data_store.h"

#include <assert.h>
#include <stdint.h>
#include <pthread.h>

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
    : db_lock_(NULL),
    plate_(dir),
    store_(dir),
    write_queue_(write_queue_size),
    read_queue_(read_queue_size) {
  }
#else
  explicit EngineRace(const std::string& dir)
    : db_lock_(NULL),
    plate_(dir),
    store_(dir),
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

  void WriteEntry();

#ifdef READ_QUEUE
  void ReadEntry();
#endif

  void start();

 private:
  FileLock* db_lock_;
  DoorPlate plate_;
  DataStore store_;

  std::atomic<bool> stop_{false};
  Queue<write_item> write_queue_;

#ifdef READ_QUEUE
  Queue<read_item> read_queue_;
#endif
};

}  // namespace polar_race
