// production consumer queue in cpp11.

#include "include/engine.h"

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

namespace polar_race {

static constexpr int32_t kMaxQueueSize = 4096; // 4K * 4Kitem ~= 16MB

struct write_item {
  const std::string *key = nullptr;
  const std::string *value = nullptr;

  int ret_code = 1;
  bool is_done = false;
  std::mutex lock_;
  std::condition_variable cond_;

  write_item(const std::string *pkey, const std::string *pvalue) :
    key(pkey), value(pvalue) {
    std::cout << "write_item::constructor()" << std::endl;
  }
  ~write_item() {
    std::cout << "write_item::destructor()" << std::endl;
  }

  void wait_done() {
    std::unique_lock<std::mutex> l(lock_);
    cond_.wait(l, [&] { return is_done; } );
  }

  void set_ret_code(int ret_code) {
    std::unique_lock<std::mutex> l(lock_);
    is_done = true;
    this->ret_code = ret_code;
    cond_.notify_all();
  }
};

class Queue {
  public:
    Queue(int32_t cap): cap_(cap) { }

    // just push the pointer of write_item into queue.
    // the address of write_item maybe local variable
    // in the stack, so the caller must wait before
    // it return from stack-function.
    void Push(write_item *w) {
      // check the queue is full or not.
      std::unique_lock<std::mutex> l(qlock_);
      // check full or not.
      produce_.wait(l, [&] { return q_.size() != cap_; });
      q_.push_back(w);
      consume_.notify_all();
    }

    void Pop(std::vector<write_item*> *vs) {
        // wait for more write here.
        qlock_.lock();
        if (q_.size() < 1024) {
          qlock_.unlock();
          // do something here.
          // is some reader blocked on the request?
          std::this_thread::sleep_for(std::chrono::nanoseconds(20));
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
    std::deque<write_item*> q_;
    std::mutex qlock_;
    std::condition_variable produce_;
    std::condition_variable consume_;
    int32_t cap_ = kMaxQueueSize;
};

class TestDB {
  public:
    TestDB(int cap): q_(cap) { }
    int Init() {
      std::thread thd(&TestDB::run, this);
      thd.detach();
    }

    void Close() {
      stop_ = true;
      std::cout << "begin to close ... " << std::endl;
    }

    int Put(const std::string &key, const std::string &value) {
      write_item w(&key, &value);

      std::cout << "w addr = " << &w << std::endl;
      q_.Push(&w);

      std::cout << "wait write over .. " << std::endl;
      std::unique_lock<std::mutex> l(w.lock_);
      w.cond_.wait(l, [&w] { return w.is_done; });

      cnt_++;
      return w.ret_code;
    }

    int Get(const std::string &key, std::string *value) {
      std::unique_lock<std::mutex> dl(db_lock_);
      auto iter = hash_.find(key);
      if (iter == hash_.end()) {
        return kNotFound;
      }
      *value = iter->second;
      return kSucc;
    }
  private:
    void run() {
      std::vector<write_item*> vs;
      std::cout << "db::run()" << std::endl;
      while (!stop_) {
        q_.Pop(&vs);
        std::cout << "vs.size() = " << vs.size() << std::endl;
        // insert the item into hash.
        for (auto &x: vs) {
          std::unique_lock<std::mutex> l(x->lock_);
          {
            std::unique_lock<std::mutex> dl(db_lock_);
            hash_[*(x->key)] = *(x->value);
          }
          x->is_done = true;
          x->ret_code = kSucc;
          x->cond_.notify_all();
        }
      }
    }
  private:
    int32_t cnt_ = 0;
    std::atomic<bool> stop_{false};
    std::unordered_map<std::string, std::string> hash_;
    std::mutex db_lock_;
    Queue q_;
};

static void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

void TEST_DB() {
  TestDB db(kMaxQueueSize);
  db.Init();

  constexpr int kMaxThread = 1024;
  std::atomic<int> cnt {0};

  auto writer = [&db, &cnt]() {
    char *kbuf = new char [101];
    char *vbuf = new char [4097];
    gen_random(kbuf, 100);
    gen_random(vbuf, 4096);

    std::string key(kbuf, 100);
    std::string val(vbuf, 4096);

    auto ret = db.Put(key, val);
    assert(ret == kSucc);

    std::string gval;
    ret = db.Get(key, &gval);
    assert(ret == kSucc);
    assert(gval == val);
    cnt ++;
    delete [] kbuf;
    delete [] vbuf;
  };

  for (int i = 0; i < kMaxThread; i++) {
    std::thread thd(writer);
    thd.detach();
  }

  while (cnt < kMaxThread) {

  }

  db.Close();
}

void TEST_Queue(void) {
  Queue q(1024);

  std::string key  = "key";
  std::string value = "value";
  write_item w(&key, &value);

  std::atomic<int> cnt {0};

  auto producer = [&q, &cnt, &w] (void) {
    for (int i = 0; i < 1024; i++) {
      q.Push(&w);
    }
    cnt++;
  };

  std::vector<write_item*> vs;

  auto consumer = [&q, &vs, &cnt](void) {
    while (cnt < 1024) {
      q.Pop(&vs);
      std::cout << "vs.size() = " << vs.size() << std::endl;
      vs.clear();
    }
  };

  for (int i = 0; i < 1024; i++) {
    std::thread thd(producer);
    thd.detach();
  }

  std::thread con(consumer);
  con.join();
}

} // end of namespace polar_race

int main(void) {
//  polar_race::TEST_Queue();
  polar_race::TEST_DB();
  return 0;
}
