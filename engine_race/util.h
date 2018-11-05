// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_SIMPLE_UTIL_H_
#define ENGINE_SIMPLE_UTIL_H_

#include "include/polar_string.h"
#include "include/engine.h"

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
#include <numeric>
#include <atomic>
#include <string>
#include <queue>
#include <map>

// Is use read queue?
// #define READ_QUEUE 0

// perf counter.
//#define PERF_COUNT 0 

#ifdef PERF_COUNT
  #define BEGIN_POINT(x)  auto x = std::chrono::system_clock::now()
  #define END_POINT(y,x, msg)  do {                                                                       \
    auto y = std::chrono::system_clock::now();                                                            \
    auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(data_end_time - data_start_time);    \
    std::cout << msg << diff.count() / kNanoToMS << " (ms)" << std::endl;                                 \
  } while (0)
#else
  #define BEGIN_POINT(x)
  #define END_POINT(y,x, msg)
#endif

#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

namespace polar_race {

constexpr int64_t kNanoToMS = 1000000;
constexpr uint32_t kMaxKeyLen = 8;
constexpr uint32_t kMaxValueLen = 4096;
constexpr size_t kMaxBucketSize = 17 * 19 * 23 * 29 * 31 + 1;
constexpr int kSingleRequest = 1;
constexpr int kMinNumber = 1;
constexpr int kPageSize = 4096;
constexpr uint32_t kReadValueCnt = 1024;
constexpr uint32_t k4MB = kPageSize * 1024;
constexpr uint32_t kMaxIndexSize = 1024 * 1024 * 1024; // 1GB
constexpr int kLastCharIn4MB = k4MB - 1;
constexpr uint64_t k251230MB = 251230;
constexpr uint64_t kBigFileSize{263433748480};
constexpr int kValueLengthBits = 12;  // stands for 4K
constexpr size_t kMaxQueueSize = 128; // 4K * 4Kitem ~= 16MB
constexpr int kMaxThreadNumber = 64; // max number of thread.
constexpr int kMaxIOEvent = 64;

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

void ComputeMeanSteDev(const std::vector<size_t> &vs, double *mean, double *std);

// Hash
uint32_t StrHash(const char* s, int size);

// Env
int GetDirFiles(const std::string& dir, std::vector<std::string>* result);
int GetFileLength(const std::string& file);
int FileAppend(int fd, const std::string& value);
int FileAppend(int fd, const void *buf, size_t len);
bool FileExists(const std::string& path);

// get aligned buffer size.
char *GetAlignedBuffer(uint64_t buffer_size);

// FileLock
class FileLock  {
 public:
    FileLock() {}
    virtual ~FileLock() {}

    int fd_;
    std::string name_;

 private:
    // No copying allowed
    FileLock(const FileLock&);
    void operator=(const FileLock&);
};

int LockFile(const std::string& f, FileLock** l);
int UnlockFile(FileLock* l);

}  // namespace polar_race

#endif  // ENGINE_SIMPLE_UTIL_H_
