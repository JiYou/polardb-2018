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

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

// Is use read queue?
// #define READ_QUEUE 1

// perf counter.
#define PERF_COUNT 1

#ifdef PERF_COUNT
  #define BEGIN_POINT(x)  auto x = std::chrono::system_clock::now()
  #define END_POINT(y,x, msg)  do {                                                                       \
    auto y = std::chrono::system_clock::now();                                                            \
    auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(y - x);                              \
    std::cout << msg << " " << diff.count() / kNanoToMS << " (ms)" << std::endl;                          \
  } while (0)
#else
  #define BEGIN_POINT(x)
  #define END_POINT(y,x, msg)
#endif

#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"(): "<<" [" <<strerror(errno) << "] "

#define ROUND_UP_1KB(x) (((x) + 1023) & (~1023))
#define ROUND_DOWN_1KB(x) ((x) & (~1023))

namespace polar_race {

static const std::string kMetaDirName = "index";
static const std::string kDataDirName = "data";

constexpr size_t kCacheLineSize = 128;

constexpr int64_t kNanoToMS = 1000000;
constexpr uint32_t kMaxKeyLen = 8;
constexpr uint32_t kMaxValueLen = 4096;
constexpr size_t kMaxBucketSize = 65536; // 64K
constexpr uint64_t kMaxHashFileSize = kMaxBucketSize * sizeof(uint32_t);
constexpr int kSingleRequest = 1;
constexpr int kMinNumber = 1;
constexpr uint64_t kPageSize = 4096;
constexpr uint64_t k256KB = kPageSize * 64; // 256KB
constexpr uint64_t k1KB = 1024;
constexpr uint64_t kReadValueCnt = 1024;
constexpr uint64_t k4MB = kPageSize * 1024;
constexpr uint64_t k16MB = k4MB * 4;
constexpr uint64_t k256MB = k4MB * 64;
constexpr uint64_t kMaxIndexSize = 1024 * 1024 * 1024; // 1GB
//constexpr uint64_t kMaxIndexSize{2 * k16MB * 64ull}; // 2GB
constexpr uint64_t kBigFileSize{263433748480};
// constexpr uint64_t kBigFileSize{279172874240ull}; // 260GB
constexpr int kValueLengthBits = 12;  // stands for 4K
constexpr size_t kMaxQueueSize = 128; // 4K * 4Kitem ~= 16MB
constexpr int kMaxThreadNumber = 64; // max number of thread.
constexpr int kMaxIOEvent = 64;
constexpr uint64_t kMaxDataFileSize = 16777216ull; // 16MB
constexpr uint64_t kMaxIndexFileSize = 12582912ull; // 24MB
constexpr uint64_t kMaxFileNumber = 65536;
constexpr uint64_t kPathLength = 64; // path length
constexpr uint64_t kMaxKVItem = 64000000; // 64M

constexpr uint32_t kThreadShardNumber = 256;
// is key/value disk_item type.
constexpr uint32_t kValidType = 1;

constexpr int kInitStage = -1;
constexpr int kWriteStage = 0;
constexpr int kReadStage = 1;
constexpr int kRangeStage = 2;

// use a maxium skip type to jump out the invalid item.
constexpr uint64_t kIndexSkipType{18446744073709551615ull};

struct wait_item {
  RetCode ret_code = kSucc;
  bool is_done = false;
  std::mutex lock_;
  std::condition_variable cond_;

  void wait_done() {
    std::unique_lock<std::mutex> l(lock_);
    cond_.wait(l, [&] { return is_done; } );
  }

  void feed_back() {
    std::unique_lock<std::mutex> l(lock_);
    is_done = true;
    ret_code = kSucc;
    cond_.notify_one(); // just one wait there.
  }
};

struct write_item : wait_item {
  const PolarString *key = nullptr;
  const PolarString *value = nullptr;
  write_item(const PolarString *pkey, const PolarString *pvalue) :
    key(pkey), value(pvalue) {
  }
};

struct read_item : public wait_item {
  uint64_t pos;
  char *buf;
  read_item(uint64_t p, char *b): pos(p), buf(b) { }
};

struct disk_index;
struct visitor_item : public wait_item {
  uint64_t start;
  uint64_t end;
  Visitor *vs = nullptr;
  visitor_item(uint64_t s, uint64_t e, Visitor *vis):
    start(s), end(e), vs(vis) { }
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

// change the string input to uint64_t
// NOTE: this function is not simply change
// the value to static_cast<uint64_t>
// the changed value must keep the memcmp
// order.
// Such as:
//  - abcdefg < abcdefgh
// the convert result must also keep the
// same order.
uint64_t toInt(const char *s, uint64_t n);
uint64_t toInt(const std::string &s);
uint64_t toInt(const PolarString &ps);

uint64_t toBack(uint64_t be);

// head of the string, uin16_t
// string = "abcdefgh"
// hv = "ab"
// little endian is "ba"
uint16_t head(const char *s, uint64_t n);
uint16_t head(const std::string &s);
uint16_t head(const PolarString &ps);
uint16_t head(const uint64_t key);

}  // namespace polar_race

#endif  // ENGINE_SIMPLE_UTIL_H_
