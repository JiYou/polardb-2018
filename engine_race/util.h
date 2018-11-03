// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_SIMPLE_UTIL_H_
#define ENGINE_SIMPLE_UTIL_H_
#include <stdint.h>
#include <pthread.h>
#include <string>
#include <vector>
#include<iostream>

#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

namespace polar_race {

constexpr uint32_t kMaxKeyLen = 8;
constexpr uint32_t kMaxValueLen = 4096;
constexpr size_t kMaxBucketSize = 17 * 19 * 23 * 29 * 31 + 1;
constexpr int kSingleRequest = 1;
constexpr int kMinNumber = 1;
constexpr int kPageSize = 4096;
constexpr int kMetaFileNamePrefixLen = 5;
constexpr int kSplitPos = 16;
constexpr int kValueLengthBits = 12;  // stands for 4K
constexpr size_t kMaxQueueSize = 256; // 4K * 4Kitem ~= 16MB
constexpr size_t kMaxFlushItem = 64;   // because there are 64 threads r/w.

// Hash
uint32_t StrHash(const char* s, int size);

// Env
int GetDirFiles(const std::string& dir, std::vector<std::string>* result);
int GetFileLength(const std::string& file);
int FileAppend(int fd, const std::string& value);
int FileAppend(int fd, const void *buf, size_t len);
bool FileExists(const std::string& path);

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
