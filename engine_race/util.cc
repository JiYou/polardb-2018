// Copyright [2018] Alibaba Cloud All rights reserved

#include "util.h"
#include <errno.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <byteswap.h>

#include <vector>
#include <algorithm>
#include <math.h>
#include <numeric>

namespace polar_race {

void ComputeMeanSteDev(const std::vector<size_t> &resultSet, double *mean_, double *std_) {
  double sum = std::accumulate(std::begin(resultSet), std::end(resultSet), 0.0);
  double mean =  sum / resultSet.size();
  double accum  = 0.0;
  std::for_each (std::begin(resultSet), std::end(resultSet), [&](const double d) {
    accum  += (d-mean)*(d-mean);
  });
  double stdev = sqrt(accum/(resultSet.size()-1));
  *mean_ = mean;
  *std_ = stdev;
}

static const int kA = 54059;  // a prime
static const int kB = 76963;  // another prime
static const int kFinish = 37;  // also prime
uint32_t StrHash(const char* s, int size) {
  uint32_t h = kFinish;
  while (size > 0) {
    h = (h * kA) ^ (s[0] * kB);
    s++;
    size--;
  }
  return h;
}

int GetDirFiles(const std::string& dir, std::vector<std::string>* result) {
  int res = 0;
  result->clear();
  DIR* d = opendir(dir.c_str());
  if (d == NULL) {
    return errno;
  }
  struct dirent* entry;
  while ((entry = readdir(d)) != NULL) {
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    result->push_back(entry->d_name);
  }
  closedir(d);
  return res;
}

int GetFileLength(const std::string& file) {
  struct stat stat_buf;
  int rc = stat(file.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

int FileAppend(int fd, const std::string& value) {
  if (fd < 0) {
    return -1;
  }
  size_t value_len = value.size();
  const char* pos = value.data();
  while (value_len > 0) {
    ssize_t r = write(fd, pos, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      return -1;
    }
    pos += r;
    value_len -= r;
  }
  return 0;
}

int FileAppend(int fd, const void *buf, size_t len) {
  if (fd < 0) {
    return -1;
  }

  size_t value_len = len;
  const char* pos = static_cast<const char*>(buf);
  while (value_len > 0) {
    ssize_t r = write(fd, pos, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      return -1;
    }
    pos += r;
    value_len -= r;
  }
  return 0;
}



bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

int LockFile(const std::string& fname, FileLock** lock) {
  *lock = NULL;
  int result = 0;
  int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    result = errno;
  } else if (LockOrUnlock(fd, true) == -1) {
    result = errno;
    close(fd);
  } else {
    FileLock* my_lock = new FileLock;
    my_lock->fd_ = fd;
    my_lock->name_ = fname;
    *lock = my_lock;
  }
  return result;
}

int UnlockFile(FileLock* lock) {
  int result = 0;
  if (LockOrUnlock(lock->fd_, false) == -1) {
    result = errno;
  }
  close(lock->fd_);
  delete lock;
  return result;
}

char *GetAlignedBuffer(uint64_t bufer_size) {
  char *buf = nullptr;
  if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, bufer_size)) {
      DEBUG << "posix_memalign failed!\n";
      return nullptr;
  }
  return buf;
}

uint64_t toInt(const char *s, uint64_t n) {
  uint64_t ret = 0;
  // the string is 8 bytes.
  if (n == kMaxKeyLen) {
    ret = *reinterpret_cast<const uint64_t*>(s);
    return bswap_64(ret);
  }

  char *b = (char*)(&ret);
  for (uint32_t i = 0; i < n && i < kMaxKeyLen; i++) {
    b[kMaxKeyLen-i-1] = s[i];
  }
  return ret;
}

uint64_t toBack(uint64_t be) {
  return bswap_64(be);
}

uint64_t toInt(const std::string &s) {
  return toInt(s.c_str(), s.length());
}

uint64_t toInt(const PolarString &ps) {
  return toInt(ps.data(), ps.size());
}

uint16_t head(const char *s, uint64_t n) {
  return toInt(s, n) >> 48;
}

uint16_t head(const std::string &s) {
  return head(s.c_str(), s.length());
}

uint16_t head(const PolarString &ps) {
  return head(ps.data(), ps.size());
}

uint16_t head(const uint64_t key) {
  return key >> 48;
}

}  // namespace polar_race
