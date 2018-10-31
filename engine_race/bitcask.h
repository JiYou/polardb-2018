#pragma once

#include <iostream>
#include <cstring>
#include <unordered_map>
#include <vector>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

namespace polar_race {
namespace bitcask {

const std::string IndexDirectory = "/index";
const std::string DataDirectory = "/db";
const std::string DataFileName = "data";
const std::string HintFileName = "hint";
const std::string LOCK = "/LOCK";

static constexpr int kMaxKeyLength = 8;
static constexpr int kMaxValueLength = 4096;

// Key-value
struct BitcaskData {
  char value[kMaxValueLength];
};

// Index
struct BitcaskIndex {
  char key[8];
  int32_t data_pos;
  int32_t value_len;
  int16_t file_id;
  int8_t key_len;
  int8_t vaild;
};

constexpr int kMaxDataFileSize = 1024 * 1024 * 100;
constexpr int kMaxIndexFileSize = 1024 * 1024 * 100;

struct Options {
  Options() :
    max_file_size(kMaxDataFileSize),
    max_index_size(kMaxIndexFileSize),
    read_write(true) { }

  // The size of data file
  size_t max_file_size;

  // The size of index file
  size_t max_index_size;

  // If this process is going to be a writer and not just a reader
  bool read_write;

};

class Status;
class Env;
class FileLock;

// Bitcask implement
class Bitcask {
 public:
  Bitcask();

  ~Bitcask();

  Status Open(const Options& options, const std::string& dbname);

  Status Put(const std::string& key, const std::string& value);

  Status Get(const std::string& key, std::string* value);

  Status Delete(const std::string& key);

  Status Close();

  FileLock* db_lock_;
  std::string dbname_;
private:
  Options options_;
  std::unordered_map<std::string, BitcaskIndex> index_;

  // Active data file
  int32_t active_id_;
  int64_t active_file_size_;
  int active_file_ = -1;

  // Hint file
  int32_t hint_id_;
  int64_t hint_file_size_;
  int hint_file_ = -1;

  // file id -> fd & pos.
  struct file_info {
    int fd;
    int pos;
    file_info(int f, int p): fd(f), pos(p) { }
  };
  std::unordered_map<int, file_info> fd_cache_;

  Env * env_;

  Status Init();
  Status NewFileWriter(const int32_t & id,
                       int64_t& file_size,
                       const std::string& directory,
                       const std::string& filename,
                       int *fd);

  int64_t SyncData(const BitcaskData& data);

  Status SyncIndex(const BitcaskIndex& index);

  Status Retrieve(const BitcaskIndex& index, std::string* value);

  //Status Merge();

  Status LoadIndex(const std::string& file);

  Bitcask(const Bitcask& b);
  void operator=(const Bitcask& b);
};

class Status {
 public:
  Status() : code_(kOk) { }
  virtual ~Status() { }

  static Status OK() { return Status(); }

  static Status NotFound(const std::string& msg) {
    return Status(kNotFound, msg);
  }
  static Status IOError(const std::string& msg) {
    return Status(kIOError, msg);
  }

  bool ok() const { return (code_ == kOk); }

  bool IsNotFound() const { return code() == kNotFound; }

  bool IsIOError() const { return code() == kIOError; }

  std::string ToString() const {
      return msg_;
  }
 private:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kIOError = 2
  };

  Code code_;
  std::string msg_;

  Code code() const {
    return code_;
  }

  Status(Code code, const std::string& msg)
          : code_(code),
          msg_(msg) { }
};

class FileLock {
 public:
  FileLock() { }
  ~FileLock() { }

    int fd_;
    std::string name_;
 private:
  // No copying allowd
  FileLock(const FileLock&);
  void operator=(const FileLock&);
};

class Env {
 public:
  Status CreateDir(const std::string& name);
  time_t TimeStamp() {
    time_t timer;
    time(&timer);
    return timer;
  }

  Status LockFile(const std::string& name, FileLock** l);

  Status UnlockFile(FileLock* l);

  Status GetChildren(const std::string& name, std::vector<std::string>& files);

  int32_t FindMaximumId(const std::vector<std::string>& files);

  bool FileExists(const std::string& name);
 private:
  static int LockOrUnlock(int fd, bool lock);

  Env(const Env& env);
  void operator=(const Env& env);
};

} // namespace bitcask
} // namespace polar_race
