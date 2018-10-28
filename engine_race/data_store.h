#pragma once


#include "include/engine.h"
#include "engine_race/engine_cache.h"
#include <string.h>
#include <unistd.h>

#include <string>

namespace polar_race {

struct Location {
  Location() : file_no(0), offset(0), len(0) {
  }
  uint32_t file_no;
  uint32_t offset;
  uint32_t len;
};

inline bool operator==(const Location &l, const Location& r) {
  return l.file_no == r.file_no && l.offset == r.offset;
}

} // end of namespace polar_race


namespace std {
  template<>
  struct hash<polar_race::Location> {
    uint64_t operator()(const polar_race::Location &r) const {
      uint64_t ret = r.file_no;
      ret = (ret << 32) | r.offset;
      return ret;
    }
  };
} // namespace std

namespace polar_race {
// begin of namespace polar_race

class DataStore  {
 public:
  explicit DataStore(const std::string dir)
    : fd_(-1), dir_(dir), cache_(160*1024*1024) {}

  ~DataStore() {
    if (fd_ > 0) {
      close(fd_);
    }
  }

  RetCode Init();
  RetCode Read(const Location& l, std::string* value);
  RetCode Append(const std::string& value, Location* location);

 private:
  int fd_;
  std::string dir_;
  Location next_location_;

  // 2GB memory.
  // there are 674K*2*28bytes ~= 37,744K ~= 40MB for index cache.
  // Here set 1024MB cache for content.
  // value is 4KB, so the memory should be:
  // For the content cache, take 256K items.
  // each item is nearly 4K
  // So, 256K*2*4K = 256*8MB = 2048MB ~= 2G
  // 128K*2*4K = 1G
  // for some free memory, here set 160K
  // 160K * 8K ~= 1.28G
  LRUCache<Location, std::string> cache_;
  RetCode OpenCurFile();
};

}  // namespace polar_race
