#pragma once


#include "include/engine.h"
#include "engine_race/engine_cache.h"
#include "engine_race/spin_lock.h"
#include <string.h>
#include <unistd.h>

#include <string>

namespace polar_race {

struct Location {
  Location() : offset(0), file_no(0), len(0) {
  }
  uint32_t offset;
  uint16_t file_no;
  uint16_t len;
};

inline bool operator==(const Location &l, const Location& r) {
  return l.file_no == r.file_no && l.offset == r.offset;
}

} // end of namespace polar_race


// this is for content cache_.
// but it may cost a lot of memory.
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
  explicit DataStore(const std::string dir);

  ~DataStore();

  RetCode Init();
  RetCode BatchRead(std::vector<read_item*> &to_read, std::vector<Location> &l);
  RetCode Read(const Location& l, std::string* value);
  RetCode Append(const std::string& value, Location* location);
  RetCode Sync();

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
  // LRUCache<Location, std::string> cache_;

  int fd_cache_num_ = 0;
  int *fd_cache_ = nullptr;

  RetCode OpenCurFile();
};

}  // namespace polar_race
