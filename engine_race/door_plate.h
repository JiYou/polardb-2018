// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_EXAMPLE_DOOR_PLATE_H_
#define ENGINE_EXAMPLE_DOOR_PLATE_H_

#include "engine_cache.h"
#include "data_store.h"


#include <fcntl.h>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <string>
#include <unordered_map>

namespace polar_race {

static const uint32_t kMaxKeyLen = 8;

// Item to write on the META file.
// for the memory hash map.
// Just need to record
// key->location mapping.
struct Item {
  Item() : key_size(0), in_use(0) {
  }
  // 位置 int32_t offset,  uint16_t file_no  uint16_t value_len
  Location location;
  char key[kMaxKeyLen];   // key
  uint8_t key_size;      // key_size
  uint8_t in_use;        // 这个item是否被使用
};

// Hash index for key
// 这里就是利用开地址法，在磁盘上一个大文件里面实现了一个巨大的hash
// 任何的读写操作都是在这个基于文件的hash上完成
class DoorPlate  {
 public:
    explicit DoorPlate(const std::string& path);
    ~DoorPlate();

    RetCode Init();

    RetCode Append(const std::string& key, const Location& l);
    RetCode Sync();

    RetCode Find(const std::string& key, Location *location);

    RetCode GetRangeLocation(const std::string& lower,
                             const std::string& upper,
                             std::map<std::string, Location> *locations);
 private:
   RetCode OpenCurFile();
  void UpdateHashMap(const std::string &key, const Location &l);
 private:
    std::string dir_;
    int fd_ = -1;
    uint32_t last_no_ = 0;
    uint32_t offset_ = 0;
    std::unordered_map<std::string, Location> hash_map_;
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_DOOR_PLATE_H_
