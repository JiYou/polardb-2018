// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_EXAMPLE_DOOR_PLATE_H_
#define ENGINE_EXAMPLE_DOOR_PLATE_H_

#include "include/engine.h"

#include "engine_cache.h"
#include "data_store.h"

#include <fcntl.h>
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>
#include <unordered_map>

namespace polar_race {

static const uint32_t kMaxKeyLen = 8;

struct Item {
  Item() : key_size(0), in_use(0) {
  }
  Location location;      // 位置
  char key[kMaxKeyLen];   // key
  uint32_t key_size;      // key_size
  uint8_t in_use;         // 这个item是否被使用
};

// Hash index for key
// 这里就是利用开地址法，在磁盘上一个大文件里面实现了一个巨大的hash
// 任何的读写操作都是在这个基于文件的hash上完成
class DoorPlate  {
 public:
    explicit DoorPlate(const std::string& path);
    ~DoorPlate();

    RetCode Init();

    RetCode AddOrUpdate(const std::string& key, const Location& l);

    RetCode Find(const std::string& key, Location *location);

    RetCode GetRangeLocation(const std::string& lower, const std::string& upper,
        std::map<std::string, Location> *locations);

 private:
    std::string dir_;
    int fd_;
    LRUCache<std::string, Location> cache_;
    Item *items_;
    // cache for position.
    std::unordered_map<int, int> pos_;
    // LRUCache for item.
    // record the content of key->item.
    // decide to use enough items.
    // each item is 28bytes.
    // 28 * 1024 * 1024 ~= 28MB
    // for this kind of cache, may use 512MB.
    // for eache level, the capacity is 18MB
    // So, the cache size = 18MB / 28 = 674K

    int CalcIndex(const std::string& key, bool is_write);
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_DOOR_PLATE_H_
