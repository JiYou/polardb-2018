// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_EXAMPLE_DOOR_PLATE_H_
#define ENGINE_EXAMPLE_DOOR_PLATE_H_

#include "engine_race/spin_lock.h"
#include "engine_race/engine_cache.h"
#include "engine_race/data_store.h"

#include <fcntl.h>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <string>
#include <unordered_map>

namespace polar_race {

static constexpr uint32_t kMaxKeyLen = 8;
static constexpr uint32_t kMaxValueLen = 4096;

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

class IndexHash {
 public:
  RetCode Get(const std::string &key, Location *l);
  RetCode Set(const std::string &key, const Location &l);
  IndexHash() { }
  ~IndexHash() { }

 public:
  IndexHash(const IndexHash &) = delete;
  IndexHash(const IndexHash &&) = delete;
  IndexHash &operator=(const IndexHash&) = delete;
  IndexHash &operator=(const IndexHash&&) = delete;
 private:
  std::unordered_map<int64_t, uint32_t> hash_;
};


static constexpr size_t kMaxBucketSize = 17 * 19 * 23 * 29 * 31 + 1;

class HashTreeTable {
 public:
  RetCode Get(const std::string &key, Location *l);
  RetCode Set(const std::string &key, const Location &l);

 public:
  HashTreeTable(): hash_lock_(kMaxBucketSize) {
    hash_.resize(kMaxBucketSize);
  }
  ~HashTreeTable() { }

 private:
  void LockHashShard(uint32_t index);
  void UnlockHashShard(uint32_t index);

 public:
  HashTreeTable(const HashTreeTable &) = delete;
  HashTreeTable(const HashTreeTable &&) = delete;
  HashTreeTable &operator=(const HashTreeTable&) = delete;
  HashTreeTable &operator=(const HashTreeTable&&) = delete;
 private:
  // uint64 & uint32_t would taken 16 bytes, if not align with bytes.
  #pragma pack(push, 1)
  struct kv_info {
    uint64_t key;
    uint32_t pos;
    kv_info(uint64_t k, uint32_t v): key(k), pos(v) { }
    kv_info(): key(0), pos(0) { }
  };
  #pragma pack(pop)
  std::vector<std::vector<kv_info>> hash_;
  std::vector<spinlock> hash_lock_;
 private:
  uint32_t compute_pos(uint64_t key);
  RetCode find(std::vector<kv_info> &vs, uint64_t key, kv_info **ptr);
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
    // IndexHash compress_hash_map_;
    HashTreeTable hash_table_;
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_DOOR_PLATE_H_
