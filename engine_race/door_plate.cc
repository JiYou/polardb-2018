// Copyright [2018] Alibaba Cloud All rights reserved

#include "util.h"
#include "door_plate.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include <stdio.h>

namespace polar_race {

// actually this is for single list.
// there are 2 list in the cache.
static constexpr int32_t kMaxCacheCnt = 4; // 674000;
static const uint64_t kMaxDoorCnt = 1024 * 1024 * 52;
static const char kMetaFileName[] = "META";
static const int64_t kMaxRangeBufCount = kMaxDoorCnt;

// just simply compare the key is same or not.
static bool ItemKeyMatch(const Item &item, const std::string& target) {
  if (target.size() != item.key_size
      || memcmp(item.key, target.data(), item.key_size) != 0) {
    // Conflict
    return false;
  }
  return true;
}

// can use this item place
static bool ItemTryPlace(const Item &item, const std::string& target) {
  if (item.in_use == 0) {
    return true;
  }
  // 如果不空，那么这里比较一下key
  return ItemKeyMatch(item, target);
}

DoorPlate::DoorPlate(const std::string& path)
  : dir_(path),
  fd_(-1),
  cache_(kMaxCacheCnt),
  items_(NULL) {
}

RetCode DoorPlate::Init() {
  std::cout << "DoorPlate::Init()" << std::endl;
  bool new_create = false;
  // 使用了memmap?
  // 32M item * sizeof(Item);
  const uint64_t map_size = kMaxDoorCnt * sizeof(Item);
  DEBUG << "item_size = " << sizeof(Item) << std::endl;
  DEBUG << "map_size = " << map_size << std::endl;

  // 如果目录不存在，创建之
  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    DEBUG << "mkdir " << dir_ << " failed "  << std::endl;
    return kIOError;
  } else {
    printf("DoorPlace::Init() mkdir %s success\n", dir_.c_str());
  }

  // 找到META文件
  std::string path = dir_ + "/" + kMetaFileName;
  int fd = open(path.c_str(), O_RDWR, 0644);
  if (fd < 0 && errno == ENOENT) {
    // not exist, then create
    fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd >= 0) {
      new_create = true;
      // allocate the file size.
      if (posix_fallocate(fd, 0, map_size) != 0) {
        close(fd);
        DEBUG << "posix_fallocate failed: " << strerror(errno) << std::endl;
        return kIOError;
      } else {
        std::cout << "posix_fallocate: success: " << map_size << std::endl;
      }
    }
  }
  if (fd < 0) {
    DEBUG << "create file failed = " << path << std::endl;
    return kIOError;
  }
  fd_ = fd;

  void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    close(fd);
    DEBUG << "MAP_FAILED: " << strerror(errno) << std::endl;
    return kIOError;
  }

  // 需要初始化
  if (new_create) {
    memset(ptr, 0, map_size);
  }
  // 指向了这个磁盘上的大文件
  items_ = reinterpret_cast<Item*>(ptr);
  return kSucc;
}

DoorPlate::~DoorPlate() {
  if (fd_ > 0) {
    const int map_size = kMaxDoorCnt * sizeof(Item);
    munmap(items_, map_size);
    close(fd_);
  }
}

// Very easy hash table, which deal conflict only by try the next one
// 这里只是计算出可用的index的位置
int DoorPlate::CalcIndex(const std::string& key, bool is_write) {
  uint32_t jcnt = 0;
  // 根据字符串取模
  // 得到相应的hash位置
  int index = StrHash(key.data(), key.size()) % kMaxDoorCnt;
  const int origin_index = index;

  // search in the cache.hash
  auto pos = pos_.find(index);
  if (pos != pos_.end()) {
    index = pos->second;
  }

  while (!ItemTryPlace(*(items_ + index), key)
      && ++jcnt < kMaxDoorCnt) {
    index = (index + 1) % kMaxDoorCnt;
  }

  if (jcnt == kMaxDoorCnt) {
    // full
    DEBUG << "Can not find usable Index for item jcnt = " << jcnt << std::endl;
    return -1;
  // if is write, then the index is taken.
  } else if (is_write) {
    pos_[origin_index] = index;
  }
  return index;
}

// 如果key太大,返回失败
RetCode DoorPlate::AddOrUpdate(const std::string& key, const Location& l) {
  if (key.size() > kMaxKeyLen) {
    return kInvalidArgument;
  }

  int index = CalcIndex(key, true /*is_write*/);
  if (index < 0) {
    DEBUG << " space is full " << std::endl;
    return kFull;
  }

  Item* iptr = items_ + index;

  if (iptr->in_use == 0) {
    // new item
    memcpy(iptr->key, key.data(), key.size());
    iptr->key_size = key.size();
    iptr->in_use = 1;  // Place
  }
  iptr->location = l;

  // if use msync, we need to check the page size.
  // the content may across several pages.
  void *origin_addr = reinterpret_cast<void*>(iptr->key);
  uint64_t mod {4095};
  mod = ~mod;
  auto align_addr = reinterpret_cast<void*>(reinterpret_cast<uint64_t>(origin_addr) & (~4095));
  auto left = reinterpret_cast<size_t>(iptr->key) & 4095;
  if (0 != msync(align_addr, left + key.size(), MS_SYNC)) {
    DEBUG << "errno = " << strerror(errno) << std::endl;
    DEBUG << " msync() failed " << std::endl;
    return kIOError;
  }

  // find and updte the cache.
  // put the item into cache.
  cache_.FindThenUpdate(key, l);

  return kSucc;
}

RetCode DoorPlate::Find(const std::string& key, Location *location) {
  // try to find localtion in cache.
  Location pos;
  auto ret = cache_.Get(key, &pos);
  // if cache hit.
  if (ret == kSucc) {
    *location = pos;
    return kSucc;
  }

  int index = CalcIndex(key, false /*just for read*/);
  if (index < 0
      || !ItemKeyMatch(*(items_ + index), key)) {
    DEBUG << " index < 0: " << (index < 0) << " KeyNotMatch() = " << (!ItemKeyMatch(*(items_ + index), key)) << std::endl;
    return kNotFound;
  }

  *location = (items_ + index)->location;
  cache_.Put(key, *location);
  return kSucc;
}

RetCode DoorPlate::GetRangeLocation(const std::string& lower,
    const std::string& upper,
    std::map<std::string, Location> *locations) {
  int count = 0;
  for (Item *it = items_ + kMaxDoorCnt - 1; it >= items_; it--) {
    if (!it->in_use) {
      continue;
    }
    std::string key(it->key, it->key_size);
    if ((key >= lower || lower.empty())
        && (key < upper || upper.empty())) {
      locations->insert(std::pair<std::string, Location>(key, it->location));
      if (++count > kMaxRangeBufCount) {
        DEBUG << " Out of Memory " << std::endl;
        return kOutOfMemory;
      }
    }
  }
  return kSucc;
}

}  // namespace polar_race
