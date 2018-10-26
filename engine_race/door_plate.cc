// Copyright [2018] Alibaba Cloud All rights reserved

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include "util.h"
#include "door_plate.h"

#include <stdio.h>

namespace polar_race {

// 128M item
static const uint32_t kMaxDoorCnt = 1024 * 1024 * 32;
static const char kMetaFileName[] = "META";
static const int kMaxRangeBufCount = kMaxDoorCnt;


// get memory page size.
static long page_size() {
  static long page_size = sysconf(_SC_PAGESIZE);
  return page_size;
}

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
  items_(NULL) {
}

RetCode DoorPlate::Init() {
  std::cout << "DoorPlate::Init()" << std::endl;
  bool new_create = false;
  // 使用了memmap?
  // 32M item * sizeof(Item);
  const int map_size = kMaxDoorCnt * sizeof(Item);

  // 如果目录不存在，创建之
  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    printf("DoorPlate::Init(): mkdir  %s failed\n", dir_.c_str());
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
        std::cerr << "posix_fallocate failed: " << strerror(errno) << std::endl;
        close(fd);
        return kIOError;
      } else {
        std::cout << "posix_fallocate: success: " << map_size << std::endl;
      }
    }
  }
  if (fd < 0) {
    return kIOError;
  }
  fd_ = fd;

  void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    std::cerr << "MAP_FAILED: " << strerror(errno) << std::endl;
    close(fd);
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
int DoorPlate::CalcIndex(const std::string& key) {
  uint32_t jcnt = 0;
  // 根据字符串取模
  // 得到相应的hash位置
  int index = StrHash(key.data(), key.size()) % kMaxDoorCnt;
  while (!ItemTryPlace(*(items_ + index), key)
      && ++jcnt < kMaxDoorCnt) {
    index = (index + 1) % kMaxDoorCnt;
  }

  // 如果已经到找到了最大位置处，失败
  if (jcnt == kMaxDoorCnt) {
    // full
    return -1;
  }
  return index;
}

// 如果key太大,返回失败
RetCode DoorPlate::AddOrUpdate(const std::string& key, const Location& l) {
  if (key.size() > kMaxKeyLen) {
    return kInvalidArgument;
  }

  int index = CalcIndex(key);
  if (index < 0) {
    return kFull;
  }

  Item* iptr = items_ + index;

  if (iptr->in_use == 0) {
    // new item
    memcpy(iptr->key, key.data(), key.size());
    if (0 != msync(iptr->key, key.size(), MS_SYNC)) {
      return kIOError;
    }
    iptr->key_size = key.size();
    iptr->in_use = 1;  // Place
  }
  iptr->location = l;
  return kSucc;
}

RetCode DoorPlate::Find(const std::string& key, Location *location) {
  int index = CalcIndex(key);
  if (index < 0
      || !ItemKeyMatch(*(items_ + index), key)) {
    return kNotFound;
  }

  *location = (items_ + index)->location;
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
        return kOutOfMemory;
      }
    }
  }
  return kSucc;
}

}  // namespace polar_race
