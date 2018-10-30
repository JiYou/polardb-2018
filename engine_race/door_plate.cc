// Copyright [2018] Alibaba Cloud All rights reserved

#include "util.h"
#include "door_plate.h"

#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include <algorithm>

namespace polar_race {

static const std::string kMetaDirName = "index";
static const char kMetaFileNamePrefix[] = "META_";
static constexpr int kMetaFileNamePrefixLen = 5;
static const int kSingleFileSize = 1024 * 1024 * 100;  // 100MB

// 生成特定的文件名
static std::string FileName(const std::string &dir, uint32_t fileno) {
  return dir + "/" + kMetaFileNamePrefix + std::to_string(fileno);
}

DoorPlate::DoorPlate(const std::string& path)
  : dir_(path + "/" + kMetaDirName),
  fd_(-1) {
  if (!FileExists(path)
      && 0 != mkdir(path.c_str(), 0755)) {
    DEBUG << "mkdir " << path<< " failed "  << std::endl;
  }
}

RetCode DoorPlate::Init() {
  std::cout << "DoorPlate::Init()" << std::endl;

  // create the dir.
  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    DEBUG << "mkdir " << dir_ << " failed "  << std::endl;
    return kIOError;
  } else {
    printf("DoorPlace::Init() mkdir %s success\n", dir_.c_str());
  }

  // get all the files.
  std::vector<std::string> files;
  if (0 != GetDirFiles(dir_, &files)) {
    DEBUG << "call GetDirFiles() failed: " << dir_ << std::endl;
    return kIOError;
  }

  // sort the meta files.
  std::sort(files.begin(), files.end(),
    [](const std::string &a, const std::string &b) {
      const int va = atoi(a.c_str() + kMetaFileNamePrefixLen);
      const int vb = atoi(b.c_str() + kMetaFileNamePrefixLen);
      return va < vb;
    }
  );

  // rebuild the index.
  for (auto fn: files) {
    // open file to read.
    std::string file_name = dir_ + "/" + fn;
    int fd = open(file_name.c_str(), O_RDONLY, 0644);
    if (fd < 0) {
      DEBUG << "open " << file_name << " failed" << std::endl;
      return kIOError;
    }

    // at the begining of file.
    Item item;
    while (read(fd, &item, sizeof(item)) == sizeof(item)) {
      if (item.in_use) {
        std::string key(item.key, item.key_size);
        UpdateHashMap(key, item.location);
      }
    }
    close(fd);
  }

  // get the last file no, and offset;
  if (!files.empty()) {
    std::string file_name = files.back();
    last_no_ = std::atoi(file_name.c_str() + kMetaFileNamePrefixLen);
    int len = GetFileLength(FileName(dir_, last_no_));
    if (len > 0) {
      offset_ = len;
    }
  }

  // Open file
  return OpenCurFile();
}

DoorPlate::~DoorPlate() {
  if (fd_ > 0) {
    close(fd_);
  }
}

RetCode DoorPlate::Sync() {
    if (fd_ > 0) {
      if (fsync(fd_) < 0) {
        DEBUG << " fsync failed" << std::endl;
        return kIOError;
      }
      return kSucc;
    }
    return kIOError;
}

RetCode DoorPlate::Append(const std::string& key, const Location& l) {
  if (offset_ + sizeof(Item) > kSingleFileSize) {
    if (fsync(fd_) < 0) {
      DEBUG << " fsync failed" << std::endl;
      return kIOError;
    }
    close(fd_);

    last_no_ += 1;
    offset_ = 0;

    auto ret = OpenCurFile();
    if (ret != kSucc) {
      DEBUG << " call OpenCurFile() failed" << std::endl;
      return ret;
    }
  }


  if (key.size() > kMaxKeyLen) {
    DEBUG << " key length larger than kMaxKeyLen" << std::endl;
    return kIOError;
  }

  // create Item
  Item item;
  item.location = l;
  if (key.size() == kMaxKeyLen) {
    const uint64_t *a = reinterpret_cast<const uint64_t*>(key.c_str());
    uint64_t *b = reinterpret_cast<uint64_t*>(item.key);
    *b = *a;
  } else {
    for (size_t i = 0; i < key.size(); i++) {
      item.key[i] = key[i];
    }
  }
  item.key_size = static_cast<uint8_t>(key.size());
  item.in_use = 1;

  // insert into hash_map
  UpdateHashMap(key, l);

  if (0 != FileAppend(fd_, &item, sizeof(item))) {
    DEBUG << " FileAppend()  failed" << std::endl;
    return kIOError;
  }

  offset_ += sizeof(Item);
  return kSucc;
}

RetCode DoorPlate::Find(const std::string& key, Location *location) {
  auto pos = hash_map_.find(key);
  if (pos == hash_map_.end()) {
    return kNotFound;
  }
  *location = pos->second;
  return kSucc;
}

RetCode DoorPlate::GetRangeLocation(const std::string& lower,
    const std::string& upper,
    std::map<std::string, Location> *locations) {
  return kSucc;
}

RetCode DoorPlate::OpenCurFile() {
  std::string file_name = FileName(dir_, last_no_);
  int fd = open(file_name.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    DEBUG << " open " << file_name << " failed" << std::endl;
    return kIOError;
  }
  fd_ = fd;
  return kSucc;
}

void DoorPlate::UpdateHashMap(const std::string &key, const Location &l) {
  auto ret = hash_map_.emplace(std::piecewise_construct,
                               std::forward_as_tuple(key),
                               std::forward_as_tuple(l));
  if (!ret.second) {
    ret.first->second = l;
  }
}

}  // namespace polar_race
