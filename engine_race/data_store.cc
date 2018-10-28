// Copyright [2018] Alibaba Cloud All rights reserved
#include "util.h"
#include "data_store.h"

#include <stdio.h>

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>


namespace polar_race {

//static constexpr int kMaxDataCacheCnt = 160 * 1024 * 1024; // this is for single list.
static const char kDataFilePrefix[] = "DATA_";
static const int kDataFilePrefixLen = 5;
static const int kSingleFileSize = 1024 * 1024 * 100;  // 100MB

// 生成特定的文件名
static std::string FileName(const std::string &dir, uint32_t fileno) {
  return dir + "/" + kDataFilePrefix + std::to_string(fileno);
}

RetCode DataStore::Init() {
  // 如果目录不存在，创建之
  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    DEBUG << dir_ << "not exit, but mkdir failed" << std::endl;
    return kIOError;
  }

  // 拿到所有的文件
  std::vector<std::string> files;
  if (0 != GetDirFiles(dir_, &files)) {
    DEBUG << "call GetDirFiles() failed: " << dir_ << std::endl;
    return kIOError;
  }

  uint32_t last_no = 0;
  uint32_t cur_offset = 0;

  // Get the last data file no
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = files.begin(); it != files.end(); ++it) {
    if ((*it).compare(0, kDataFilePrefixLen, kDataFilePrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kDataFilePrefixLen);
    if (std::stoul(sindex) > last_no) {
      last_no = std::stoi(sindex);
    }
  }

  // Get last data file offset
  int len = GetFileLength(FileName(dir_, last_no));
  if (len > 0) {
    cur_offset = len;
  }

  next_location_.file_no = last_no;
  next_location_.offset = cur_offset;

  // Open file
  return OpenCurFile();
}

// because shere just use append, so no need to update the LRUCache.
RetCode DataStore::Append(const std::string& value, Location* location) {
  if (value.size() > kSingleFileSize) {
    DEBUG << " invalid argument size" << value.size() << std::endl;
    return kInvalidArgument;
  }

  if (next_location_.offset + value.size() > kSingleFileSize) {
    close(fd_);
    next_location_.file_no += 1;
    next_location_.offset = 0;
    OpenCurFile();
  }

  // Append write
  if (0 != FileAppend(fd_, value)) {
    DEBUG << " FileAppend()  failed" << std::endl;
    return kIOError;
  }
  if (0 != fsync(fd_)) {
    DEBUG << " fsync() failed" << std::endl;
    return kIOError;
  }
  location->file_no = next_location_.file_no;
  location->offset = next_location_.offset;
  location->len = value.size();

  next_location_.offset += location->len;
  return kSucc;
}

RetCode DataStore::Read(const Location& l, std::string* value) {
  // try to find the location in cache.
  auto is_find = cache_.Get(l, value);
  if (is_find == kSucc) {
    return kSucc;
  }

  // 这里直接定位到相应的文件，然后打开之.
  int fd = open(FileName(dir_, l.file_no).c_str(), O_RDONLY, 0644);
  if (fd < 0) {
    DEBUG << " open " << FileName(dir_, l.file_no).c_str() << " failed" << std::endl;
    return kIOError;
  }
  lseek(fd, l.offset, SEEK_SET);

  char* buf = new char[l.len]();
  char* pos = buf;
  uint32_t value_len = l.len;

  while (value_len > 0) {
    ssize_t r = read(fd, pos, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      close(fd);
      DEBUG << " read(" << pos << ":" << value_len << "failed" << std::endl;
      return kIOError;
    }
    pos += r;
    value_len -= r;
  }
  *value = std::string(buf, l.len);

  // if not find,  then put it into cache.
  cache_.Put(l, *value);

  delete [] buf;
  close(fd);
  return kSucc;
}

RetCode DataStore::OpenCurFile() {
  std::string file_name = FileName(dir_, next_location_.file_no);
  int fd = open(file_name.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    DEBUG << " open " << file_name << " failed" << std::endl;
    return kIOError;
  }
  fd_ = fd;
  return kSucc;
}

}  // namespace polar_race
