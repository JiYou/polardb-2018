// Copyright [2018] Alibaba Cloud All rights reserved
#include "util.h"
#include "data_store.h"
#include "libaio.h"

#include <sys/types.h>
#include <unistd.h>

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>


namespace polar_race {

static const std::string kDataDirName = "data";
static const char kDataFilePrefix[] = "DATA_";
static const int kDataFilePrefixLen = 5;
static const int kSingleFileSize = 1024 * 1024 * 100;  // 100MB

// 生成特定的文件名
static std::string FileName(const std::string &dir, uint32_t fileno) {
  return dir + "/" + kDataFilePrefix + std::to_string(fileno);
}

DataStore::DataStore(const std::string path)
    : fd_(-1), dir_(path+ "/" + kDataDirName) {
  if (!FileExists(path)
      && 0 != mkdir(path.c_str(), 0755)) {
    DEBUG << "mkdir " << path<< " failed "  << std::endl;
  }
}

DataStore::~DataStore() {
  if (fd_ > 0) {
    close(fd_);
  }

  for (int i = 0; i < fd_cache_num_; i++) {
    if (fd_cache_[i]) {
      close(fd_cache_[i]);
    }
  }
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
  for (auto &fn: files) {
    if (fn.compare(0, kDataFilePrefixLen, kDataFilePrefix) != 0) {
      continue;
    } else {
      uint32_t no = std::atoi(fn.c_str() + kDataFilePrefixLen);
      if (no > last_no) {
        last_no = no;
      }
    }
  }
  // alloc the memory
  fd_cache_ = (int *) malloc(sizeof(int) * (last_no + 1));
  if (!fd_cache_) {
    DEBUG << "fd_cache_ is empty nullptr" << std::endl;
  }
  memset(fd_cache_, 0, sizeof(int) * (last_no + 1));
  fd_cache_num_ = last_no + 1;

  // open all the files into hash
  for (auto &fn: files) {
    if (fn.compare(0, kDataFilePrefixLen, kDataFilePrefix) != 0) {
      continue;
    } else {
      int no = std::atoi(fn.c_str() + kDataFilePrefixLen);
      auto path = FileName(dir_, no);
      fd_cache_[no] = open(path.c_str(), O_RDONLY | O_DIRECT, 0644);
      if (fd_cache_[no] < 0) {
        DEBUG << "open file failed" << std::endl;
        fd_cache_[no] = 0;
      }
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

RetCode DataStore::Sync() {
    if (fd_ > 0) {
      if (fsync(fd_) < 0) {
        DEBUG << " fsync failed" << std::endl;
        return kIOError;
      }
      return kSucc;
    }
    return kIOError;
}

// because shere just use append, so no need to update the LRUCache.
RetCode DataStore::Append(const std::string& value, Location* location) {
  if (value.size() > kSingleFileSize) {
    DEBUG << " invalid argument size" << value.size() << std::endl;
    return kInvalidArgument;
  }

  if (next_location_.offset + value.size() > kSingleFileSize) {
    if (fsync(fd_) < 0) {
      DEBUG << " fsync failed" << std::endl;
      return kIOError;
    }
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
  location->file_no = next_location_.file_no;
  location->offset = next_location_.offset;
  location->len = value.size();

  next_location_.offset += location->len;
  return kSucc;
}

struct aio_env {
  aio_env() {
    // !!! must align, do not use value->data() directly.
    if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
      DEBUG << "posix_memalign failed!\n";
    }

    // prepare the io context.
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(1, &ctx) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }

    timeout.tv_sec = 0;
    timeout.tv_nsec = 0;

    iocbs = &iocb;

    memset(&iocb, 0, sizeof(iocb));
    // iocb->aio_fildes = fd;
    iocb.aio_lio_opcode = IO_CMD_PREAD;
    iocb.aio_reqprio = 0;
    // iocb->u.c.buf = buf;
    iocb.u.c.nbytes = kPageSize;
    // iocb->u.c.offset = offset;
  }

  ~aio_env() {
    io_destroy(ctx);
    free(buf);
  }

  io_context_t ctx;
  char *buf = nullptr;
  struct iocb iocb;
  struct iocb* iocbs;
  struct io_event events;
  struct timespec timeout;
};


RetCode DataStore::BatchRead(std::vector<read_item*> &to_read, std::vector<Location> &file_pos) {
  // set io_ctx
  return kSucc;
}


RetCode DataStore::Read(const Location& l, std::string* value) {
  static thread_local aio_env ae;
  static thread_local Location pre_pos;
  static thread_local bool has_read_ = false;

  if (has_read_ && pre_pos == l) {
    // hit previous read content.
    // manual memcpy
    value->clear();
    value->resize(kPageSize);
    uint64_t *to = reinterpret_cast<uint64_t*>(const_cast<char*>(value->data()));
    uint64_t *from = reinterpret_cast<uint64_t*>(ae.buf);
    for (int i = 0; i < 512; i++) {
      *to++ = *from++;
    }
    return kSucc;
  }

  // if not hit previous cache.
  // find or open the related fd.
  int fd = -1;
  if (l.file_no >= fd_cache_num_ || !fd_cache_[l.file_no]) {
    auto ret = find_shared_cache(l.file_no, &fd);
    if (ret == kNotFound) {
      fd = open(FileName(dir_, l.file_no).c_str(), O_RDONLY | O_DIRECT, 0644);
      if (fd < 0) {
        DEBUG << " open " << FileName(dir_, l.file_no).c_str() << " failed" << std::endl;
        return kIOError;
      }
    }
  } else {
    fd = fd_cache_[l.file_no];
  }

  // prepare the io
  ae.iocb.aio_fildes = fd;
  ae.iocb.u.c.buf = ae.buf;
  ae.iocb.u.c.offset = l.offset;

  if ((io_submit(ae.ctx, kSingleRequest, &(ae.iocbs))) != kSingleRequest) {
    DEBUG << "io_submit meet error, " << std::endl;;
    printf("io_submit error\n");
    return kIOError;
  }

  // Task begin=========
  value->clear();
  value->resize(l.len);
  pre_pos = l;
  has_read_ = true;
  // manual memcpy
  uint64_t *to = reinterpret_cast<uint64_t*>(const_cast<char*>(value->data()));
  uint64_t *from = reinterpret_cast<uint64_t*>(ae.buf);
  update_shared_cache(l.file_no, fd);
  // Task end===========

  // after submit, need to wait all read over.
  while (io_getevents(ae.ctx, kSingleRequest, kSingleRequest,
                      &(ae.events), &(ae.timeout)) != kSingleRequest) {
    /**/
  }

  for (int i = 0; i < 512; i++) {
    *to++ = *from++;
  }

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
