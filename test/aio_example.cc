#include "engine_race/engine_aio.h"
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <iostream>

#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

static const std::string path = "/tmp/testfile"; // "Path to the file to manipulate";
static const int32_t file_size = 1000; // "Length of file in 4k blocks"
static const int32_t concurrent_requests = 100; // "Number of concurrent requests"
static const int32_t min_nr = 1; // "min_nr"
static const int32_t max_nr = 1; // "max_nr"

// The size of operation that will occur on the device
static const int kPageSize = 4096;

#define DEBUG std::cerr<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<"()"<<"msg="<<strerror(errno)

#define CHECK_EQ(a,b) do {            \
  if ((a) != (b)) {                   \
    DEBUG << "FIND NOT EQUAL!\n";     \
  }                                   \
} while (0)

#define LOG(x) std::cout

static inline void io_set_callback(struct iocb *iocb, io_callback_t cb)
{
  iocb->data = (void *)cb;
}

static inline void io_prep_pread(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IO_CMD_PREAD;
  iocb->aio_reqprio = 0;
  iocb->u.c.buf = buf;
  iocb->u.c.nbytes = count;
  iocb->u.c.offset = offset;
}

static inline void io_prep_pwrite(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IO_CMD_PWRITE;
  iocb->aio_reqprio = 0;
  iocb->u.c.buf = buf;
  iocb->u.c.nbytes = count;
  iocb->u.c.offset = offset;
}

class AIORequest {
 public:
  int* buffer_;

  virtual void Complete(int res) = 0;

  AIORequest() {
    int ret = posix_memalign(reinterpret_cast<void**>(&buffer_),
                             kPageSize, kPageSize);
    CHECK_EQ(ret, 0);
  }

  virtual ~AIORequest() {
    free(buffer_);
  }
};

class Adder {
 public:
  virtual void Add(int amount) = 0;

  virtual ~Adder() { };
};

class AIOReadRequest : public AIORequest {
 private:
  Adder* adder_;

 public:
  AIOReadRequest(Adder* adder) : AIORequest(), adder_(adder) { }

  virtual void Complete(int res) {
    CHECK_EQ(res, kPageSize); // << "Read incomplete or error " << res;
    int value = buffer_[0];
    LOG(INFO) << "Read of " << value << " completed";
    adder_->Add(value);
  }
};

class AIOWriteRequest : public AIORequest {
 private:
  int value_;

 public:
  AIOWriteRequest(int value) : AIORequest(), value_(value) {
    buffer_[0] = value;
  }

  virtual void Complete(int res) {
    CHECK_EQ(res, kPageSize); // << "Write incomplete or error " << res;
    LOG(INFO) << "Write of " << value_ << " completed";
  }
};

class AIOAdder : public Adder {
 public:
  int fd_;
  io_context_t ioctx_;
  int counter_;
  int reap_counter_;
  int sum_;
  int length_;

  AIOAdder(int length)
      : ioctx_(0), counter_(0), reap_counter_(0), sum_(0), length_(length) { }

  void Init() {
    LOG(INFO) << "Opening file";
    fd_ = open(path.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0644);
    if (fd_ < 0) {
      DEBUG << "Error opening file" << std::endl;
    }
    LOG(INFO) << "Allocating enough space for the sum" << std::endl;
    if (fallocate(fd_, 0, 0, kPageSize * length_) < 0) {
      DEBUG << "Error in fallocate" << std::endl;
    }
    LOG(INFO) << "Setting up the io context" << std::endl;
    if (io_setup(100, &ioctx_) < 0) {
      DEBUG << "Error in io_setup" << std::endl;
    }
  }

  virtual void Add(int amount) {
    sum_ += amount;
    LOG(INFO) << "Adding " << amount << " for a total of " << sum_ << std::endl;
  }

  void SubmitWrite() {
    LOG(INFO) << "Submitting a write to " << counter_ << std::endl;
    struct iocb iocb;
    struct iocb* iocbs = &iocb;
    AIORequest *req = new AIOWriteRequest(counter_);
    io_prep_pwrite(&iocb, fd_, req->buffer_, kPageSize, counter_ * kPageSize);
    iocb.data = req;
    int res = io_submit(ioctx_, 1, &iocbs);
    CHECK_EQ(res, 1);
  }

  void WriteFile() {
    reap_counter_ = 0;
    for (counter_ = 0; counter_ < length_; counter_++) {
      SubmitWrite();
      Reap();
    }
    ReapRemaining();
  }

  void SubmitRead() {
    LOG(INFO) << "Submitting a read from " << counter_ << std::endl;
    struct iocb iocb;
    struct iocb* iocbs = &iocb;
    AIORequest *req = new AIOReadRequest(this);
    io_prep_pread(&iocb, fd_, req->buffer_, kPageSize, counter_ * kPageSize);
    iocb.data = req;
    int res = io_submit(ioctx_, 1, &iocbs);
    CHECK_EQ(res, 1);
  }

  void ReadFile() {
    reap_counter_ = 0;
    for (counter_ = 0; counter_ < length_; counter_++) {
        SubmitRead();
        Reap();
    }
    ReapRemaining();
  }

  int DoReap(int min_nr) {
    LOG(INFO) << "Reaping between " << min_nr << " and "
              << max_nr << " io_events" << std::endl;
    struct io_event* events = new io_event[max_nr];
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 100000000;
    int num_events;
    LOG(INFO) << "Calling io_getevents" << std::endl;;
    num_events = io_getevents(ioctx_, min_nr, max_nr, events,
                              &timeout);
    LOG(INFO) << "Calling completion function on results" << std::endl;
    for (int i = 0; i < num_events; i++) {
      struct io_event event = events[i];
      AIORequest* req = static_cast<AIORequest*>(event.data);
      req->Complete(event.res);
      delete req;
    }
    delete events;
    LOG(INFO) << "Reaped " << num_events << " io_events" << std::endl;
    reap_counter_ += num_events;
    return num_events;
  }

  void Reap() {
    if (counter_ >= min_nr) {
      DoReap(min_nr);
    }
  }

  void ReapRemaining() {
    while (reap_counter_ < length_) {
      DoReap(1);
    }
  }

  ~AIOAdder() {
    LOG(INFO) << "Closing AIO context and file" << std::endl;
    io_destroy(ioctx_);
    close(fd_);
  }

  int Sum() {
    LOG(INFO) << "Writing consecutive integers to file" << std::endl;
    WriteFile();
    LOG(INFO) << "Reading consecutive integers from file" << std::endl;
    ReadFile();
    return sum_;
  }
};

int main(int argc, char* argv[]) {
  AIOAdder adder(file_size);
  adder.Init();
  int sum = adder.Sum();
  int expected = (file_size * (file_size - 1)) / 2;
  LOG(INFO) << "AIO is complete" << std::endl;
  CHECK_EQ(sum, expected); //  << "Expected " << expected << " Got " << sum;
  printf("Successfully calculated that the sum of integers from 0"
         " to %d is %d\n", file_size - 1, sum);
  return 0;
}
