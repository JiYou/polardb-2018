#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <libaio.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

DEFINE_string(path, "/tmp/testfile", "Path to the file to manipulate");
DEFINE_int32(file_size, 1000, "Length of file in 4k blocks");
DEFINE_int32(concurrent_requests, 100, "Number of concurrent requests");
DEFINE_int32(min_nr, 1, "min_nr");
DEFINE_int32(max_nr, 1, "max_nr");

// The size of operation that will occur on the device
static const int kPageSize = 4096;

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
    CHECK_EQ(res, kPageSize) << "Read incomplete or error " << res;
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
    CHECK_EQ(res, kPageSize) << "Write incomplete or error " << res;
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
    fd_ = open(FLAGS_path.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0644);
    PCHECK(fd_ >= 0) << "Error opening file";
    LOG(INFO) << "Allocating enough space for the sum";
    PCHECK(fallocate(fd_, 0, 0, kPageSize * length_) >= 0) << "Error in fallocate";
    LOG(INFO) << "Setting up the io context";
    PCHECK(io_setup(100, &ioctx_) >= 0) << "Error in io_setup";
  }

  virtual void Add(int amount) {
    sum_ += amount;
    LOG(INFO) << "Adding " << amount << " for a total of " << sum_;
  }

  void SubmitWrite() {
    LOG(INFO) << "Submitting a write to " << counter_;
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
    LOG(INFO) << "Submitting a read from " << counter_;
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
              << FLAGS_max_nr << " io_events";
    struct io_event* events = new io_event[FLAGS_max_nr];
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 100000000;
    int num_events;
    LOG(INFO) << "Calling io_getevents";
    num_events = io_getevents(ioctx_, min_nr, FLAGS_max_nr, events,
                              &timeout);
    LOG(INFO) << "Calling completion function on results";
    for (int i = 0; i < num_events; i++) {
      struct io_event event = events[i];
      AIORequest* req = static_cast<AIORequest*>(event.data);
      req->Complete(event.res);
      delete req;
    }
    delete events;
    LOG(INFO) << "Reaped " << num_events << " io_events";
    reap_counter_ += num_events;
    return num_events;
  }

  void Reap() {
    if (counter_ >= FLAGS_min_nr) {
      DoReap(FLAGS_min_nr);
    }
  }

  void ReapRemaining() {
    while (reap_counter_ < length_) {
      DoReap(1);
    }
  }

  ~AIOAdder() {
    LOG(INFO) << "Closing AIO context and file";
    io_destroy(ioctx_);
    close(fd_);
  }

  int Sum() {
    LOG(INFO) << "Writing consecutive integers to file";
    WriteFile();
    LOG(INFO) << "Reading consecutive integers from file";
    ReadFile();
    return sum_;
  }
};

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  AIOAdder adder(FLAGS_file_size);
  adder.Init();
  int sum = adder.Sum();
  int expected = (FLAGS_file_size * (FLAGS_file_size - 1)) / 2;
  LOG(INFO) << "AIO is complete";
  CHECK_EQ(sum, expected) << "Expected " << expected << " Got " << sum;
  printf("Successfully calculated that the sum of integers from 0"
         " to %d is %d\n", FLAGS_file_size - 1, sum);
  return 0;
}
