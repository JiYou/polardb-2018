// Copyright [2018] Alibaba Cloud All rights reserved

#include "util.h"
#include "engine_race.h"
#include "libaio.h"

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <numeric>
#include <chrono>
#include <atomic>
#include <algorithm>

namespace polar_race {

static const char kLockFile[] = "LOCK";
// all data locate in this file.
static const char kBigFileName[] = "DB";
// the whole index maxiumly take 1G space.

//---------------------------------------------------------
// HashTreeTable
//---------------------------------------------------------

uint32_t HashTreeTable::compute_pos(uint64_t x) {
  // hash tree algorithm
  return (((((x % 17) * 19 + x % 19) * 23 + x % 23) * 29 + x % 29) * 31 + x % 31);
}

void HashTreeTable::LockHashShard(uint32_t index) {
  spin_lock(hash_lock_[index]);
}

void HashTreeTable::UnlockHashShard(uint32_t index) {
  spin_unlock(hash_lock_[index]);
}

RetCode HashTreeTable::find(std::vector<kv_info> &vs, 
                            bool sorted,
                            uint64_t key,
                            kv_info **ptr) {
  // to check is sorted?
  if (sorted) {
    // if sort, then use binary_search;
    auto pos = std::lower_bound(vs.begin(),
      vs.end(), key, [](const kv_info &a, uint64_t b) {
      return a < b;
    });
    // has find.
    if (pos != vs.end() && !(key < pos->key)) {
      *ptr = &(*pos);
      return kSucc;
    }
  } else {
    // if not sort, find one by one.
    for (auto &x: vs) {
      if (x.key == key) {
        *ptr = &x;
        return kSucc;
      }
    }
  }
  return kNotFound;
}

// l is the real offset.
RetCode HashTreeTable::Get(const char* key, uint64_t *l) {
  const uint64_t *k = reinterpret_cast<const uint64_t*>(key);
  const uint64_t array_pos = compute_pos(*k);

  // then begin to search this array.
  LockHashShard(array_pos);
  auto &vs = hash_[array_pos];
  uint32_t pos = 0;
  kv_info *ptr = nullptr;
  auto ret = find(vs, has_sort_.test(array_pos), *k, &ptr);

  if (ret == kNotFound) {
    UnlockHashShard(array_pos);
    return kNotFound;
  }
  pos = ptr->offset_4k_;
  UnlockHashShard(array_pos);
  
  // just get the offset in the big file.
  *l = pos;
  *l <<= kValueLengthBits;
  return kSucc;
}

RetCode HashTreeTable::Get(const std::string &key, uint64_t *l) {
  return Get(key.c_str(), l);
}

RetCode HashTreeTable::Set(const char *key, uint64_t l) {
  const int64_t *k = reinterpret_cast<const int64_t*>(key);
  const uint64_t array_pos = compute_pos(*k);
  l >>= kValueLengthBits;
  uint32_t pos = l;

  LockHashShard(array_pos);
  auto &vs = hash_[array_pos];
  kv_info *ptr;
  auto ret = find(vs, has_sort_.test(array_pos), *k, &ptr);

  if (ret == kNotFound) {
    vs.emplace_back(*k, pos);
    // broken the sorted list.
    has_sort_.reset(array_pos);
  } else {
    ptr->offset_4k_ = pos;
  }
  UnlockHashShard(array_pos);
  return kSucc;
}

RetCode HashTreeTable::Set(const std::string &key, uint64_t l) {
  return Set(key.c_str(), l);
}

void HashTreeTable::Sort() {
  // there are 64 thread.
  // split all the range to 64 threads.
  // every thread would contains 104354.
  auto sort_range = [this](const size_t begin, const size_t end) {
    for (size_t i = begin; i < end && i < kMaxBucketSize; i++) {
      auto &vs = hash_[i];
      std::sort(vs.begin(), vs.end());
    }
  };

  auto set_all_sorted = [this]() {
      has_sort_.set();
      if (!has_sort_.test(0)) {
        DEBUG << "ERROR: sorted set error." << std::endl;
      }
  };
  std::thread set_sort_bit(set_all_sorted);

  std::vector<std::thread> thread_list;
  constexpr int segment_size = 104355;
  for (int i = 0; i < kMaxThreadNumber; i++) {
    const size_t begin = i * segment_size;
    const size_t end = (i + 1) * segment_size;
    thread_list.emplace_back(std::thread(sort_range, begin, end));
  }

  for (auto &x: thread_list) {
    x.join();
  }
  set_sort_bit.join();
}

void HashTreeTable::PrintMeanStdDev() {
  std::vector<size_t> vs;
  for (auto &shard: hash_) {
    vs.push_back(shard.size());
  }
  double mean = 0, stdev = 0;
  ComputeMeanSteDev(vs, &mean, &stdev);
  std::cout << "HashHard Stat: mean = " << mean
            << " , " << "stdev = " << stdev << std::endl;
}

//--------------------------------------------------------
// Engine
//--------------------------------------------------------

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}


Engine::~Engine() {
}

//--------------------------------------------------------
// EngineRace
//--------------------------------------------------------


RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);
  std::atomic<bool> meet_error{false};
  std::string file_name_str = name + "/" + kBigFileName;
  const char *file_name = file_name_str.c_str();

  // use truncate to create the big file.
  auto create_big_file = [&engine_race, &meet_error, &name, &file_name]() {
    int fd = open(file_name, O_CREAT | O_RDWR, S_IWRITE | S_IREAD);
    // this would not change the file content.
    if (ftruncate(fd, kBigFileSize) < 0) {
      DEBUG << "create big file failed\n";
      delete engine_race;
      meet_error = true;
      return;
    }
    close(fd);
    // then try go open the file with O_DIRECT fd.
    // O_RDWR | O_DIRECT
    engine_race->fd_ = open(file_name, O_RDWR | O_DIRECT, 0644);
    if (engine_race->fd_ < 0) {
      DEBUG << "open big file failed\n";
      meet_error = true;
      return;
    }
    // init the aio env
    engine_race->write_aio_.SetFD(engine_race->fd_);
    engine_race->read_aio_.SetFD(engine_race->fd_);
  };
  std::thread thd_create_big_file(create_big_file);

  auto creat_lock_file = [&engine_race, &meet_error, &name, &file_name]() {
    if (0 != LockFile(name + "/" + kLockFile, &(engine_race->db_lock_))) {
      meet_error = true;
      DEBUG << "Generate LOCK file failed" << std::endl;
    }
  };
  std::thread thd_creat_lock_file(creat_lock_file);

  // setup write buffer.
  // 4KB for index
  // 256KB for write data
  // 4MB for read data.
  auto alloc_buf = [&engine_race, &meet_error] () {
    engine_race->index_buf_ = GetAlignedBuffer(kPageSize);
    if (!(engine_race->index_buf_)) {
      return;
    }
    engine_race->write_data_buf_ = GetAlignedBuffer(kPageSize * kMaxThreadNumber);
    if (!(engine_race->write_data_buf_)) {
      return;
    }
    engine_race->read_data_buf_ = GetAlignedBuffer(kPageSize * kReadValueCnt);
    if (!(engine_race->read_data_buf_)) {
      return;
    }
  };
  std::thread thd_alloc_buf(alloc_buf);

  // next thread to create the big file.
  thd_create_big_file.join();
  thd_creat_lock_file.join();
  thd_alloc_buf.join();
  if (meet_error) {
    delete engine_race;
    return kIOError;
  }

  // after the big file is create, then begin to bulid the
  // HashTable.
  // the fd_ is opened.
  // after build Hash table, also record the next to write pos.
  engine_race->BuildHashTable();
  engine_race->hash_.Sort();
  engine_race->hash_.PrintMeanStdDev();

  engine_race->start();
  *eptr = engine_race;

  return kSucc;
}

void EngineRace::BuildHashTable() {
  // the file fd has been open.

  // read disk thread is producer.
  // producer just need to put the char* buf
  // into Q.
  // actually it contains char*
  // build hash index is the consumer.
  Queue<char> q(4);
  std::atomic<bool> read_over_{false};

  // to read disk.
  auto read_disk = [this, &q, &read_over_]() {
    // start from the begin of file.
    uint64_t offset = 0;
    char *buf = nullptr;
    bool is_valid = true;
    // the index part is maxiumly 1GB.
    for (uint32_t i = 0; i < kMaxIndexSize / k4MB && is_valid; i++) {
      buf = GetAlignedBuffer(k4MB);
      read_aio_.PrepareRead(offset, buf, k4MB);
      read_aio_.Submit();
      read_aio_.WaitOver();
      q.Push(buf);
      is_valid = buf[kLastCharIn4MB];
      offset += k4MB;
    }
    read_over_ = true;
  };
  std::thread thd_read_disk(read_disk);

  auto build_index = [this, &q, &read_over_] () {
    std::vector<char*> buf_list;
    for  (uint32_t i = 0; i < kMaxIndexSize / k4MB && !read_over_; i++) {
      q.Pop(&buf_list, false/*read_disk*/);
      for (auto &buf: buf_list) {
        struct disk_index *ar = reinterpret_cast<struct disk_index*>(buf);
        for (uint32_t j = 0; j < k4MB / sizeof(struct disk_index); j++) {
          struct disk_index *ref = ar + j;
          if (ref->valid) {
            hash_.Set(ref->key, ref->offset);
            uint64_t pos = ref->offset;
            pos <<= 12;
            // where the data to write begin.
            max_data_offset_ = std::max(max_data_offset_, pos);
          }
        }
        free(buf);
      }
    }
  };
  std::thread thd_build_index(build_index);

  thd_read_disk.join();
  thd_build_index.join();

  // free the left bufer
  std::vector<char*> buf_list;
  q.Pop(&buf_list, false);
  for (auto &buf: buf_list) {
    free(buf);
  }

  max_data_offset_ += kPageSize;
}

EngineRace::~EngineRace() {
  stop_ = true;
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  if (index_buf_) {
    free(index_buf_);
  }

  if (write_data_buf_) {
    free(write_data_buf_);
  }

  if (read_data_buf_) {
    free(read_data_buf_);
  }

  if (-1 != fd_) {
    close(fd_);
  }
}

void EngineRace::WriteEntry() {
  std::vector<write_item*> vs(64, nullptr);
  DEBUG << "db::WriteEntry()" << std::endl;

  auto feed_back = [](write_item *x, int code=kSucc) {
    std::unique_lock<std::mutex> l(x->lock_);
    x->is_done = true;
    x->ret_code = kSucc;
    x->cond_.notify_all();
  };

  while (!stop_) {
    write_queue_.Pop(&vs);
    // firstly, write all the content into file.
    // write all the data.
    for (auto &x: vs) {
      // write the item into disk.
      // deal with each request.
      // step 1. use aio to trigger write of index & value.
      //         NOTE: need to prepare the aligned memory.
      // step 2. feed_back.
      // TODO: update struct io_ev to support multiple events.
      std::cout << x->is_done << std::endl;
    }

    for (auto &x: vs) {
      feed_back(x, kSucc);
    }
  }
}

#ifdef READ_QUEUE
void EngineRace::ReadEntry() {
  std::vector<read_item*> to_read(64, nullptr);
  std::vector<Location> file_pos(64);
  DEBUG << "db::ReadEntry()" << std::endl;

  to_read.resize(0);
  file_pos.resize(0);

  while (!stop_) {
    read_queue_.Pop(&to_read);
  }
}
#endif

void EngineRace::start() {
  std::thread write_thread_(&EngineRace::WriteEntry, this);
  write_thread_.detach();

#ifdef READ_QUEUE
  std::thread read_thread_(&EngineRace::ReadEntry, this);
  read_thread_.detach();
#endif
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  if (key.size() != kMaxKeyLen || value.size() != kMaxValueLen) {
    // check the key size.
    static bool have_find_larger_key = false;
    if (!have_find_larger_key) {
      DEBUG << "[WARN] have find key size = " << key.size() << ":" << " value size = " << value.size() << std::endl;
      have_find_larger_key = true;
    }
  }

  write_item w(&key, &value);
  write_queue_.Push(&w);

  // wait the request writen to disk.
  std::unique_lock<std::mutex> l(w.lock_);
  w.cond_.wait(l, [&w] { return w.is_done; });
  return w.ret_code;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  return kSucc;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;  
}

}  // namespace polar_race

