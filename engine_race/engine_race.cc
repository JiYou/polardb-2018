// Copyright [2018] Alibaba Cloud All rights reserved

#include "util.h"
#include "engine_race.h"
#include "libaio.h"

#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <deque>
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
  kv_info *ptr = nullptr;
  auto ret = find(vs, has_sort_.test(array_pos), *k, &ptr);

  if (ret == kNotFound) {
    UnlockHashShard(array_pos);
    return kNotFound;
  }
  uint64_t pos = ptr->offset_4k_;
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
  DEBUG << "HashHard Stat: mean = " << mean
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

  // create the dir.
  if (!FileExists(name)
      && 0 != mkdir(name.c_str(), 0755)) {
    DEBUG << "mkdir " << name << " failed "  << std::endl;
    return kIOError;
  }

  bool new_create = false;
  // use truncate to create the big file.
  auto create_big_file = [&engine_race, &meet_error, &name, &file_name, &new_create]() {
    engine_race->fd_ = open(file_name, O_RDWR | O_DIRECT, 0644);

    // if not exists. then create it.
    if (engine_race->fd_ < 0 && errno == ENOENT) {
      engine_race->fd_ = open(file_name, O_RDWR | O_CREAT | O_DIRECT, 0644);
      if (engine_race->fd_ < 0) {
        DEBUG << "create big file failed!\n";
        meet_error = true;
        return;
      }
      // if open success.
      int ret = 0;
      if ((ret=posix_fallocate(engine_race->fd_, 0, kBigFileSize))) {
        DEBUG << "posix_fallocate failed, ret = " << ret << std::endl;
        meet_error = true;
        return;
      }
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
  auto alloc_buf = [&engine_race, &meet_error] () {
    engine_race->index_buf_ = GetAlignedBuffer(kPageSize);
    if (!(engine_race->index_buf_)) {
      meet_error = true;
      return;
    }
    engine_race->write_data_buf_ = GetAlignedBuffer(kPageSize * kMaxThreadNumber + 2 * kPageSize);
    if (!(engine_race->write_data_buf_)) {
      meet_error = true;
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
  BEGIN_POINT(begin_build_hash_table);
  engine_race->BuildHashTable();
  END_POINT(end_build_hash_table, begin_build_hash_table, "build_hash_time");

  engine_race->hash_.Sort();
  engine_race->hash_.PrintMeanStdDev();

  engine_race->start();
  *eptr = engine_race;

  return kSucc;
}

// or just read 800MB from disk?
// which is faster?
void EngineRace::BuildHashTable() {
  // JIYOU -- begin
  {
    std::string file_name_str = dir_ + "/" + kBigFileName;
    const char *file_name = file_name_str.c_str();

    int fd = open(file_name, O_RDONLY, 0644);
    if (fd < 0) {
      DEBUG << "open big file failed in build hash\n";
      return;
    }
    // read 16 bytes every time.
    struct disk_index di;
    int cnt = 0;
    int valid_cnt = 0;
    while (true) {
      di.pos = 0;
      if (read(fd, &di, 16) != 16) {
        break;
      }
      if (di.pos == 0) break;
      ++cnt;
      if (di.pos == kIndexSkipType) continue;
      valid_cnt ++;
      hash_.Set(di.key, di.pos);
    }
    std::cout << "read over: cnt = " << cnt << " , valid_cnt = " << valid_cnt << std::endl;
    close(fd);
    max_index_offset_ = cnt * 16;

    // set the next begin to write position.
    if (cnt) {
      max_data_offset_ += kPageSize;
      max_index_offset_ += sizeof(struct disk_index);
    } else {
      DEBUG << "not find valid item" << std::endl;
    }

    DEBUG << "max_data_offset_ = " << max_data_offset_ << std::endl;
    DEBUG << "max_index_offset_ = " << max_index_offset_ << std::endl;

    return;
  }
  // JIYOU  -- end


  // read 4MB from disk. this is a single read.

  spinlock md_lock;
  std::atomic<uint64_t> md_pos{max_data_offset_};
  std::atomic<uint64_t> id_pos{max_index_offset_};
  std::atomic<bool> has_find_valid{false};
  auto buf_to_hash = [&](char *buf) {
    // read the buf content then put them into hash table.
    struct disk_index *array = reinterpret_cast<struct disk_index*>(buf);
    for (uint32_t i = 0; i < k4MB / sizeof(struct disk_index); i++) {
      // puth every index into hash table.
      auto ref = array + i;
      if (ref->pos == 0) {
        break;
      }
      id_pos += sizeof(struct disk_index);
      if (ref->pos == kIndexSkipType) {
        continue;
      }
      has_find_valid = true;
      md_lock.lock();
      hash_.Set(ref->key, ref->pos);
      if (ref->pos > md_pos) {
        md_pos = ref->pos;
      }
      md_lock.unlock();
    }
  };

  // there would be 64 thread to read the different
  // part and 4MB a time. then build the hash map.
  // such as: thread 0 reads [0, 4MB), [64MB, 68MB)
  //          thread 1 reads [4MB, 8MB), [68MB, 72MB)
  //          ....
  //          thread 63 reads [63*4MB ....)
  int fd = fd_;
  auto build_thread = [&](int id) {
    auto buf = GetAlignedBuffer(k4MB);
    struct aio_env raio;
    raio.SetFD(fd);
    bool read_over = false;
    uint64_t file_offset = id * k4MB;
    while (!read_over) {
      // read data from disk.
      raio.Clear();
      raio.PrepareRead(file_offset, buf, k4MB);
      raio.Submit();
      raio.WaitOver();

      // check the last uint64_t
      uint64_t *ar = reinterpret_cast<uint64_t*>(buf);
      if (ar[k4MB/8-1] == 0) {
        read_over = true;
      }
      buf_to_hash(buf);
      file_offset += k4MB * 64ull; // step is 256MB
    }
    free(buf);
  };

  std::vector<std::thread> vthreads;
  for (uint32_t i = 0; i < kMaxThreadNumber; i++) {
    vthreads.emplace_back(std::thread(build_thread, i));
  }
  for (auto &x: vthreads) {
    x.join();
  }

  // set the next begin to write position.
  max_data_offset_ = md_pos;
  max_index_offset_ = id_pos;
  if (has_find_valid) {
    max_data_offset_ += kPageSize;
    max_index_offset_ += sizeof(struct disk_index);
  }
  DEBUG << "max_data_offset_ = " << max_data_offset_ << std::endl;
  DEBUG << "max_index_offset_ = " << max_index_offset_ << std::endl;
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

  if (-1 != fd_) {
    close(fd_);
  }
}

void EngineRace::WriteEntry() {
  std::vector<write_item*> vs(64, nullptr);
  DEBUG << "db::WriteEntry()" << std::endl;
  uint64_t empty_key = 0;

  while (!stop_) {
    write_queue_.Pop(&vs);
    struct disk_index *di = reinterpret_cast<struct disk_index*>(index_buf_);
    char *to = write_data_buf_;
    for (uint32_t i = 0; i <= kMaxThreadNumber; i++) {
      if (i < vs.size()) {
        auto &x = vs[i];
        char *key_buf = const_cast<char*>(x->key->ToString().c_str());
        uint64_t *key = reinterpret_cast<uint64_t*>(key_buf);
        di->SetKey(key);
        di->pos = max_data_offset_ + (i<<kValueLengthBits);
        di++;
        memcpy(to, x->value->ToString().c_str(), kPageSize);
        to += kPageSize;
      } else {
        di->SetKey(&empty_key);
        di->pos = kIndexSkipType;
        di++;
      }
    }

    uint32_t data_write_size = vs.size() << 12;
    write_aio_.Clear();
    write_aio_.PrepareWrite(max_index_offset_, index_buf_, k1KB);
    write_aio_.PrepareWrite(max_data_offset_, write_data_buf_, data_write_size);
    write_aio_.Submit();

    // then you can do something here usefull. instead of waiting the disk write over.
    // ==================
    // BEGIN of co-task

    // update the hash table at the same time.
    uint64_t old_pos = max_data_offset_;
    for (auto &x: vs) {
      // begin to insert all the items into HashTable.
      // may sort here, because the write may take some time.
      hash_.Set(x->key->ToString().c_str(), old_pos);
      old_pos += kPageSize;
    }

    max_index_offset_ += k1KB;
    max_data_offset_ += data_write_size;
    // ==================
    // END of co-task

    BEGIN_POINT(begin_wait_disk_over);
    write_aio_.WaitOver(); // would stuck here.
    END_POINT(end_write_disk_over, begin_wait_disk_over, "write_aio_.WaitOver()");

    // ack all the writers.
    for (auto &x: vs) {
      x->feed_back();
    }
  }
}

#ifdef READ_QUEUE
void EngineRace::ReadEntry() {
  std::vector<read_item*> to_read(kMaxThreadNumber, nullptr);
  std::vector<uint64_t> file_pos(kMaxThreadNumber, 0);
  DEBUG << "db::ReadEntry()" << std::endl;

  while (!stop_) {
    read_queue_.Pop(&to_read);
    read_aio_.Clear();
    for (auto &x: to_read) {
      x->buf[0] = 0;
      assert (x->pos >= kMaxIndexSize);
      read_aio_.PrepareRead(x->pos, x->buf, kPageSize, x);
    }
    read_aio_.Submit();
    read_aio_.WaitOver(); // it will call the call back function.
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
  // TODO: add write cache hit system ?
  // if hit the previous value.
  write_item w(&key, &value);
  write_queue_.Push(&w);

  // wait the request writen to disk.
  std::unique_lock<std::mutex> l(w.lock_);
  w.cond_.wait(l, [&w] { return w.is_done; });
  return w.ret_code;
}

// for 64 read threads, it would take 64MB as cache read.
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  // compute the position.
  // just give pos,buf to read queue.
  struct local_buf {
    char *buf = nullptr;
    local_buf() {
      buf = GetAlignedBuffer(kPageSize); // just 1 MB for every thread as cache.
    }
    ~local_buf() {
      free(buf);
    }
  };

  static thread_local struct local_buf lb;
  // cache strategy. Just cache the found items.
  // all the not found item
  // need to search in the hash.
  // TODO: design a bloom filter?
  static thread_local bool has_read = false;
  static thread_local uint64_t pre_key = 0;

  value->resize(kPageSize);
  // opt 1. hit the pre-key?
  //       . no need to compute the position with hash.
  const uint64_t *new_key = reinterpret_cast<const uint64_t*>(key.ToString().c_str());
  if (has_read && pre_key == *new_key) {
    // just copy the buffer.
    char *tob = const_cast<char*>(value->c_str());
    char *fob = lb.buf;
    //engine_memcpy(tob, fob);
    memcpy(tob, fob, kPageSize);
    return kSucc;
  }

  //        compute the position.
  uint64_t offset = 0;
  RetCode ret = hash_.Get(key.ToString().c_str(), &offset);
  if (ret == kNotFound) {
    return ret;
  }

  // NOTE: update the cache.
  // udpate the cache.
  pre_key = *new_key;
  has_read = true;

  read_item r(offset, lb.buf);
  read_queue_.Push(&r);

  std::unique_lock<std::mutex> l(r.lock_);
  r.cond_.wait(l, [&r] { return r.is_done; });

  // copy the buffer
  char *new_buf = lb.buf;
  char *target_buf = const_cast<char*>(value->c_str());
  // engine_memcpy(target_buf, new_buf);
  memcpy(target_buf, new_buf, kPageSize);
  return r.ret_code;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

}  // namespace polar_race

