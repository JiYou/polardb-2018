// Copyright [2018] Alibaba Cloud All rights reserved

#include "util.h"
#include "engine_race.h"
#include "engine_aio.h"
#include "engine_hash.h"

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <future>
#include <deque>
#include <map>
#include <numeric>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <set>

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

#ifdef HASH_LOCK

void HashTreeTable::LockHashShard(uint32_t index) {
  spin_lock(hash_lock_[index]);
}

void HashTreeTable::UnlockHashShard(uint32_t index) {
  spin_unlock(hash_lock_[index]);
}
#endif

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

RetCode HashTreeTable::GetNoLock(const char* key, uint64_t *l) {
  const uint64_t *k = reinterpret_cast<const uint64_t*>(key);
  const uint64_t array_pos = compute_pos(*k);

  // then begin to search this array.
  auto &vs = hash_[array_pos];
  kv_info *ptr = nullptr;

#ifdef HASH_LOCK
  auto ret = find(vs, has_sort_.test(array_pos), *k, &ptr);
#else
  auto ret = find(vs, true, *k, &ptr);
#endif

  if (ret == kNotFound) {
    return kNotFound;
  }
  uint64_t pos = ptr->offset_4k_;

  // just get the offset in the big file.
  *l = pos;
  *l <<= kValueLengthBits;
  return kSucc;
}

RetCode HashTreeTable::GetNoLock(const std::string &key, uint64_t *l) {
  return GetNoLock(key.c_str(), l);
}

#ifdef HASH_LOCK
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
#endif


RetCode HashTreeTable::SetNoLock(const char *key, uint64_t l) {
  const int64_t *k = reinterpret_cast<const int64_t*>(key);
  const uint64_t array_pos = compute_pos(*k);
  l >>= kValueLengthBits;
  uint32_t pos = l;

  auto &vs = hash_[array_pos];
  kv_info *ptr;

#ifdef HASH_LOCK
  auto ret = find(vs, has_sort_.test(array_pos), *k, &ptr);
#else
  auto ret = find(vs, true, *k, &ptr);
#endif

  if (ret == kNotFound) {
    vs.emplace_back(*k, pos);
    // broken the sorted list.
#ifdef HASH_LOCK
    has_sort_.reset(array_pos);
#endif
  } else {
    ptr->offset_4k_ = pos;
  }
  return kSucc;
}

RetCode HashTreeTable::SetNoLock(const std::string &key, uint64_t l) {
  return SetNoLock(key.c_str(), l);
}

#ifdef HASH_LOCK

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
#endif

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

#ifdef HASH_LOCK
  auto set_all_sorted = [this]() {
      has_sort_.set();
  };
  std::thread set_sort_bit(set_all_sorted);
#endif

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

#ifdef HASH_LOCK
  set_sort_bit.join();
#endif
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
  bool new_db = false;
  // create the dir.
  if (!FileExists(name)) {
    new_db = true;
    if (mkdir(name.c_str(), 0755)) {
      DEBUG << "mkdir " << name << " failed "  << std::endl;
      return kIOError;
    }
  }

  // use truncate to create the big file.
  auto create_big_file = [&]() {
    engine_race->fd_ = open(file_name, O_RDWR | O_DIRECT, 0644);

    // if not exists. then create it.
    if (engine_race->fd_ < 0 && errno == ENOENT) {
      new_db = true;
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
    // engine_race->write_aio_.SetFD(engine_race->fd_);
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

  // next thread to create the big file.
  thd_create_big_file.join();
  thd_creat_lock_file.join();
  if (meet_error) {
    delete engine_race;
    return kIOError;
  }

  // after the big file is create, then begin to bulid the
  // HashTable.
  // the fd_ is opened.
  // after build Hash table, also record the next to write pos.
  if (!new_db) {
  }

  engine_race->max_cpu_cnt_ = std::thread::hardware_concurrency();
  *eptr = engine_race;

  return kSucc;
}

// or just read 800MB from disk?
// which is faster?
void EngineRace::BuildHashTable() {
  hash_.Init();
  // use two thread, on read every 16MB from disk.
  // the other one insert the item into hash table.
  struct buffer_mgr {
    std::mutex free_lock;
    std::condition_variable free_cond;
    std::vector<char*> free_buffers;

    std::mutex data_lock;
    std::condition_variable data_cond;
    std::deque<char*> data_buffers;

    std::atomic<bool> read_over{false};

    buffer_mgr() {
      for (uint64_t i = 0; i < 10; i++) {
        free_buffers.push_back(GetAlignedBuffer(k16MB));
      }
    }

    ~buffer_mgr() {
      for (auto &x: free_buffers) {
        DEBUG << "free 16MB\n";
        free(x);
      }
      for (auto &x: data_buffers) {
        DEBUG << "free 16MB\n";
        free(x);
      }
    }

    char *GetFreeBuffer() {
      std::unique_lock<std::mutex> l(free_lock);
      free_cond.wait(l, [&] { return free_buffers.size() > 0; });
      char *buf = free_buffers.back();
      free_buffers.pop_back();
      return buf;
    }

    void PutFreeBuffer(char *buf) {
      std::unique_lock<std::mutex> l(free_lock);
      free_buffers.push_back(buf);
      free_cond.notify_one();
    }

    // must get from front buffer.
    char *GetDataBuffer() {
      std::unique_lock<std::mutex> l(data_lock);
      data_cond.wait(l, [&] { return data_buffers.size() > 0; });
      char *buf = data_buffers.front();
      data_buffers.pop_front();
      return buf;
    }

    void PutDataBuffer(char *buf) {
      std::unique_lock<std::mutex> l(data_lock);
      data_buffers.push_back(buf);
      data_cond.notify_one();
    }
  };
  struct buffer_mgr mgr;

  // just read the content from disk, then put
  // the content into data buffer.
  auto read_disk = [&]() {
    uint64_t offset = 0;
    while (!mgr.read_over) {
      char *buf = mgr.GetFreeBuffer();
      read_aio_.Clear();
      read_aio_.PrepareRead(offset, buf, k16MB);
      read_aio_.Submit();
      read_aio_.WaitOver();
      uint64_t *ar = reinterpret_cast<uint64_t*>(buf);
      if (ar[k16MB/8-1] == 0) {
        mgr.read_over = true;
      }
      mgr.PutDataBuffer(buf);
      offset += k16MB;
    }
  };
  std::thread thd_read_disk(read_disk);

  bool has_find_valid = false;
  // this is not thread function.
  auto buf_to_hash = [&](char *buf) {
    struct disk_index *array = reinterpret_cast<struct disk_index*>(buf);
    for (uint64_t i = 0; i < k16MB / sizeof(struct disk_index); i++) {
      auto ref = array + i;
      if (ref->pos == 0) {
        break;
      }
      max_index_offset_ += sizeof(struct disk_index);
      if (ref->pos == kIndexSkipType) {
        continue;
      }
      has_find_valid = true;
      hash_.SetNoLock(ref->key, ref->pos);
      max_data_offset_ = std::max(max_data_offset_, ref->pos);
    }
  };

  auto insert_hash = [&]() {
    while (!mgr.read_over) {
      char *buf = mgr.GetDataBuffer();
      buf_to_hash(buf);
      mgr.PutFreeBuffer(buf);
    }

    // deal with all the left data buffer;
    // note: no lock here.
    for (auto &x: mgr.data_buffers) {
      buf_to_hash(x);
    }
  };
  std::thread thd_insert_hash(insert_hash);

  thd_read_disk.join();
  thd_insert_hash.join();
  DEBUG << "max_data_offset_ = " << max_data_offset_ << std::endl;
  DEBUG << "max_index_offset_ = " << max_index_offset_ << std::endl;
}

EngineRace::~EngineRace() {
  stop_ = true;
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  if (-1 != fd_) {
    close(fd_);
  }
}

void EngineRace::WriteEntry() {
  // there are two threads here.
  // one is just flush the io into disks
  // after submit, then it return to client.
  // the other one, call the WaitOver with specific time.
  struct aio_mgr {
    std::mutex free_lock;
    std::condition_variable free_cond;
    std::vector<struct aio_env_two*> free_io;

    std::mutex data_lock;
    std::condition_variable data_cond;
    std::deque<struct aio_env_two*> data_io;

    std::atomic<bool> game_over {false};
    // every round of write,
    // 1KB for index. 256KB for data.
    // then nearly 257MB for all the buffer and memory.
    #define TWO_AIO_SZ 1024
    struct aio_env_two _aio_[TWO_AIO_SZ];
    aio_mgr(int fd_) {
      for (uint64_t i = 0; i < TWO_AIO_SZ; i++) {
        _aio_[i].SetFD(fd_);
        free_io.push_back(_aio_+i);
      }
    }

    ~aio_mgr() {
      game_over = true;
    }

    struct aio_env_two *GetFreeAIO() {
      std::unique_lock<std::mutex> l(free_lock);
      free_cond.wait(l, [&] { return free_io.size() > 0 || game_over; });
      if (game_over) {
        return nullptr;
      }
      auto *aio= free_io.back();
      free_io.pop_back();
      return aio;
    }

    void PutFreeAIO(struct aio_env_two *aio) {
      std::unique_lock<std::mutex> l(free_lock);
      free_io.push_back(aio);
      free_cond.notify_one();
    }

    // must get from front buffer.
    struct aio_env_two *GetDataAIO() {
      std::unique_lock<std::mutex> l(data_lock);
      data_cond.wait(l, [&] { return data_io.size() > 0 || game_over; });
      if (game_over) {
        return nullptr;
      }
      auto *aio = data_io.front();
      data_io.pop_front();
      return aio;
    }

    void PutDataAIO(struct aio_env_two *aio) {
      std::unique_lock<std::mutex> l(data_lock);
      data_io.push_back(aio);
      data_cond.notify_one();
    }
  };

  struct aio_mgr mgr(fd_);

  auto wait_aio = [&]() {
    while (!mgr.game_over) {
      auto aio = mgr.GetDataAIO();
      if (!aio) {
        break;
      }
      aio->WaitOver();
      mgr.PutFreeAIO(aio);
    }
  };
  std::thread thd_wait_aio(wait_aio);
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(1, &cpuset);
    auto thread_pid = thd_wait_aio.native_handle();
    int rc = pthread_setaffinity_np(thread_pid, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
}

  std::vector<write_item*> vs(64, nullptr);
  DEBUG << "db::WriteEntry()" << std::endl;
  uint64_t empty_key = 0;

  while (!stop_) {
    write_queue_.Pop(&vs);
    // get free aio
    auto aio = mgr.GetFreeAIO();
    if (!aio) {
      break;
    }

    struct disk_index *di = reinterpret_cast<struct disk_index*>(aio->index_buf);
    char *to = aio->data_buf;
    for (uint32_t i = 0; i < kMaxThreadNumber; i++) {
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

    auto f =  std::async(std::launch::async, [&]() {
      aio->Clear();
      aio->PrepareWrite(max_index_offset_, aio->index_buf, k1KB);
      aio->PrepareWrite(max_data_offset_, aio->data_buf, data_write_size);
      aio->Submit();
      max_index_offset_ += k1KB;
      max_data_offset_ += data_write_size;
    });

    for (auto &x: vs) {
      x->feed_back();
    }
    f.get();
    mgr.PutDataAIO(aio);
  }
  mgr.game_over = true;
  thd_wait_aio.join();
}

void EngineRace::start_write_thread() {
  static std::once_flag initialized_write;
  std::call_once (initialized_write, [this] {
    std::thread write_thread(&EngineRace::WriteEntry, this);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    auto thread_pid = write_thread.native_handle();
    int rc = pthread_setaffinity_np(thread_pid, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    write_thread.detach();
  });
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  start_write_thread();

  static thread_local uint64_t m_cpu_id = 0xffff;
  if (m_cpu_id == 0xffff) {
    auto thread_pid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    m_cpu_id = cpu_id_++;
    CPU_SET(m_cpu_id % max_cpu_cnt_, &cpuset);
    int rc = pthread_setaffinity_np(thread_pid, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
  }

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
  // lasy init of hash table.
  // init the read map.
  static std::once_flag init_mptr;
  std::call_once (init_mptr, [this] {
    BEGIN_POINT(begin_build_hash_table);
    BuildHashTable();
    END_POINT(end_build_hash_table, begin_build_hash_table, "build_hash_time");

    BEGIN_POINT(begin_sort_hash_table);
    hash_.Sort();
    END_POINT(end_sort_hash_table, begin_sort_hash_table, "hash_sort_time");
  });

  static thread_local uint64_t m_cpu_id = 0xffff;
  if (m_cpu_id == 0xffff) {
    auto thread_pid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    m_cpu_id = cpu_id_++;
    CPU_SET(m_cpu_id % max_cpu_cnt_, &cpuset);
    int rc = pthread_setaffinity_np(thread_pid, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
  }

  // read the content parallel.
  // just give pos,buf to read queue.
  struct local_buf_read {
    char *buf = nullptr;
    local_buf_read() {
      buf = GetAlignedBuffer(kPageSize); // just 1 MB for every thread as cache.
    }
    ~local_buf_read() {
      free(buf);
    }
  };

  static thread_local struct local_buf_read lb;
  static thread_local struct aio_env_single_read raio(fd_);
  static thread_local uint64_t cnt= 0;
  cnt++;
  if (cnt % 500000 == 0) {
    printf(".\n");
  }
  uint64_t offset = 0;
  RetCode ret = hash_.GetNoLock(key.ToString().c_str(), &offset);
  if (ret == kNotFound) {
    return ret;
  }

  raio.PrepareRead(offset, lb.buf, kPageSize);
  raio.Submit();
  raio.WaitOver();
  value->assign(lb.buf, kPageSize);
  return kSucc;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

}  // namespace polar_race

