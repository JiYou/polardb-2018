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
#include <sys/mman.h>

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

RetCode HashTreeTable::find(std::vector<struct disk_index> &vs,
                            uint64_t key,
                            struct disk_index**ptr) {
  auto pos = std::lower_bound(vs.begin(),
    vs.end(), key, [](const disk_index &a, uint64_t b) {
    return a.key < b;
  });
  // has find.
  if (pos != vs.end() && !(key < pos->key)) {
    *ptr = &(*pos);
    return kSucc;
  }
  return kNotFound;
}

RetCode HashTreeTable::GetNoLock(const char* key, uint32_t *file_no, uint32_t *file_offset) {
  const uint64_t *k = reinterpret_cast<const uint64_t*>(key);
  const uint64_t array_pos = compute_pos(*k);
  // then begin to search this array.
  auto &vs = hash_[array_pos];
  struct disk_index *ptr = nullptr;
  auto ret = find(vs, *k, &ptr);
  if (ret == kNotFound) {
    return kNotFound;
  }
  *file_no = ptr->file_no;
  *file_offset = ptr->file_offset;
  return kSucc;
}

RetCode HashTreeTable::GetNoLock(const std::string &key, uint32_t *file_no, uint32_t *file_offset) {
  return GetNoLock(key.c_str(),  file_no, file_offset);
}

RetCode HashTreeTable::SetNoLock(const char *key, uint32_t file_no, uint32_t file_offset) {
  const int64_t *k = reinterpret_cast<const int64_t*>(key);
  const uint64_t array_pos = compute_pos(*k);
  auto &vs = hash_[array_pos];
  struct disk_index *ptr = nullptr;
  auto ret = find(vs, *k, &ptr);

  if (ret == kNotFound) {
    vs.emplace_back(*k, file_no, file_offset);
  } else {
    ptr->file_no = file_no;
    ptr->file_offset = file_offset;
  }
  return kSucc;
}

RetCode HashTreeTable::SetNoLock(const std::string &key, uint32_t file_no, uint32_t file_offset) {
  return SetNoLock(key.c_str(), file_no, file_offset);
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

  engine_race->begin_ = std::chrono::system_clock::now();

  std::atomic<bool> meet_error{false};
  engine_race->file_name_ = name + "/";
  // create the dir.
  if (!FileExists(name)) {
    if (mkdir(name.c_str(), 0755)) {
      DEBUG << "mkdir " << name << " failed "  << std::endl;
      return kIOError;
    }
  }

  auto creat_lock_file = [&]() {
    if (0 != LockFile(name + "/" + kLockFile, &(engine_race->db_lock_))) {
      meet_error = true;
      DEBUG << "Generate LOCK file failed" << std::endl;
    }
  };
  std::thread thd_creat_lock_file(creat_lock_file);

  thd_creat_lock_file.join();
  if (meet_error) {
    delete engine_race;
    return kIOError;
  }

  engine_race->max_cpu_cnt_ = std::thread::hardware_concurrency();
  *eptr = engine_race;

  return kSucc;
}

void EngineRace::BuildHashTable() {
  hash_.Init();
/*
  // the content into data buffer.
  auto read_disk = [&]() {
    uint64_t offset = 0;
    while (!mgr.read_over) {
      char *buf = mgr.GetFreeBuffer();
      read_aio.Prepare16MB(offset, buf);
      read_aio.Submit();
      read_aio.WaitOver();
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
*/
}

EngineRace::~EngineRace() {
  stop_ = true;
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  end_ = std::chrono::system_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - begin_);
  std::cout << "Total Time " << diff.count() / kNanoToMS << " (micro second)" << std::endl;
}

void EngineRace::WriteEntry() {
  std::vector<write_item*> vs(64, nullptr);
  uint64_t idx_size = 0;
  uint64_t data_size = 0;
  struct aio_env_two aio;

  // write file every time from the start
  uint64_t idx_no = 0;
  uint64_t data_no = 0;
  int idx_fd = -1;
  int data_fd = -1;

  while (!stop_) {
    write_queue_.Pop(&vs);

    struct disk_index *di = reinterpret_cast<struct disk_index*>(aio.index_buf);
    char *to = aio.data_buf;

    auto cp_mem = [&]() {
      for (uint32_t i = 0; i < kMaxThreadNumber; i++) {
        if (i < vs.size()) {
          auto &x = vs[i];
          di->key = toKey(x->key->ToString().c_str());
          di->file_no = data_no;
          di->file_offset = data_size;
          di++;
          memcpy(to, x->value->ToString().c_str(), kPageSize);
          to += kPageSize;
        } else {
          di->key = 0;
          di->file_no = 0xffff;
          di->file_offset = 0xffff;
          di++;
        }
      }
    };
    std::thread thd_cp_mem(cp_mem);


    auto create_file = [](const char *file_name) ->int {
        int fd = open(file_name, O_RDWR | O_CREAT | O_DIRECT, 0644);
        if (fd < 0) {
          DEBUG << "open inex file " << file_name << "failed\n";
          return -1;
        }
        // pre alloc the file data.
        auto ret = posix_fallocate(fd, 0, kMaxFileSize);
        if (ret) {
          DEBUG << "posix_fallocate failed, ret = " << ret << std::endl;
          return -1;
        }
        return fd;
    };

    auto cr_fd = [&]() {
      if (idx_fd == -1 || (idx_size + kPageSize) > kMaxFileSize) {
        idx_no++;
        auto idx_name = file_name_ + "idx_" + std::to_string(idx_no);
        if (idx_fd > 0) {
          close(idx_fd);
        }
        idx_fd = create_file(idx_name.c_str());
        idx_size = 0;
      }

      if(data_fd == -1 || (data_size + k256KB) > kMaxFileSize) {
        data_no++;
        auto data_name = file_name_ + "data" + std::to_string(data_no);
        if (data_fd > 0) {
          close(data_fd);
        }
        data_fd = create_file(data_name.c_str());
        data_size = 0;
      }
    };
    std::thread thd_cr_fd(cr_fd);

    thd_cp_mem.join();
    thd_cr_fd.join();

    auto f = std::async(std::launch::async, [&]() {
      aio.Clear();
      aio.PrepareWrite(idx_fd, idx_size, aio.index_buf, k1KB);
      aio.PrepareWrite(data_fd, data_size, aio.data_buf, k256KB);
      aio.Submit();
      idx_size += kPageSize;
      data_size += k256KB;
    });

    for (auto &x: vs) {
      x->feed_back();
    }
    f.get();
    aio.WaitOver();
  }
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
  return kSucc;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

}  // namespace polar_race

