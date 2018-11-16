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
  // create index/data dir.
  std::string index_dir = engine_race->file_name_ + kMetaDirName;
  if (mkdir(index_dir.c_str(), 0755)) {
    DEBUG << "mkdir" << index_dir << "failed\n";
  }
  std::string data_dir = engine_race->file_name_ + kDataDirName;
  if (mkdir(data_dir.c_str(), 0755)) {
    DEBUG << "mkdir" << data_dir << "failed\n";
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

  // scan all the files under the folder.
  std::vector<std::string> files;
  // scan the index file dir.
  std::string dir = file_name_ + kMetaDirName;
  if (0 != GetDirFiles(dir, &files)) {
    DEBUG << "call GetDirFiles() failed: " << dir << std::endl;
  }

  // sort the meta files.
  std::sort(files.begin(), files.end(),
    [](const std::string &a, const std::string &b) {
      const int va = atoi(a.c_str());
      const int vb = atoi(b.c_str());
      return va < vb;
    }
  );

  // read all the files into hash table.
  char *buf = GetAlignedBuffer(kMaxFileSize + 32);
  if (!buf) {
    DEBUG << "alloc memory for buf failed\n";
    return;
  }

  struct aio_env_single read_aio(-1, true/*read*/, false/*nobuf*/);
  uint64_t cnt = 0;

  for (auto fn: files) {
    // read all the conten from file.
    auto file_name = file_name_ + kMetaDirName + "/" + fn;
    auto fd = open(file_name.c_str(), O_RDONLY|O_DIRECT, 0644);
    if (fd < 0) {
      DEBUG << "can not open file: " << file_name << std::endl;
      return;
    }

    read_aio.SetFD(fd);
    read_aio.Prepare100MB(0, buf);
    read_aio.Submit();
    read_aio.WaitOver();

    struct disk_index *di = (struct disk_index*)buf;
    for (uint64_t i = 0; i < kMaxFileSize / sizeof(struct disk_index); i++) {
      auto ref = di[i];
      if (ref.file_no == 0 && ref.file_offset == 0) {
        break;
      }
      if (!ref.is_valid()) {
        continue;
      }
      cnt++;
      const char *k = reinterpret_cast<const char*>(&(ref.key));
      hash_.SetNoLock(k, ref.file_no, ref.file_offset);
    }
    // NOTE: if the file is less than 100MB, need to clear the memory.

    DEBUG << "item = " << cnt << std::endl;

    close(fd);
  }

  free(buf);

  // then open all the data_fds_;
  files.clear();
  dir = file_name_ + kDataDirName;
  if (0 != GetDirFiles(dir, &files)) {
    DEBUG << "call GetDirFiles() failed: " << dir << std::endl;
  }
  data_fds_.resize(files.size() + 1, 0);

  for (auto &fn : files) {
    std::string file_name = file_name_ + kDataDirName + "/" + fn;
    auto fd = open(file_name.c_str(), O_RDONLY|O_DIRECT, 0644);
    if (fd < 0) {
      DEBUG << "can not open file " << file_name << std::endl;
      return;
    }
    data_fds_[atoi(fn.c_str())] = fd;
  }
}

EngineRace::~EngineRace() {
  stop_ = true;
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  for (auto &fd: data_fds_) {
    close(fd);
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
  int64_t idx_no = -1;
  int64_t data_no = 0;
  int idx_fd = -1;
  int data_fd = -1;

  while (!stop_) {
    write_queue_.Pop(&vs);

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
      if (idx_fd == -1 || (idx_size + k1KB) > kMaxFileSize) {
        idx_no++;
        auto idx_name = file_name_ + kMetaDirName + "/" + std::to_string(idx_no);
        if (idx_fd > 0) {
          close(idx_fd);
        }
        idx_fd = create_file(idx_name.c_str());
        idx_size = 0;
      }

      if(data_fd == -1 || (data_size + k256KB) > kMaxFileSize) {
        data_no++;
        auto data_name = file_name_ + kDataDirName +"/" + std::to_string(data_no);
        if (data_fd > 0) {
          close(data_fd);
        }
        data_fd = create_file(data_name.c_str());
        data_size = 0;
      }
    };
    cr_fd();

    struct disk_index *di = reinterpret_cast<struct disk_index*>(aio.index_buf);
    char *to = aio.data_buf;
    auto file_pos = data_size;
    auto cp_mem = [&]() {
      for (uint32_t i = 0; i < kMaxThreadNumber; i++) {
        if (i < vs.size()) {
          auto &x = vs[i];
          di->key = toKey(x->key->ToString().c_str());
          di->file_no = data_no;
          di->file_offset = file_pos;
          di++;
          memcpy(to, x->value->ToString().c_str(), kPageSize);
          to += kPageSize;
          file_pos += kPageSize;
        } else {
          di->key = 0;
          di->file_no = 0xffffffff;
          di->file_offset = 0xffffffff;
          di++;
        }
      }
    };
    cp_mem();

    auto f = std::async(std::launch::async, [&]() {
      aio.Clear();
      aio.PrepareWrite(idx_fd, idx_size, aio.index_buf, k1KB);
      aio.PrepareWrite(data_fd, data_size, aio.data_buf, k256KB);
      aio.Submit();
      idx_size += k1KB;
      data_size += k256KB;
    });

    for (auto &x: vs) {
      x->feed_back();
    }
    f.get();
    aio.WaitOver();
  }

  close(idx_fd);
  close(data_fd);
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

  static thread_local uint64_t m_thread_id = 0xffff;
  if (m_thread_id == 0xffff) {
    auto thread_pid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    m_thread_id = thread_id_++;
    CPU_SET(m_thread_id % max_cpu_cnt_, &cpuset);
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

  static thread_local uint64_t m_thread_id = 0xffff;
  if (m_thread_id == 0xffff) {
    auto thread_pid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    m_thread_id = thread_id_++;
    CPU_SET(m_thread_id % max_cpu_cnt_, &cpuset);
    int rc = pthread_setaffinity_np(thread_pid, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
  }

  static thread_local struct aio_env_single read_aio(-1, true/*read*/, true/*buf*/);
  uint32_t file_no = 0;
  uint32_t file_offset = 0;
  auto ret = hash_.GetNoLock(key.ToString().c_str(), &file_no, &file_offset);
  if (ret != kSucc) {
    return kNotFound;
  }
  // begin to find the key & pos
  read_aio.SetFD(data_fds_[file_no]);
  read_aio.Prepare(file_offset);
  read_aio.Submit();
  read_aio.WaitOver();
  value->assign(read_aio.buf, kPageSize);
  return kSucc;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

}  // namespace polar_race

