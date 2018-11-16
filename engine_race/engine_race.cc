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

RetCode HashTreeTable::SetNoLock(const char *key, uint32_t file_no, uint32_t file_offset, spinlock *ar) {
  const int64_t *k = reinterpret_cast<const int64_t*>(key);
  const uint64_t array_pos = compute_pos(*k);

  if (ar) {
    ar[array_pos].lock();
  }

  auto &vs = hash_[array_pos];
  struct disk_index *ptr = nullptr;
  auto ret = find(vs, *k, &ptr);

  if (ret == kNotFound) {
    vs.emplace_back(*k, file_no, file_offset);
  } else {
    ptr->file_no = file_no;
    ptr->file_offset = file_offset;
  }

  if (ar) {
    ar[array_pos].unlock();
  }

  return kSucc;
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
  spinlock *shard_locks = new spinlock[kMaxBucketSize];

  std::vector<std::string> idx_dirs;
  std::string full_idx_dir = file_name_ + kMetaDirName;
  if (0 != GetDirFiles(full_idx_dir, &idx_dirs)) {
    DEBUG << "call GetDirFiles() failed: " << full_idx_dir << std::endl;
  }

  auto insert_item = [&](const struct disk_index &di) {
    const char *k = reinterpret_cast<const char*>(&(di.key));
    hash_.SetNoLock(k, di.file_no, di.file_offset, shard_locks);
  };

  auto init_hash_per_thread = [&](const std::string &fn) {
    // open the folder.
    std::string sub_idx_dir = full_idx_dir + "/" + fn;
    std::vector<std::string> files;
    if (0 != GetDirFiles(sub_idx_dir, &files)) {
      DEBUG << "call GetDirFiles() failed: " << sub_idx_dir << std::endl;
    }
    // sort the meta files.
    std::sort(files.begin(), files.end(),
      [](const std::string &a, const std::string &b) {
        const int va = atoi(a.c_str());
        const int vb = atoi(b.c_str());
        return va < vb;
      }
    );

    // read all the files.
    for (auto &fn: files) {
      auto file_name = sub_idx_dir + "/" + fn;
      // TODO: use NON_BLOCK read,
      // know how many bytes have been read, then deal with them.
      auto fd = open(file_name.c_str(), O_RDONLY, 0644);
      struct disk_index di;
      while (read(fd, &di, sizeof(disk_index)) == sizeof(struct disk_index)) {
        if (di.key == 0 && di.file_no == 0 && di.file_offset == 0) {
          break;
        }
        insert_item(di);
      }
      close(fd);
    }
  };

  std::vector<std::thread> thd_build_hash_list;
  for (auto &idx_dir: idx_dirs) {
    thd_build_hash_list.emplace_back(std::thread(init_hash_per_thread, idx_dir));
  }

  // then open all the data_fds_;
  std::vector<std::string> data_dirs;
  std::string full_data_dir = file_name_ + kDataDirName;
  if (0 != GetDirFiles(full_data_dir, &data_dirs)) {
    DEBUG << "call GetDirFiles() failed: " << full_data_dir << std::endl;
  }
  data_fds_.resize(data_dirs.size() + 1);

  auto deal_single_data_dir = [&](const std::string &dn) {
    std::vector<std::string> files;
    std::string sub_dir_name = full_data_dir + "/" + dn;
    if (0 != GetDirFiles(sub_dir_name, &files)) {
      DEBUG << "call GetDirFiles() failed: " << sub_dir_name << std::endl;
    }
    int x = atoi(dn.c_str());
    data_fds_[x].resize(files.size()+1);
    for (auto &f: files) {
      auto file_name = sub_dir_name + "/" + f;
      auto fd = open(file_name.c_str(), O_RDONLY|O_DIRECT, 0644);
      if (fd < 0) {
        DEBUG << "can not open file " << file_name << std::endl;
        return;
      }
      data_fds_[x][atoi(f.c_str())] = fd;
    }
  };

  std::vector<std::thread> thd_list_data_fds;
  for (auto &d: data_dirs) {
    thd_list_data_fds.emplace_back(std::thread(deal_single_data_dir, d));
  }

  for (auto &v: thd_build_hash_list) {
    v.join();
  }
  for (auto &v: thd_list_data_fds) {
    v.join();
  }

  delete [] shard_locks;
}

EngineRace::~EngineRace() {
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  for (auto &v: data_fds_) {
    for (auto &x: v) {
      close(x);
    }
  }

  end_ = std::chrono::system_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - begin_);
  std::cout << "Total Time " << diff.count() / kNanoToMS << " (micro second)" << std::endl;
}

// get the index dir name.
std::string EngineRace::index_dir(int thread_id) {
  std::string index_dir = file_name_ + kMetaDirName + "/" + std::to_string(thread_id);
  return index_dir;
}

std::string EngineRace::data_dir(int thread_id) {
  std::string data_dir = file_name_ + kDataDirName + "/" + std::to_string(thread_id);
  return data_dir;
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  static thread_local int m_thread_id = 0xffff;
  static thread_local int idx_no = -1;
  static thread_local int data_no = 0;
  static thread_local int idx_fd = -1;
  static thread_local int data_fd = -1; // data file index start from 1, to help on invalid check.
  static thread_local uint64_t idx_size = 0;
  static thread_local uint64_t data_size = 0;
  static thread_local struct disk_index di;
  static thread_local char *idx_buf = GetAlignedBuffer(k1KB);
  static thread_local char *data_buf = GetAlignedBuffer(kPageSize);
  static thread_local char path[64];
  static thread_local int idx_i = 0; // 1K page, just write 1K move forward.

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

    // need to create the thread dir.
    sprintf(path, "%sindex/%d", file_name_.c_str(), m_thread_id);
    mkdir(path, 0755);
    sprintf(path, "%sdata/%d", file_name_.c_str(), m_thread_id);
    mkdir(path, 0755);

    // TODO: this need to find out the last index of index file and data file.
    // in real project.
    idx_no++;
    sprintf(path, "%sindex/%d/%d", file_name_.c_str(), m_thread_id, idx_no);
    DEBUG << "open index file = " << path << std::endl;
    idx_fd = open(path, O_WRONLY | O_CREAT | O_DIRECT | O_NONBLOCK, 0644);
    posix_fallocate(idx_fd, 0, kMaxFileSize);

    data_no++;
    sprintf(path, "%sdata/%d/%d", file_name_.c_str(), m_thread_id, data_no);
    DEBUG << "open data file = " << path << std::endl;
    data_fd = open(path, O_WRONLY | O_CREAT | O_DIRECT | O_NONBLOCK, 0644);
    posix_fallocate(data_fd, 0, kMaxFileSize);
  }

  if ((idx_size + sizeof(struct disk_index)) > kMaxFileSize) {
    idx_no++;
    close(idx_fd);
    sprintf(path, "%sindex/%d/%d", file_name_.c_str(), m_thread_id, idx_no);
    idx_fd = open(path, O_WRONLY | O_CREAT | O_DIRECT | O_NONBLOCK, 0644);
    posix_fallocate(idx_fd, 0, kMaxFileSize);
    idx_size = 0;
  }

  if((data_size + kPageSize) > kMaxFileSize) {
    data_no++;
    close(data_fd);
    sprintf(path, "%sdata/%d/%d", file_name_.c_str(), m_thread_id, data_no);
    data_fd = open(path, O_WRONLY | O_CREAT | O_DIRECT | O_NONBLOCK, 0644);
    posix_fallocate(data_fd, 0, kMaxFileSize);
    data_size = 0;
  }

  // write the position first.
  di.key = toKey(key);
  di.file_no = (m_thread_id<<16) | data_no;
  di.file_offset = data_size;

  struct disk_index *head = (struct disk_index*)idx_buf;
  head[idx_i++] = di; idx_i %= 64;

  // begin to write the index.
  // 1. move
  auto pos = idx_size - (idx_size & 1023);
  lseek(idx_fd, pos, SEEK_SET);
  // 2. then write.
  write(idx_fd, head, k1KB);
  idx_size += sizeof(struct disk_index);

  // check the address is aligned or not?
  const uint64_t addr = (const uint64_t)(value.ToString().c_str());
  if (addr & 4095) {
    memcpy(data_buf, value.ToString().c_str(), kPageSize);
    write(data_fd, data_buf, kPageSize);
  } else {
    write(data_fd, value.ToString().c_str(), kPageSize);
  }
  data_size += kPageSize;
  return kSucc;
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

  // then begin to get the data dir.
  int data_dir = file_no >> 16;
  int sub_file_no = file_no & 0xffff;
  // begin to find the key & pos
  // TODO use non-block read to count the bytes read then copy to value.
  read_aio.SetFD(data_fds_[data_dir][sub_file_no]);
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

