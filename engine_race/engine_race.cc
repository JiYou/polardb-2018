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

bool HashTreeTable::CopyToAll() {
  if (!hash_.empty()) {
    for (auto &vs: hash_) {
      for (auto &x: vs) {
        all_.push_back(x);
      }
    }
    return true;
  }
  return false;
}

void HashTreeTable::Save(const char *file_name) {
  BEGIN_POINT(begin_save_index);
  int fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_NOATIME, 0644);
  if (fd < 0 ) {
    DEBUG << "open " << file_name << "failed\n";
    return;
  }

  // begin to write all the index into single file.
  for (auto &vs: hash_) {
    if (!vs.empty()) {
      const int write_size = vs.size() * sizeof(struct disk_index);
      if (write(fd, vs.data(), write_size) != write_size) {
        DEBUG << "write vector to " << file_name << " failed\n";
      }
    }
  }
  close(fd);
  DEBUG << "save all index to " << file_name << std::endl;
  END_POINT(end_save_index, begin_save_index, "save_all_time_ms");
}

bool HashTreeTable::Load(const char *file_name) {
  int fd = open(file_name, O_RDONLY | O_NOATIME, 0644);
  if (fd < 0) {
    DEBUG << "open " << file_name << " failed\n";
    return false;
  }

  hash_.clear();
  all_.resize(kMaxKVItem);
  const int read_size = kMaxKVItem * sizeof(struct disk_index);
  int ret = read(fd, all_.data(), read_size);
  if (ret != read_size) {
    DEBUG << "[WARN] read file size = " << ret << " , " << file_name << " failed\n";
  }
  close(fd);
  return true;
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
  engine_race->all_index_file_ = name + "/ALL";
  // create the dir.
  if (!FileExists(name)) {
    if (mkdir(name.c_str(), 0755)) {
      DEBUG << "mkdir " << name << " failed "  << std::endl;
      return kIOError;
    }
  }
  // create index/data dir.
  std::string index_dir = engine_race->file_name_ + kMetaDirName;
  mkdir(index_dir.c_str(), 0755);
  std::string data_dir = engine_race->file_name_ + kDataDirName;
  mkdir(data_dir.c_str(), 0755);

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

void EngineRace::BuildHashTable(bool is_hash) {
  hash_.Init(is_hash);

  std::vector<std::thread> thread_list(kMaxThreadNumber << 1);
  thread_list.clear();

  spinlock *shard_locks = is_hash ?  new spinlock[kMaxBucketSize] : nullptr;
  std::vector<std::string> idx_dirs(kMaxThreadNumber);
  std::string full_idx_dir = file_name_ + kMetaDirName;

  if (is_hash) {
    DEBUG << "begin to build the hash index.\n";

    idx_dirs.clear();
    if (0 != GetDirFiles(full_idx_dir, &idx_dirs)) {
      DEBUG << "call GetDirFiles() failed: " << full_idx_dir << std::endl;
    }

    auto init_hash_per_thread = [&](const std::string &fn) {
      // open the folder.
      auto file_name = full_idx_dir + "/" + fn + "/0";
      auto fd = open(file_name.c_str(), O_RDONLY | O_NOATIME, 0644);
      if (fd < 0) {
        DEBUG << "open " << file_name << "failed\n";
      }
      struct disk_index di;
      while (read(fd, &di, sizeof(disk_index)) == sizeof(struct disk_index)) {
        if (di.key == 0 && di.file_no == 0 && di.file_offset == 0) {
          break;
        }
        const char *k = reinterpret_cast<const char*>(&(di.key));
        assert(shard_locks);
        hash_.SetNoLock(k, di.file_no, di.file_offset, shard_locks);
      }
      close(fd);
    };

    for (auto &idx_dir: idx_dirs) {
      thread_list.emplace_back(std::thread(init_hash_per_thread, idx_dir));
    }
  } else {
    hash_.Load(AllIndexFile());
  }

  // then open all the data_fds_;
  if (data_fds_.empty()) {
    std::vector<std::string> data_dirs(kMaxThreadNumber);
    data_dirs.clear();
    std::string full_data_dir = file_name_ + kDataDirName;
    if (0 != GetDirFiles(full_data_dir, &data_dirs)) {
      DEBUG << "call GetDirFiles() failed: " << full_data_dir << std::endl;
    }
    data_fds_.resize(data_dirs.size() + 1);

    auto deal_single_data_dir = [&](const std::string &dn) {
      std::vector<std::string> files(kMaxThreadNumber);
      files.clear();
      std::string full_data_dir = file_name_ + kDataDirName;
      std::string sub_dir_name = full_data_dir + "/" + dn;
      if (0 != GetDirFiles(sub_dir_name, &files)) {
        DEBUG << "call GetDirFiles() failed: " << sub_dir_name << std::endl;
      }
      int x = atoi(dn.c_str());
      data_fds_[x].resize(files.size()+1);
      for (auto &f: files) {
        auto file_name = sub_dir_name + "/" + f;
        auto fd = open(file_name.c_str(), O_RDONLY|O_DIRECT | O_NOATIME, 0644);
        if (fd < 0) {
          DEBUG << "can not open file " << file_name << std::endl;
          return;
        }
        data_fds_[x][atoi(f.c_str())] = fd;
      }
    };

    for (auto &d: data_dirs) {
      thread_list.emplace_back(std::thread(deal_single_data_dir, d));
    }
  }
  for (auto &v: thread_list) {
    v.join();
  }

  if (is_hash && shard_locks) {
    delete [] shard_locks;
  }
}

EngineRace::~EngineRace() {
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  for (auto &v: data_fds_) {
    for (auto &x: v) {
      if (x > 0) {
        close(x);
      }
    }
  }

  if (stage_ == kReadStage) {
    // save all the kv into single file.
    hash_.Save(AllIndexFile());
  }

  end_ = std::chrono::system_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - begin_);
  std::cout << "Total Time " << diff.count() / kNanoToMS << " (micro second)" << std::endl;
}


RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  struct fd_wrapper {
    int fd = -1;
    ~fd_wrapper() {
      if (-1 != fd) {
        fdatasync(fd);
        close(fd);
      }
    }
  };
  static thread_local int m_thread_id = 0xffff;
  static thread_local int data_no = 0;
  static thread_local char path[64];
  static thread_local struct fd_wrapper index_fw;
  static thread_local struct fd_wrapper data_fw;
  static thread_local struct disk_index di;

  di.key = toInt(key);
  if (m_thread_id == 0xffff) {
    stage_ = kWriteStage; // 0 stands for write.
    auto thread_pid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    m_thread_id = thread_id_++;
    CPU_SET(m_thread_id % max_cpu_cnt_, &cpuset);
    pthread_setaffinity_np(thread_pid, sizeof(cpu_set_t), &cpuset);

    // need to create the thread dir.
    sprintf(path, "%sindex/%d", file_name_.c_str(), m_thread_id);
    mkdir(path, 0755);
    sprintf(path, "%sdata/%d", file_name_.c_str(), m_thread_id);
    mkdir(path, 0755);

    // TODO: this need to find out the last index of index file and data file.
    // in real project.
    sprintf(path, "%sindex/%d/%d", file_name_.c_str(), m_thread_id, 0/*index_id*/);
    index_fw.fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK | O_NOATIME , 0644);
    posix_fallocate(index_fw.fd, 0, kMaxIndexFileSize);

    data_no++;
    sprintf(path, "%sdata/%d/%d", file_name_.c_str(), m_thread_id, data_no);
    data_fw.fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK | O_NOATIME, 0644);
    posix_fallocate(data_fw.fd, 0, kMaxDataFileSize);
    di.file_no = (m_thread_id<<16) | data_no;
  }

  if((di.file_offset + kPageSize) > kMaxDataFileSize) {
    data_no++;
    close(data_fw.fd);
    sprintf(path, "%sdata/%d/%d", file_name_.c_str(), m_thread_id, data_no);
    data_fw.fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK | O_NOATIME, 0644);
    posix_fallocate(data_fw.fd, 0, kMaxDataFileSize);
    di.file_offset = 0;
    di.file_no = (m_thread_id<<16) | data_no;
  }

  // begin to write the index.
  if (write(index_fw.fd, &di, sizeof(struct disk_index)) != sizeof(struct disk_index)) {
    return kIOError;
  }
  if (write(data_fw.fd, value.ToString().c_str(), kPageSize) != kPageSize) {
    return kIOError;
  }
  di.file_offset += kPageSize;
  return kSucc;
}

// for 64 read threads, it would take 64MB as cache read.
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  // lasy init of hash table.
  // init the read map.
  static std::once_flag init_mptr;
  std::call_once (init_mptr, [this] {
    stage_ = kReadStage; // Read
    BEGIN_POINT(begin_build_hash_table);
    BuildHashTable(true);
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
  uint64_t k64 = toInt(key);
  auto ret = hash_.GetNoLock((char*)(&k64), &file_no, &file_offset);
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

void EngineRace::RangeEntry() {
  // read 1024 kv at a time.
  // every item alread have buffer.
  constexpr int aio_size = 1024;
  struct aio_env_single read_aio[aio_size];

  int cnt = 0;
  std::thread thd_exit([&]{
    std::this_thread::sleep_for(std::chrono::seconds(300));
    DEBUG << "cnt = " << cnt << std::endl;
    exit(-1);
  });
  thd_exit.detach();
  DEBUG << "start range entry\n";

  std::vector<visitor_item*> vs;
  while (!stop_) {
    q_.Pop(&vs);

    DEBUG << "get vs size = " << vs.size() << std::endl;
    auto &all = hash_.GetAll();
    for (auto &v: vs) {
      DEBUG << "range: " << (v->start == all.begin())
            << " , " << (v->end == all.end()) << std::endl;
    }
    // get vs size.
    // TODO: assume all the range are in the same range.
    // this is just for the assumption.
    auto start_pos = vs[0]->start;
    auto end_pos = vs[0]->end;
    // read 1024 first.
    int aio_iter = -1;
    for (auto iter = start_pos; iter != end_pos; iter++) {
      if (aio_iter == -1) {
        // read_ahead 1024 item.
        aio_iter = 0;
        for (auto jter = iter; jter != end_pos && aio_iter < aio_size; jter++, aio_iter++) {
          uint32_t file_no = jter->file_no;
          uint32_t file_offset = jter->file_offset;
          int data_dir = file_no >> 16;
          int sub_file_no = file_no & 0xffff;
          int fd = data_fds_[data_dir][sub_file_no];

          read_aio[aio_iter].SetFD(fd);
          read_aio[aio_iter].Prepare(file_offset);
          read_aio[aio_iter].Submit();
        }
        cnt += aio_size;
        aio_iter = 0;
      }

      // begin to wait the related io over.
      read_aio[aio_iter].WaitOver();
      uint64_t k64 = toBack(iter->key);
      PolarString k((char*)(&k64), kMaxKeyLen);
      PolarString v(read_aio[aio_iter].buf, kPageSize);
      for (auto &x: vs) {
        x->vs->Visit(k, v);
      }
      aio_iter++;
      if (aio_iter == aio_size) {
        aio_iter = -1;
      }
    }

    for (auto &v: vs) {
      v->feed_back();
    }
    vs.clear();
  }
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {

  // lasy init of hash table.
  // init the read map.
  static std::once_flag init_range;
  std::call_once (init_range, [this] {
    if (stage_ == kReadStage) {
      q_.SetNoWait();
    }

    BEGIN_POINT(begin_build_hash_table);
    if (!hash_.CopyToAll()) {
      // if copy failed, then need to read from disk.
      // TODO: need to deal with the duplicated keys in 64Mkv
      BuildHashTable(false/*read all index into single vector*/);
    }
    END_POINT(end_build_hash_table, begin_build_hash_table, "build_hash_time");

    BEGIN_POINT(begin_sort_hash_table);
    auto &all = hash_.GetAll();
    std::sort(all.begin(), all.end());
    DEBUG << "all.size = " << all.size() << std::endl;
    END_POINT(end_sort_hash_table, begin_sort_hash_table, "build_sort_time");


    stage_ = kRangeStage; // 2 for range scan.
    DEBUG << "start the thread for range\n";
    std::thread write_thread(&EngineRace::RangeEntry, this);
    write_thread.detach();
  });

  // after sort, then begin to read the content.
  uint64_t start = toInt(lower);
  uint64_t end = toInt(upper);

  bool include_start = false;
  if (lower.empty()) {
    include_start = true;
  }
  bool include_last = false;
  if (upper.empty()) {
    include_last = true;
  }

  auto &all = hash_.GetAll();

  auto start_pos = all.begin();
  // if not the start of begin
  // use binary search find the position.
  if (!include_start) {
    // need to use binary search find the start position.
    start_pos = std::lower_bound(all.begin(), all.end(), start,
      [](const struct disk_index &d, const uint64_t v) -> bool {
        return d.key < v;
      }
    );
  }

  auto end_pos = all.end();
  if (!include_last) {
    end_pos = std::lower_bound(start_pos, all.end(), end,
      [](const disk_index &d, const uint64_t v) -> bool {
        return d.key < v;
      }
    );
  }

  visitor_item vi(start_pos, end_pos, &visitor);
  q_.Push(&vi);
  std::unique_lock<std::mutex> l(vi.lock_);
  vi.cond_.wait(l, [&vi] { return vi.is_done; });
  return kSucc;
}

}  // namespace polar_race

