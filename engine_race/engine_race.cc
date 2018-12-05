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

#include <functional>
#include <queue>
#include <vector>
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
  //return (((((x % 17) * 19 + x % 19) * 23 + x % 23) * 29 + x % 29) * 31 + x % 31);
  return x >> 41;
}

RetCode HashTreeTable::find(std::vector<struct disk_index> &vs,
                            uint64_t key,
                            struct disk_index**ptr) {
  auto pos = std::lower_bound(vs.begin(),
    vs.end(), key, [](const disk_index &a, uint64_t b) {
    return a.get_key() < b;
  });
  // has find.
  if (pos != vs.end() && !(key < pos->get_key())) {
    *ptr = &(*pos);
    return kSucc;
  }
  return kNotFound;
}

RetCode HashTreeTable::GetNoLock(uint64_t key, uint64_t *file_no, uint32_t *file_offset) {
  const uint64_t array_pos = compute_pos(key);
  // then begin to search this array.
  auto &vs = hash_[array_pos];
  struct disk_index *ptr = nullptr;
  auto ret = find(vs, key, &ptr);
  if (ret == kNotFound) {
    return kNotFound;
  }
  *file_no = ptr->get_file_number();
  *file_offset = ptr->get_offset();
  return kSucc;
}

RetCode HashTreeTable::SetNoLock(uint64_t key, uint32_t file_offset, spinlock *ar) {
  const uint64_t array_pos = compute_pos(key);

  if (ar) {
    ar[array_pos].lock();
  }

  auto &vs = hash_[array_pos];
  for (auto &x: vs) {
    if (static_cast<uint64_t>(x.get_key()) == key) {
      x.set_offset(file_offset);
      if (ar) {
        ar[array_pos].unlock();
      }
      return kSucc;
    }
  }

  vs.emplace_back(key, file_offset);
  if (ar) {
    ar[array_pos].unlock();
  }

  return kSucc;
}

void HashTreeTable::Sort() {
  auto sort_range = [this](const size_t begin, const size_t end) {
    for (size_t i = begin; i < end && i < kMaxBucketSize; i++) {
      auto &vs = hash_[i];
      std::sort(vs.begin(), vs.end());
    }
  };

  std::vector<std::thread> thread_list;
  constexpr int segment_size = kMaxBucketSize / kMaxThreadNumber + 2;
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

void HashTreeTable::Save(const char *file_name) {
  BEGIN_POINT(begin_save_index);
  int fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_NOATIME, 0644);
  if (fd < 0 ) {
    DEBUG << "open " << file_name << "failed\n";
    return;
  }
  // constexpr uint64_t write_buffer_size = k256MB;
  // NOTE!! Big Bug
  // if use 256, it can not devided by sizeof (struct disk_index)
  // it would cause read-error.
  // so, need to change to times of sizeof(struct disk_index)
  // Because sizeof(struct disk_index) == 12,
  // So, here uses 240MB
  constexpr uint64_t write_buffer_size = sizeof(struct disk_index) * 1024ull * 1024ull * 20;
  char *write_buffer = GetAlignedBuffer(write_buffer_size);
  if (!write_buffer) {
    DEBUG << "alloc memory for write_buffer failed\n";
    return;
  }
  struct disk_index *di = (struct disk_index*) write_buffer;
  int iter = 0;
  const int total = write_buffer_size / sizeof(struct disk_index);
  int kv_item_cnt = 0;

  for (auto &vs: hash_) {
    kv_item_cnt += vs.size();
    if (!vs.empty()) {
      for (auto &x: vs) {
        if (iter == total) {
          if (write(fd, write_buffer, write_buffer_size) != write_buffer_size) {
            DEBUG << "write file " << file_name << " meet error\n";
            return;
          }
          iter = 0;
        }

        di[iter++] = x;
      }
    }
  }
  DEBUG << "total kv_item_cnt = " << kv_item_cnt << std::endl;
  if (iter) {
    int write_size = iter * sizeof(struct disk_index);
    if (write(fd, write_buffer, write_size) != write_size) {
      DEBUG << "write file = " << file_name << " meet error\n";
      exit(-1);
    }
  }
  free(write_buffer);
  close(fd);
  DEBUG << "save all index to " << file_name << std::endl;
  END_POINT(end_save_index, begin_save_index, "save_all_time_ms");
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

// this is build up just for read.
void EngineRace::init_read() {
  hash_.Init();

  std::vector<std::thread> thread_list(kMaxThreadNumber);
  thread_list.clear();

  spinlock *shard_locks = new spinlock[kMaxBucketSize];
  if (!shard_locks) {
    DEBUG << "new shard_locks failed\n";
    return;
  }

  // just read the index files.
  // NOTE change the dir to test_engine/index/(0~~255)
  const std::string index_dir = file_name_ + kMetaDirName;
  auto init_hash_per_thread = [&](int thread_id) {
    // open the folder.
    char path[64];
    sprintf(path, "%s/%d", index_dir.c_str(), thread_id);
    auto fd = open(path, O_RDONLY | O_NOATIME, 0644);
    if (fd < 0) {
      /*is there is just single thread, open would failed*/;
      return;
    }
    struct disk_index di;
    while (read(fd, &di, sizeof(disk_index)) == sizeof(struct disk_index)) {
      if (!di.is_valid()) {
        break;
      }
      hash_.SetNoLock(di.get_key(), di.get_offset(), shard_locks);
    }
    close(fd);
  };

  for (int i = 0; i < (int)kMaxThreadNumber; i++) {
    thread_list.emplace_back(std::thread(init_hash_per_thread, i));
  }

  open_data_fd_read();

  for (auto &v: thread_list) {
    v.join();
  }

  if (shard_locks) {
    delete [] shard_locks;
  }
}

EngineRace::~EngineRace() {
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  // clean the data file direct IO opened files.
  close_data_fd();

  if (stage_ == kReadStage) {
    // save all the kv into single file.
    hash_.Save(AllIndexFile());
  }

  end_ = std::chrono::system_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - begin_);
  std::cout << "Total Time " << diff.count() / kNanoToMS << " (micro second)" << std::endl;
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  static std::once_flag init_write;
  std::call_once (init_write, [this] {
    stage_ = kWriteStage;
    write_lock_ = new spinlock[kThreadShardNumber];
    if (!write_lock_) {
      DEBUG << "new write_lock_ failed\n";
      exit (-1);
    }
    open_data_fd_write();
  });

  static thread_local int m_thread_id = 0xffff;
  static thread_local char path[kPathLength];
  static thread_local int index_fd = -1;
  static thread_local struct disk_index di;
  static thread_local struct disk_index *mptr = nullptr;
  static thread_local int mptr_iter = 0;

  di.set_key(toInt(key));
  if (m_thread_id == 0xffff) {
    m_thread_id = pin_cpu();
    // every thread has its own index file.
    sprintf(path, "%sindex/%d", file_name_.c_str(), m_thread_id);
    index_fd = open(path, O_RDWR | O_CREAT | O_NOATIME | O_TRUNC, 0644);
    posix_fallocate(index_fd, 0, kMaxIndexFileSize); // 12MB

    auto ptr = mmap(NULL, kMaxIndexFileSize, PROT_READ|PROT_WRITE, MAP_SHARED | MAP_POPULATE, index_fd, 0);
    if (ptr == MAP_FAILED) {
      DEBUG << "map failed for index write\n";
    }
    mptr = reinterpret_cast<struct disk_index*>(ptr);
    memset(mptr, 0, kMaxIndexFileSize); // 12MB
  }

  // because there just 256 files.
  auto data_fd_iter = di.get_file_number();

  write_lock_[data_fd_iter].lock();
  di.set_offset(data_fd_len_[data_fd_iter]);
  if (write(data_fd_[data_fd_iter], value.data(), kPageSize) != kPageSize) {
    return kIOError;
  }
  data_fd_len_[data_fd_iter] += kPageSize;
  write_lock_[data_fd_iter].unlock();

  // pointer operate the ptr.
  mptr[mptr_iter++] = di;
  return kSucc;
}

// for 64 read threads, it would take 64MB as cache read.
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  // lasy init of hash table.
  // init the read map.
  static std::once_flag init_mptr;
  std::call_once (init_mptr, [this] {
    stage_ = kReadStage; // Read
    thread_id_ = 0;
    BEGIN_POINT(begin_build_hash_table);
    init_read();
    END_POINT(end_build_hash_table, begin_build_hash_table, "init_read_time");

    BEGIN_POINT(begin_sort_hash_table);
    hash_.Sort();
    END_POINT(end_sort_hash_table, begin_sort_hash_table, "hash_sort_time");
  });

  static thread_local uint64_t m_thread_id = 0xffff;
  if (m_thread_id == 0xffff) {
    m_thread_id = pin_cpu();
  }

  static thread_local struct aio_env_single read_aio(-1, true/*read*/, true/*buf*/);
  uint64_t data_fd_iter = 0;
  uint32_t file_offset = 0;
  uint64_t k64 = toInt(key);
  auto ret = hash_.GetNoLock(k64, &data_fd_iter, &file_offset);

  if (ret != kSucc) {
    return kNotFound;
  }

  // begin to find the key & pos
  // TODO use non-block read to count the bytes read then copy to value.
  read_aio.SetFD(data_fd_[data_fd_iter]);
  read_aio.Prepare(file_offset);
  read_aio.Submit();
  read_aio.WaitOver();

  value->assign(read_aio.buf, kPageSize);
  return kSucc;
}

RetCode EngineRace::SlowRead(const PolarString &lower, const PolarString &upper, Visitor &visitor) {
  uint64_t start = toInt(lower);
  uint64_t end = toInt(upper);
  bool include_start = lower.empty();
  bool include_end = upper.empty();

  // just read all the index file.
  int file_length = GetFileLength(std::string(AllIndexFile()));
  int fd = open(AllIndexFile(), O_RDONLY | O_NOATIME, 0644);
  if (fd < 0) {
    DEBUG << "open file failed\n";
    return kIOError;
  }

  if (file_length <= 0) {
    return kSucc;
  }

  struct disk_index *all = (struct disk_index*) malloc(file_length);
  if (!all) {
    DEBUG << "malloc disk index failed\n";
    return kIOError;
  }
  if (read(fd, all, file_length) != file_length) {
    DEBUG << "read file meet error\n";
    return kIOError;
  }
  close(fd);

  const int tail = file_length / sizeof(struct disk_index);
  // binary_search find the start pos and end pos;
  auto start_pos = all;
  if (!include_start) {
    start_pos = std::lower_bound(all, all + tail, start,
      [](const struct disk_index &d, const uint64_t v) -> bool {
        return d.get_key() < v;
      }
    );
  }
  auto end_pos = all + tail;
  if (!include_end) {
    end_pos = std::lower_bound(start_pos, all + tail, end,
      [](const disk_index &d, const uint64_t v) -> bool {
        return d.get_key() < v;
      }
    );
  }

  std::string data_path = file_name_ + kDataDirName;
  char path[kPathLength];
  for (auto iter = start_pos; iter != end_pos; iter++) {
      uint64_t file_no = iter->get_file_number();
      uint32_t file_offset = iter->get_offset();
      // NOTE!! file_no should add 1
      // if want to open it directl
      int file_id = file_no;
      sprintf(path, "%s/%d", data_path.c_str(), file_id);
      int fd = open(path, O_RDONLY | O_NOATIME, 0644);
      if (fd < 0) {
        DEBUG << "can not open file " << path << std::endl;
        return kIOError;
      }
      lseek(fd, file_offset, SEEK_SET);
      char *value = GetAlignedBuffer(kPageSize);
      if (read(fd, value, kPageSize) != kPageSize) {
        DEBUG << "read file meet error\n";
        return kIOError;
      }
      uint64_t k64 = toBack(iter->get_key());
      PolarString k((char*)(&k64), kMaxKeyLen);
      PolarString v(value, kPageSize);
      visitor.Visit(k, v);
      close(fd);
      free(value);
  }
  free(all);
  return kSucc;
}

void EngineRace::ReadIndexEntry() {
  // open index file.
  int index_fd = open(AllIndexFile(), O_RDONLY | O_NOATIME | O_DIRECT, 0644);
  auto flen = get_file_length(AllIndexFile());
  int read_pos = 0;

  // NOTE: must be times  of sizeof(struct disk_index);
  // 24MB here.
  // Other values such as 4K can not be deviced by 12, would cause bug.
  constexpr uint64_t k24MB = 1024ull * 1024ull * sizeof(struct disk_index) * 2;
  index_buf_ = GetAlignedBuffer(k24MB);
  struct aio_env_single read_aio(index_fd, true, false);
  while (true) {
    int bytes = 0;
    is_ok_to_read_index();
    // if read to the end.
    // seek back to the header.
    if (read_pos == flen) {
      read_pos = 0;
      buf_size_ = 0;
      ask_to_visit_index();
      continue;
    }
    read_aio.Prepare(read_pos, index_buf_, k24MB);
    read_aio.Submit();
    bytes = read_aio.WaitOver();
    read_pos += bytes;
    buf_size_ = bytes;
    ask_to_visit_index();
  }
}

void EngineRace::ReadDataEntry() {
  open_data_fd_range();
  // get the maxium file length.
  int max_file_length = 0;
  for (uint32_t i = 0; i < kThreadShardNumber; i++) {
    DEBUG << " file[" << i << "] = " << data_fd_len_[i] << std::endl;
    max_file_length = std::max(max_file_length, data_fd_len_[i]);
  }
  DEBUG << "Range: max_file_length = " << max_file_length << std::endl;

  const uint64_t cache_size = max_file_length + kPageSize;
  char *current_buf = GetAlignedBuffer(cache_size);
  char *next_buf = GetAlignedBuffer(cache_size);
  char *has_data_buf = nullptr;
  if (!current_buf || !next_buf) {
    DEBUG << "alloc memory for data buf failed\n";
    exit(-1);
  }

  struct aio_env_single read_aio(-1, true/*read*/, false/*no_buf*/);
  auto read_next_buf = [&](int file_no) {
    DEBUG << " reading file = " << file_no << std::endl;
    read_aio.SetFD(data_fd_[file_no]);
    has_data_buf = next_buf;
    read_aio.Prepare(0, next_buf, data_fd_len_[file_no]);
    read_aio.Submit();
  };
  read_next_buf(0);

  int next_op_file_no = 0;
  while (true) {
    is_ok_to_read_data();
    read_aio.WaitOver();
    DEBUG << "read over = " << next_op_file_no << std::endl;
    std::swap(next_buf, current_buf);
    data_buf_ = has_data_buf;
    ask_to_visit_data();
    next_op_file_no = (next_op_file_no + 1) % kThreadShardNumber;
    read_next_buf(next_op_file_no);
  }
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper, Visitor &visitor) {
  static std::once_flag init_range;
  std::call_once (init_range, [this] {
    thread_id_ = 0;
    // do not wait for multi-input.
    if (stage_ == kReadStage) {
      // save it to all file.
      hash_.Save(AllIndexFile());
    } else {
      // start index-cache thread
      for (uint64_t i = 0; i < kMaxThreadNumber; i++) {
        index_chan_[i].init();
        data_chan_[i].init();
        visit_index_chan_[i].init();
        visit_data_chan_[i].init();
      }

      DEBUG << "start the thread for index\n";
      std::thread thd_index_cache(&EngineRace::ReadIndexEntry, this);
      thd_index_cache.detach();

      // start data-cache thread.
      DEBUG << "start the thread for data\n";
      std::thread thd_index_data(&EngineRace::ReadDataEntry, this);
      thd_index_data.detach();
    }
  });

  // if has read before, use the old index cache.
  if (stage_ == kReadStage) {
    return SlowRead(lower, upper, visitor);
  }

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

  // here must deal with 64 threads.
  // When comes to here, ask the cache-thread to read cache.
  uint64_t open_file_no {0xffffffffffffffffull};
  PolarString k, v;
  k.set_change(); v.set_change();

  while (true) {
    ask_to_read_index(m_thread_id);
    is_ok_to_visit_index(m_thread_id);

    if (!buf_size_) {
      break;
    }

    // scan every item.
    struct disk_index *di = (struct disk_index*) index_buf_;
    const uint64_t total_item = buf_size_ / sizeof(struct disk_index);
    for (uint64_t i = 0; i < total_item; i++) {
      auto &ref = di[i];
      uint64_t file_no = ref.get_file_number();
      uint32_t file_offset = ref.get_offset();
      uint64_t k64 = toBack(ref.get_key());

      if (open_file_no != file_no) {
        if (m_thread_id == 0) {
          DEBUG << "visit: want to read " << file_no << std::endl;
        }
        open_file_no = file_no;
        ask_to_read_data(m_thread_id);
        is_ok_to_visit_data(m_thread_id);
      }
      char *pos = data_buf_ + file_offset;
      k.init((char*)(&k64), kMaxKeyLen);
      v.init(pos, kPageSize);
      visitor.Visit(k, v);
    }
  }

  return kSucc;
}


}  // namespace polar_race

