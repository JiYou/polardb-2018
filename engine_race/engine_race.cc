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

void HashTreeTable::Save(const char *file_name) {
  BEGIN_POINT(begin_save_index);
  int fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_NOATIME, 0644);
  if (fd < 0 ) {
    DEBUG << "open " << file_name << "failed\n";
    return;
  }

  struct Node {
    std::vector<struct disk_index>::iterator pos;
    std::vector<struct disk_index>::iterator end;
    // make small heap
    bool operator < (const Node &n) const {
      return pos->key > n.pos->key;
    }
  };

  Node *heap = (Node*) malloc(sizeof(Node) * kMaxBucketSize);
  int iter = 0;
  if (!heap) {
    DEBUG << "malloc memory for heap failed\n";
    return;
  }

  for (auto &vs: hash_) {
    if (!vs.empty()) {
      heap[iter].pos = vs.begin();
      heap[iter].end = vs.end();
      iter++;
    }
  }
  int tail = iter;
  std::make_heap(heap, heap + tail);

  // 256MB cache.
  constexpr uint32_t buffer_size = 16777216ull;
  constexpr uint32_t mem_size = buffer_size * sizeof(struct disk_index);
  struct disk_index *write_buffer = (struct disk_index*) malloc(mem_size);
  int idx = 0;

  while (tail) {
    // pop a item.
    std::pop_heap(heap, heap + tail);
    // get the last item.
    auto &last = heap[--tail];
    write_buffer[idx++] = *(last.pos);
    // if the buffer is full, flush to file.
    if (idx == buffer_size) {
      if (write(fd, write_buffer, mem_size) != mem_size) {
        DEBUG << "write index file meet error\n";
        return;
      }
      idx = 0;
    }

    // find the item, then put it into heap.
    last.pos++;
    if (last.pos != last.end) {
      tail++;
      std::push_heap(heap, heap + tail);
    }
  }

  // write the left item in the buffer.
  if (idx) {
    int write_size = sizeof(struct disk_index) * idx;
    if (write(fd, write_buffer, write_size) != write_size) {
      DEBUG << "write file index meet error\n";
      return;
    }
  }

  close(fd);
  free(write_buffer);
  free(heap);
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

  std::vector<std::string> idx_dirs(kMaxThreadNumber);

  DEBUG << "begin to build the hash index.\n";
  idx_dirs.clear();
  if (0 != GetDirFiles(file_name_ + kMetaDirName, &idx_dirs)) {
    DEBUG << "call GetDirFiles() failed: " << file_name_ + kMetaDirName << std::endl;
  }

  auto init_hash_per_thread = [&](const std::string &fn) {
    // open the folder.
    auto file_name = file_name_ + kMetaDirName + "/" + fn + "/0";
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
      hash_.SetNoLock(k, di.file_no, di.file_offset, shard_locks);
    }
    close(fd);
  };

  for (auto &idx_dir: idx_dirs) {
    thread_list.emplace_back(std::thread(init_hash_per_thread, idx_dir));
  }

  // then open all the data_fds_;
  std::vector<std::string> data_dirs(kMaxThreadNumber);
  data_dirs.clear();

  if (0 != GetDirFiles(file_name_ + kDataDirName, &data_dirs)) {
    DEBUG << "call GetDirFiles() failed: " << file_name_ + kDataDirName<< std::endl;
  }
  data_fds_.resize(data_dirs.size() + 1);

  auto deal_single_data_dir = [&](const std::string &dn) {
    std::vector<std::string> files(kMaxThreadNumber);
    files.clear();
    std::string sub_dir_name = file_name_ + kDataDirName + "/" + dn;
    if (0 != GetDirFiles(sub_dir_name, &files)) {
      DEBUG << "call GetDirFiles() failed: " << sub_dir_name << std::endl;
    }
    int x = atoi(dn.c_str());
    data_fds_[x].resize(files.size()+1);
    for (auto &f: files) {
      auto file_name = sub_dir_name + "/" + f;
      auto fd = open(file_name.c_str(), O_RDONLY | O_DIRECT | O_NOATIME, 0644);
      if (fd < 0) {
        DEBUG << "can not open file " << file_name << std::endl;
        return;
      }
      data_fds_[x][atoi(f.c_str())-1] = fd;
    }
  };

  for (auto &d: data_dirs) {
    thread_list.emplace_back(std::thread(deal_single_data_dir, d));
  }
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

  // read stage close all data fds.
  for (auto &v: data_fds_) {
    for (auto &x: v) {
      if (x > 0) {
        close(x);
      }
    }
  }
  data_fds_.clear();

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
  // open 256 files.
  struct shard_wrapper {
    int *fds = nullptr;
    uint32_t *offset = nullptr;
    shard_wrapper() {
      fds = (int*) malloc(sizeof(int) * kThreadShardNumber);
      if (!fds) {
        DEBUG << "malloc memory for shard_fds failed\n";
      }
      offset = (uint32_t*) malloc(sizeof(uint32_t) * kThreadShardNumber);
      if (!offset) {
        DEBUG << "malloc memory for offset failed\n";
      }
      memset(offset, 0, sizeof(uint32_t) * kThreadShardNumber);
    }

    void init(char *dir) {
      // open all the related files.
      char path[64];
      for (uint32_t i = 0; i < kThreadShardNumber; i++) {
        sprintf(path, "%s/%d", dir, i+1);
        int fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK | O_NOATIME, 0644);
        if (fd < 0) {
          DEBUG << "open path = " << path << "failed\n";
          return;
        }
        fds[i] = fd;
      }
    }

    ~shard_wrapper() {
      for (uint32_t i = 0; i < kThreadShardNumber; i++) {
        if (fds[i] > 0) {
          close(fds[i]);
        }
      }
      free(fds);
    }
  };

  static thread_local int m_thread_id = 0xffff;
  static thread_local char path[64];
  static thread_local struct fd_wrapper index_fw;
  static thread_local struct fd_wrapper data_fw;
  static thread_local struct disk_index di;
  static thread_local struct shard_wrapper data_fd;

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
    data_fd.init(path);

    // TODO: this need to find out the last index of index file and data file.
    // in real project.
    sprintf(path, "%sindex/%d/%d", file_name_.c_str(), m_thread_id, 0/*index_id*/);
    index_fw.fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK | O_NOATIME , 0644);
    posix_fallocate(index_fw.fd, 0, kMaxIndexFileSize);
  }

  // because there just 256 shards.
  auto idx = di.key >> 56;
  di.set_file_no(m_thread_id, idx);
  di.file_offset = data_fd.offset[idx];
  if (write(data_fd.fds[idx], value.data(), kPageSize) != kPageSize) {
    return kIOError;
  }
  // begin to write the index.
  if (write(index_fw.fd, &di, sizeof(struct disk_index)) != sizeof(struct disk_index)) {
    return kIOError;
  }
  data_fd.offset[idx] += kPageSize;
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
    init_read();
    END_POINT(end_build_hash_table, begin_build_hash_table, "init_read_time");

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
        return d.key < v;
      }
    );
  }
  auto end_pos = all + tail;
  if (!include_end) {
    end_pos = std::lower_bound(start_pos, all + tail, end,
      [](const disk_index &d, const uint64_t v) -> bool {
        return d.key < v;
      }
    );
  }

  std::string data_path = file_name_ + kDataDirName;
  char path[64];
  for (auto iter = start_pos; iter != end_pos; iter++) {
      uint32_t file_no = iter->file_no;
      uint32_t file_offset = iter->file_offset;
      int data_dir = file_no >> 16;
      int sub_file_no = file_no & 0xffff;
      // NOTE!! file_no should add 1
      // if want to open it directl
      sprintf(path, "%s/%d/%d", data_path.c_str(), data_dir, sub_file_no + 1);
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
      uint64_t k64 = toBack(iter->key);
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
  int index_fd = open(AllIndexFile(), O_RDONLY | O_NONBLOCK | O_NOATIME | O_DIRECT, 0644);
  auto flen = get_file_length(AllIndexFile());
  int read_pos = 0;
  index_buf_ = GetAlignedBuffer(k256MB << 1);
  while (true) {
    int bytes = 0;
    is_ok_to_read_index();
    // if read to the end.
    // seek back to the header.
    if (read_pos == flen) {
      lseek(index_fd, 0, SEEK_SET);
      read_pos = 0;
      buf_size_ = 0;
      ask_to_visit_index();
      continue;
    }
    bytes = read_file(index_fd, index_buf_, k256MB << 1);
    read_pos += bytes;
    buf_size_ = bytes;
    ask_to_visit_index();
  }
}

void EngineRace::ReadDataEntry() {
  total_cache_ = GetAlignedBuffer(k256MB*5);
  int next_op_file_no = 0;
  const std::string data_path = file_name_ + kDataDirName;
  char path[64];
  while (true) {
    char *head = total_cache_;
    uint64_t max_size = k256MB * 5;
    is_ok_to_read_data();
    next_op_file_no = (next_op_file_no + 1) % (kThreadShardNumber + 1);
    if (!next_op_file_no) {
      next_op_file_no = 1;
    }
    for (int i = 0; i < kMaxThreadNumber; i++) {
      sprintf(path, "%s/%d/%d", data_path.c_str(), i, next_op_file_no);
      int fd = open(path, O_RDONLY | O_NOATIME | O_DIRECT | O_NONBLOCK, 0644);
      data_buf_[i] = head;
      auto bytes = read_file(fd, data_buf_[i], max_size);
      close(fd);
      head += bytes;
      max_size -= bytes;
    }
    ask_to_visit_data();
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
  int open_file_no = -1;
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
      uint32_t file_no = ref.file_no;
      uint32_t file_offset = ref.file_offset;

      int data_dir = file_no >> 16;
      int sub_file_no = (file_no & 0xffff);
      uint64_t k64 = toBack(ref.key);

      if (open_file_no != sub_file_no) {
        open_file_no = sub_file_no;
        ask_to_read_data(m_thread_id);
        is_ok_to_visit_data(m_thread_id);
      }
      char *pos = data_buf_[data_dir] + file_offset;
      PolarString k((char*)(&k64), kMaxKeyLen);
      PolarString v(pos, kPageSize);
      k.set_change(); v.set_change();
      visitor.Visit(k, v);
    }
  }

  return kSucc;
}


}  // namespace polar_race

