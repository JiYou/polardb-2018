// Copyright [2018] Alibaba Cloud All rights reserved

#include "util.h"
#include "engine_race.h"
#include "engine_aio.h"

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
  return x >> 48;
}

RetCode HashTreeTable::find(uint64_t key, struct disk_index**ptr) {
  auto bucket_id = compute_pos(key);
  auto &first = bucket_start_pos_[bucket_id];
  auto &last = bucket_iter_[bucket_id];
  auto pos = std::lower_bound(
    first, last,
    key, [](const disk_index &a, uint64_t b) {
    return a.get_key() < b;
  });
  // has find.
  if (pos != last && !(key < pos->get_key())) {
    *ptr = &(*pos);
    return kSucc;
  }
  return kNotFound;
}

RetCode HashTreeTable::GetNoLock(uint64_t key, uint64_t *file_no, uint32_t *file_offset) {
  struct disk_index *ptr = nullptr;
  auto ret = find(key, &ptr);
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

  auto &last = bucket_iter_[array_pos];
  if (remove_dup_) {
    // try to find it, if find, then update the item..
    auto &first = bucket_start_pos_[array_pos];
    for (auto x = first; x < last; x++) {
      if (static_cast<uint64_t>(x->get_key()) == key) {
        x->set_offset(file_offset);
       if (ar) {
          ar[array_pos].unlock();
        }
        return kSucc;
      }
    }
  }
  // if not found, then just append.
  last->set_key(key);
  last->set_offset(file_offset);
  last++;

  if (ar) {
    ar[array_pos].unlock();
  }

  return kSucc;
}

// if the mem_size is not aligned with kPageSize
// use this to save the file.
void HashTreeTable::CacheSave(const char *file_name) {
  // Here juse O_NONBLOCK to write the content.
  all_index_fd_ = open(file_name, O_WRONLY | O_TRUNC | O_CREAT | O_NOATIME | O_NONBLOCK, 0644);
  if (all_index_fd_ < 0) {
    DEBUG << "open " << file_name << " meet error\n";
    exit(-1);
  }

  // note: if all threads contains duplicated items
  // after the item has been -covered,
  // there maybe some holes in hash_
  // so, need to write the item one by one.
  for (uint32_t i = 0; i < kMaxBucketSize; i++) {
    auto &first = bucket_start_pos_[i];
    auto &last = bucket_iter_[i];

    if (last == first) {
      continue;
    }

    int write_size = (last - first) * sizeof(struct disk_index);
    if (write(all_index_fd_, first, write_size) != write_size) {
      DEBUG << "write file meet error\n";
      exit(-1);
    }
  }
  // close the file handler in WaitWriteOver.
}

void HashTreeTable::AioSaveInit(const char *file_name) {
  all_index_fd_ = open(file_name, O_DIRECT | O_WRONLY | O_TRUNC | O_NOATIME | O_CREAT, 0644);
  posix_fallocate(all_index_fd_, 0, mem_size());
  if (all_index_fd_ < 0) {
    DEBUG << "open " << file_name << " meet error\n";
    exit (-1);
  }
  write_aio_ = new aio_env_single(all_index_fd_, false/*write*/, false/*no_buf*/);
}

void HashTreeTable::AioSaveSubmit() {
    // after sort, then raise write process.
    write_aio_->Prepare(0, (char*)hash_, mem_size());
    write_aio_->Submit();
    // NOTE: wait_over would in destructor func.
}

void HashTreeTable::Sort(const char *file_name) {
  auto sort_range = [this](const size_t begin, const size_t end) {
    for (size_t i = begin; i < end && i < kMaxBucketSize; i++) {
      auto first = bucket_start_pos_[i];
      auto last = bucket_iter_[i];
      std::sort(first, last);
    }
  };

  std::vector<std::thread> thread_list;
  constexpr int segment_size = kMaxBucketSize / kMaxThreadNumber +
        (kMaxBucketSize % kMaxThreadNumber ? 1 : 0);
  for (int i = 0; i < kMaxThreadNumber; i++) {
    const size_t begin = i * segment_size;
    const size_t end = (i + 1) * segment_size;
    thread_list.emplace_back(std::thread(sort_range, begin, end));
  }

  bool use_aio = true;
  if (remove_dup_) {
    CacheSave(file_name);
    use_aio = false;
  } else {
    AioSaveInit(file_name);
  }

  for (auto &x: thread_list) {
    x.join();
  }

  if (use_aio) {
    AioSaveSubmit();
  }
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
  printf("[DB_DIR] = %s\n", name.c_str());

  engine_race->file_name_ = name + "/";
  engine_race->all_index_file_ = name + "/ALL";
  mkdir(name.c_str(), 0755);

  // create index/data dir.
  std::string index_dir = engine_race->file_name_ + kMetaDirName;
  mkdir(index_dir.c_str(), 0755);
  std::string data_dir = engine_race->file_name_ + kDataDirName;
  mkdir(data_dir.c_str(), 0755);

  if (0 != LockFile(name + "/" + kLockFile, &(engine_race->db_lock_))) {
    meet_error = true;
    DEBUG << "Generate LOCK file failed" << std::endl;
  }

  engine_race->max_cpu_cnt_ = std::thread::hardware_concurrency();
  *eptr = engine_race;

  return kSucc;
}

// this is build up just for read.
void EngineRace::init_read() {
  // alloc memory for file cache.
  open_data_fd_read();
  // find out the biggest file size.
  // for build hash_map
  // At least there ould be 960MB for cache.
  // because read all the hash file would take
  // 12 * 64 ~= 768MB and other for temporiy useage.
  int max_length_file_iter = 0;
  int max_cache_size = max_data_file_length(&max_length_file_iter) + kPageSize;
  constexpr int min_cache_size {1006632960};
  max_cache_size = std::max(max_cache_size, min_cache_size);
  file_cache_for_read_ = GetAlignedBuffer(max_cache_size);
  if (!file_cache_for_read_) {
    DEBUG << "malloc memory for read stage_ data file cache failed\n";
    exit(-1);
  }

  // read all the file out and into file buffer.
  int *all_index_fd_array = (int*)(file_cache_for_read_ +\
      kMaxThreadNumber * kMaxIndexFileSize + kPageSize);
  struct aio_env_range<kMaxThreadNumber> *read_aio = \
      new (all_index_fd_array+kMaxThreadNumber) aio_env_range<kMaxThreadNumber>();
  read_aio->Clear();
  const std::string index_dir = file_name_ + kMetaDirName;
  char path[kPathLength];
  for (int i = 0; i < (int)kMaxThreadNumber; i++) {
    sprintf(path, "%s/%d", index_dir.c_str(), i);
    auto fd = open(path, O_RDONLY | O_NOATIME | O_DIRECT, 0644);
    if (fd < 0) {
      /*is there is just single thread, skip this failed*/;
      continue;
    }
    all_index_fd_array[i] = fd;
    char *buf = file_cache_for_read_ + i * kMaxIndexFileSize;
    read_aio->PrepareRead(fd, 0, buf, kMaxIndexFileSize);
  }
  read_aio->Submit();
  read_aio->WaitOver();

  // after all is read over.
  for (int i = 0; i < kMaxThreadNumber; i++) {
    if (all_index_fd_array[i] > 0) {
      close(all_index_fd_array[i]);
    }
  }
  read_aio->~aio_env_range<kMaxThreadNumber>();
  all_index_fd_array = nullptr;
  read_aio = nullptr;

  // then begin to build the hash_bucket_counter_
  // this is the header for all the 64 threads
  // counter.
  uint32_t *hash_bucket_counter_header = (uint32_t*)
          (file_cache_for_read_ + kMaxThreadNumber * kMaxIndexFileSize);

  auto bucket_counter_func = [&](int thread_id, uint32_t *counter) {
    memset(counter, 0, kMaxBucketSize * sizeof(uint32_t));
    char *start = file_cache_for_read_ + thread_id * kMaxIndexFileSize;
    char *end = start + kMaxIndexFileSize;
    struct disk_index *ar = (struct disk_index*) start;
    while (ar < (struct disk_index*)end) {
      if (!(ar->is_valid())) {
        break;
      }
      auto shard_id = hash_.Shard(ar->get_key());
      counter[shard_id]++;
      ar++;
    }
  };

  // use 64 threads to build the counter.
  std::vector<std::thread> thread_list(kMaxThreadNumber);
  thread_list.clear();

  for (uint32_t i = 0; i < kMaxThreadNumber; i++) {
    uint32_t *counter = hash_bucket_counter_header + i * kMaxBucketSize * sizeof(uint32_t);
    thread_list.emplace_back(std::thread(bucket_counter_func, i, counter));
  }
  for (auto &x: thread_list) {
    x.join();
  }
  thread_list.clear();

  // then sum all the counter to one.
  uint32_t *sum_counter = hash_bucket_counter_header;
  for (uint32_t i = 1; i < kMaxThreadNumber; i++) {
    uint32_t *counter = hash_bucket_counter_header + i * kMaxBucketSize * sizeof(uint32_t);
    for (uint32_t j = 0; j < kMaxBucketSize; j++) {
      sum_counter[j] += counter[j];
    }
  }

  hash_.Init(sum_counter);

  spinlock *shard_locks = new spinlock[kMaxBucketSize];
  if (!shard_locks) {
    DEBUG << "new shard_locks failed\n";
    return;
  }

  // use 64 threads to build the hash table.
  auto init_hash_per_thread = [&](int thread_id) {
    char *start = file_cache_for_read_ + thread_id * kMaxIndexFileSize;
    char *end = start + kMaxIndexFileSize;
    struct disk_index *ar = (struct disk_index*) start;
    while (ar < (struct disk_index*)end) {
      if (!(ar->is_valid())) {
        break;
      }
      hash_.SetNoLock(ar->get_key(), ar->get_offset(), shard_locks);
      ar++;
    }
  };

  for (int i = 0; i < (int)kMaxThreadNumber; i++) {
    thread_list.emplace_back(std::thread(init_hash_per_thread, i));
  }

  for (auto &v: thread_list) {
    v.join();
  }

  // to here, all the memory based on file_cache_for_read_
  // would free to use as cache.
  struct aio_env_single max_file_aio(data_fd_[max_length_file_iter], true, false);
  max_file_aio.Prepare(0, file_cache_for_read_, max_cache_size);
  max_file_aio.Submit();

  // do some job here.
  if (shard_locks) {
    delete [] shard_locks;
  }
  max_file_aio.WaitOver();
  cached_file_iter_ = max_length_file_iter;
}

EngineRace::~EngineRace() {
  if (db_lock_) {
    UnlockFile(db_lock_);
  }

  // clean the data file direct IO opened files.
  close_data_fd();

  // free read memory
  if (file_cache_for_read_) {
    free(file_cache_for_read_);
  }

  end_ = std::chrono::system_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - begin_);
  std::cout << "Total Time " << diff.count() / kNanoToMS << " (micro second)" << std::endl;
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  static std::once_flag init_write;
  std::call_once (init_write, [this] {
    stage_ = kWriteStage;
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
  }

  // because there just 256 files.
  auto data_fd_iter = di.get_file_number();
  uint32_t offset = data_fd_write_len_[data_fd_iter]++;
  di.set_offset(offset << 12);
  if (pwrite(data_fd_[data_fd_iter], value.data(), kPageSize, offset << 12) != kPageSize) {
    return kIOError;
  }

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
    hash_.Sort(AllIndexFile());
    END_POINT(end_sort_hash_table, begin_sort_hash_table, "sort_time");
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

  if (data_fd_iter == cached_file_iter_ && file_cache_for_read_) {
    char *buf = file_cache_for_read_ + file_offset;
    value->assign(buf, kPageSize);
    return kSucc;
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
  constexpr uint64_t kReadIndexMaxSize = 1024ull * 1024ull * sizeof(struct disk_index) * 2 * 3; // 72MB
  index_buf_ = GetAlignedBuffer(kReadIndexMaxSize);
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
    read_aio.Prepare(read_pos, index_buf_, kReadIndexMaxSize);
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
  int max_file_length = max_data_file_length();

  const uint64_t cache_size = max_file_length + kPageSize;
  char *current_buf = nullptr;
  if (file_cache_for_read_) {
    current_buf = file_cache_for_read_;
  } else {
    current_buf = GetAlignedBuffer(cache_size);
  }
  char *next_buf = GetAlignedBuffer(cache_size);
  char *has_data_buf = nullptr;
  if (!current_buf || !next_buf) {
    DEBUG << "alloc memory for data buf failed\n";
    exit(-1);
  }

  struct aio_env_single read_aio(-1, true/*read*/, false/*no_buf*/);
  auto read_next_buf = [&](int file_no) {
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
      hash_.WaitWriteOver();
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
    m_thread_id = pin_cpu();
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

