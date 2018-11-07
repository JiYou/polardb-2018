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
      if (posix_fallocate(engine_race->fd_, 0, kBigFileSize)) {
        DEBUG << "posix_fallocate failed\n";
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

void EngineRace::BuildHashTable() {
  // the file fd has been open.

  // pool of free buffers.
  std::mutex free_buffer_lock;
  std::vector<char*> free_buffers;

  auto get_free_buf = [&]() {
    std::unique_lock<std::mutex> l(free_buffer_lock);
    char *buf = nullptr;
    if (free_buffers.size() > 0) {
      buf = free_buffers.back();
      free_buffers.pop_back();
    } else {
      buf = GetAlignedBuffer(k4MB);
    }
    return buf;
  };

  auto put_free_buf = [&](char *buf) {
    std::unique_lock<std::mutex> l(free_buffer_lock);
    free_buffers.push_back(buf);
  };

  auto release_free_buf = [&]() {
    std::unique_lock<std::mutex> l(free_buffer_lock);
    for (auto &x: free_buffers) {
      free(x);
    }
    free_buffers.clear();
  };

  std::vector<char*> buffers;
  std::atomic<bool> read_over{false};
  std::mutex lock;

  // read 4MB from disk. this is a single read.
  auto get_disk_content = [&](uint64_t offset) {
    char *buf = get_free_buf();
    read_aio_.Clear();
    DEBUG << "JIYOU begin to read AA " << std::endl;
    DEBUG << " addr = " << (((uint64_t)buf) & 4095) << std::endl;
    read_aio_.PrepareRead(offset, buf, k4MB);
    read_aio_.Submit();
    read_aio_.WaitOver();
    return buf;
  };

  max_data_offset_ = kMaxIndexSize;
  max_index_offset_ = 0;
  bool has_find_valid = false;
  auto buf_to_hash = [&](char *buf) {
    // deal with the buffer.
    if (buf) {
      // read the buf content then put them into hash table.
      struct disk_index *array = reinterpret_cast<struct disk_index*>(buf);
      for (uint32_t i = 0; i < k4MB / sizeof(struct disk_index); i++) {
        // puth every index into hash table.
        auto ref = array + i;
        if (ref->valid) {
          uint64_t offset = ref->offset_4k_ << kValueLengthBits;
          hash_.Set(ref->key, offset);
          max_data_offset_ = std::max(max_data_offset_, offset);
          max_index_offset_ += sizeof(struct disk_index);
          has_find_valid = true;
        }
      }
      put_free_buf(buf);
    }
  };

  // disk_read thread:
  // read index from [0 ~ 1GB] area.
  // here contains the checkpoint for
  // kv index.
  auto disk_read = [&]() {
    int index_offset = 0;
    while (!read_over) {
      // Here will spend many time to read content from disk.
      auto buf = get_disk_content(index_offset);
      index_offset += k4MB;
      read_over = !buf[kLastCharIn4MB];

      // try to put the disk into bufers;
      std::unique_lock<std::mutex> l(lock);
      buffers.push_back(buf);
    }
  };

  // insert all the kv-pos into hash table.
  auto insert_hash = [&]() {
    while (!read_over) {
      char *buf = nullptr;
      lock.lock();
      // deal with all the buffer.
      // get the last buffer.
      if (buffers.size() > 0) {
        buf = buffers.back();
        buffers.pop_back();
      }
      lock.unlock();
      buf_to_hash(buf);
    }
    // read over. deal all the left buffers.
    for (auto &x: buffers) {
      buf_to_hash(x);
    }
    // free all the buffer
    // avoid memory leak.
    release_free_buf();
  };

  std::thread thd_disk_read(disk_read);
  std::thread thd_insert_hash(insert_hash);

  thd_disk_read.join();
  thd_insert_hash.join();

  // set the next begin to write position.
  if (has_find_valid) {
    max_data_offset_ += kPageSize;
    max_index_offset_ += sizeof(struct disk_index);
  }

  DEBUG << "index pos: " << max_index_offset_ << std::endl;
  DEBUG << "data pos - 1GB:  " << max_data_offset_ - kMaxIndexSize << std::endl;
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
  vs.clear();

  // the position aligned with 1KB in disk.
  //
  //      delta = less than 1KB
  // |***********************......................|
  // disk_align_1k          |
  //                    max_index_offset_
  //

  // mem_tail is where start to append the disk_index.
  // NOTE: is not the start point of buffer to write to disk.
  uint64_t disk_align_1k = ROUND_DOWN_1KB(max_index_offset_);
  uint64_t mem_tail = 0;
  uint64_t delta = max_index_offset_ - disk_align_1k;

  assert (delta < 1024);
  if (delta) {
    read_aio_.Clear();
    DEBUG << "JIYOU begin to read BB "<< std::endl;
    read_aio_.PrepareRead(disk_align_1k, index_buf_, k1KB /*ROUND_UP_1KB(delta)*/);
    read_aio_.Submit();
    read_aio_.WaitOver();
    mem_tail = delta;
  }

  // at this point.
  // the position aligned with 1KB in disk.
  //index_buf              mem_tail
  // |   delta              |
  // |======================|......................|
  //      delta < 1KB
  // |***********************......................|
  // disk_align_1k          |
  //                    max_index_offset_
  //

  // head of disk index in memory
  struct disk_index *imh = reinterpret_cast<struct disk_index*>(index_buf_);
  // value memory head
  uint64_t *vmh = reinterpret_cast<uint64_t*>(write_data_buf_);

  while (!stop_) {
    write_queue_.Pop(&vs);

    // move to mem_tail
    struct disk_index *di = imh + (mem_tail >> 4);
    // data part just use the head every time.
    uint64_t *to = vmh;

    for (uint32_t i = 0; i < vs.size(); i++) {
      auto &x = vs[i];
      // write_data_buf contains 256KB.
      char *key_buf = const_cast<char*>(x->key->ToString().c_str());
      uint64_t *key = reinterpret_cast<uint64_t*>(key_buf);
      di->SetKey(key);
      di->offset_4k_ = (max_data_offset_ >> kValueLengthBits) + i; // unit is 4KB
      DEBUG << "map -> " << (max_data_offset_ >> kValueLengthBits) + \
                            i - (kMaxIndexSize >> kValueLengthBits) << std::endl;
      di->valid = kValidType;
      di++;

      // copy the 4KB value part.
      char *value = const_cast<char*>(x->value->ToString().c_str());
      uint64_t *from = reinterpret_cast<uint64_t*>(value);
      for (uint32_t i = 0; i < kPageSize / sizeof(uint64_t); i++) {
        *to++ = *from++;
      }
    }

    // at this point.
    // the position aligned with 1KB in disk.
    //index_buf              mem_tail     ctail
    // |   delta              |            |       |<- align_1kb
    // |======================|++++++++++++........|.......
    //      delta = less than 1KB
    // |***********************......................|
    // disk_align_1k          |
    //                    max_index_offset_
    //
    // di maybe not aligned with 1KB.
    // [di, round_up_1kb(di))  set to 0.
    uint32_t ctail = mem_tail + (vs.size() << 4);
    // ROUND_UP 1KB
    uint32_t align_up_1kb = ROUND_UP_1KB(ctail);
    {
      uint64_t *b = (uint64_t*)(index_buf_ + ctail);
      uint64_t *e = (uint64_t*)(index_buf_ + align_up_1kb);
      while (b < e) {
        *b++ = 0;
      }
    }

    // at this point.
    // the position aligned with 1KB in disk.
    //index_buf              mem_tail     ctail
    // |   delta              |            |       |<- align_1kb
    // |======================|++++++++++++00000000|.......
    //      delta = less than 1KB
    // |***********************......................|
    // disk_align_1k          |
    //                    max_index_offset_
    //
    // So: need to write [0, align_1kb)
    // disk to write: [disk_align_1k, size=align_1kb)

    uint32_t data_write_size = vs.size() << 12;

    { // debug info
      DEBUG << "--------\n";
      for (struct disk_index *d = imh; d < (imh + (align_up_1kb>>4)); d++) {
        char *k = ((char*)(d->key));
        DEBUG << "key:";
        for (int i = 0; i < 8; i++) std::cout << d->key[i];
        std::cout << "," << d->offset_4k_ << std::endl;
        if (!(d->valid)) break;
      }
    }
    // DEBUG << "index pos: " << max_index_offset_ << " : " <<  align_up_1kb << std::endl;
    // DEBUG << "idata pos: " << max_data_offset_ - kMaxIndexSize << " : " << data_write_size << std::endl;
    write_aio_.Clear();
    write_aio_.PrepareWrite(disk_align_1k, index_buf_, align_up_1kb);
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

    max_index_offset_ += vs.size() << 4;
    max_data_offset_ += data_write_size;

    // at this point.
    // the position aligned with 1KB in disk.
    //index_buf              mem_tail     ctail
    // |   delta              |            |       |<- align_1kb
    // |======================|++++++++++++00000000|.......
    //
    // |***********************************......................|
    // disk_align_1k      |                |
    //                    |               max_index_offset_
    //                    |
    //                 new_align
    // need to delete new_align - disk_align_1k;
    // to avoid duplicate memory write to disk
    // so, index_buf need to align to new_align

    uint64_t new_align = ROUND_DOWN_1KB(max_index_offset_);
    // index_buf_ need to walk-front to_walk bytes.
    uint64_t to_walk = new_align - disk_align_1k;
    if (to_walk) {
      // just need to copy the length: max_index_offset_ - new_align
      uint32_t move_mem_size = max_index_offset_ - new_align;

      char *from = index_buf_ + to_walk;
      char *to = index_buf_;
      for (uint32_t i = 0; i < move_mem_size; i++) {
        *to++ = *from++;
      }
      mem_tail = ctail - to_walk;
      disk_align_1k = new_align;
    } else {
      mem_tail = ctail;
    }

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
      read_aio_.PrepareRead(x->pos, x->buf, k4MB >> 2, x);
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
      buf = GetAlignedBuffer(k4MB / 4); // just 1 MB for every thread as cache.
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
  static thread_local uint64_t pre_pos = 0;
  static thread_local uint64_t pre_key = 0;

  value->resize(kPageSize);
  // opt 1. hit the pre-key?
  //       . no need to compute the position with hash.
  const uint64_t *new_key = reinterpret_cast<const uint64_t*>(key.ToString().c_str());
  if (has_read && pre_key == *new_key) {
    // just copy the buffer.
    char *tob = const_cast<char*>(value->c_str());
    char *fob = lb.buf;
    engine_memcpy(tob, fob);
    return kSucc;
  }

  // opt 2. hit the nearby position.
  //        compute the position.
  uint64_t offset = 0;
  RetCode ret = hash_.Get(key.ToString().c_str(), &offset);
  if (ret == kNotFound) {
    return ret;
  }
  // find the position. check in the cache?
  bool is_hit = has_read && offset > pre_pos;
  constexpr uint64_t max_cache_item = 256; // k4MB / 4 / kPageSize;
  // NOTE: all the offset is unsigned, so carefull of use minus.
  if (is_hit && (offset - pre_pos) < max_cache_item) {
    char *from_buf = lb.buf + (offset - pre_pos);
    char *tob = const_cast<char*>(value->c_str());
    engine_memcpy(tob, from_buf);
    return kSucc;
  }

  // opt 3. need to read from the disk.
  // NOTE: update the cache.
  // udpate the cache.
  pre_pos = offset;
  pre_key = *new_key;
  has_read = true;

  std::cout << "JIYOU key -> " << *new_key << std::endl;
  std::cout << "JIYOU read-> " << offset << std::endl;

  read_item r(offset, lb.buf);
  read_queue_.Push(&r);

  std::unique_lock<std::mutex> l(r.lock_);
  r.cond_.wait(l, [&r] { return r.is_done; });

  // copy the buffer
  char *new_buf = lb.buf;
  char *target_buf = const_cast<char*>(value->c_str());
  engine_memcpy(target_buf, new_buf);

  return r.ret_code;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

}  // namespace polar_race

