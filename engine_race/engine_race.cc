// Copyright [2018] Alibaba Cloud All rights reserved

#include "engine_race/util.h"
#include "engine_race/engine_race.h"
#include "engine_race/libaio.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <map>

#include <stdio.h>

namespace polar_race {

static const char kLockFile[] = "LOCK";

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);

  RetCode ret = engine_race->plate_.Init();
  if (ret != kSucc) {
    delete engine_race;
    return ret;
  }
  ret = engine_race->store_.Init();
  if (ret != kSucc) {
    delete engine_race;
    return ret;
  }

  if (0 != LockFile(name + "/" + kLockFile, &(engine_race->db_lock_))) {
    delete engine_race;
    DEBUG << "Generate LOCK file failed" << std::endl;
    return kIOError;
  }

  engine_race->start();
  *eptr = engine_race;

  return kSucc;
}

EngineRace::~EngineRace() {
  stop_ = true;
  if (db_lock_) {
    UnlockFile(db_lock_);
  }
}

void EngineRace::WriteEntry() {
  std::vector<write_item*> vs(64, nullptr);
  DEBUG << "db::WriteEntry()" << std::endl;
  while (!stop_) {
    write_queue_.Pop(&vs);
    // firstly, write all the content into file.
    // write all the data.
    for (auto &x: vs) {
      Location location;
      RetCode ret = store_.Append((x->value)->ToString(), &location);
      if (ret == kSucc) {
        ret = plate_.Append((x->key)->ToString(), location);
      }
      x->ret_code = kSucc;
    }

    // sync to disk.
    RetCode ret = store_.Sync();
    if (ret == kSucc) {
      ret = plate_.Sync();
    }

    // if sync failed, change the ret code
    // when previous set success.
    for (auto &x: vs) {
      if (ret != kSucc && x->ret_code == kSucc) {
        x->ret_code = ret;
      }
      {
        std::unique_lock<std::mutex> l(x->lock_);
        x->is_done = true;
        x->cond_.notify_all();
      }
    }
  }
}

#ifdef READ_QUEUE
void EngineRace::ReadEntry() {
  std::vector<read_item*> to_read(64, nullptr);
  std::vector<Location> file_pos(64);
  DEBUG << "db::ReadEntry()" << std::endl;

  to_read.resize(0);
  file_pos.resize(0);

  while (!stop_) {
    read_queue_.Pop(&to_read);
    // get all the request.
    // then begin to find the location.
    // NOTE: here maybe some items can not be found.
    size_t to_index = 0;
    file_pos.resize(to_read.size());
    for (size_t i = 0; i < to_read.size(); i++) {
      auto &x = to_read[i];
      auto ret = plate_.Find((x->key)->ToString(), &file_pos[i]);
      if (ret == kNotFound) {
        std::unique_lock<std::mutex> l(x->lock_);
        x->is_done = true;
        x->cond_.notify_all();
      } else {
        if (to_index != i) {
          to_read[to_index] = x;
          file_pos[to_index] = file_pos[i];
        }
        // if is the index == i, skip copy.
        to_index++;
      }
    }

    // batch job submit to data-> get value.
    // TODO: add interface in data for batch update.
    to_read.resize(to_index);
    file_pos.resize(to_index);
    // call the release x->lock in AIO event.
    store_.BatchRead(to_read, file_pos);
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
  if (key.size() != kMaxKeyLen || value.size() != kMaxValueLen) {
    // check the key size.
    static bool have_find_larger_key = false;
    if (!have_find_larger_key) {
      DEBUG << "[WARN] have find key size = " << key.size() << ":" << " value size = " << value.size() << std::endl;
      have_find_larger_key = true;
    }
  }

  write_item w(&key, &value);
  write_queue_.Push(&w);

  // wait the request writen to disk.
  std::unique_lock<std::mutex> l(w.lock_);
  w.cond_.wait(l, [&w] { return w.is_done; });
  return w.ret_code;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  Location location;
  RetCode ret = plate_.Find(key.ToString(), &location);
  if (ret == kSucc) {
    value->clear();
    ret = store_.Read(location, value);
  }
  return ret;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  std::map<std::string, Location> locations;
  RetCode ret =  plate_.GetRangeLocation(lower.ToString(), upper.ToString(), &locations);
  if (ret != kSucc) {
    return ret;
  }

  std::string value;
  for (auto& pair : locations) {
    ret = store_.Read(pair.second, &value);
    if (kSucc != ret) {
      break;
    }
    visitor.Visit(pair.first, value);
  }
  return ret;
}

}  // namespace polar_race

