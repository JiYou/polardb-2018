// Copyright [2018] Alibaba Cloud All rights reserved

#include "engine_race/util.h"
#include "engine_race/engine_race.h"

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

void EngineRace::run() {
  int64_t cnt = 0;
  std::vector<write_item*> vs;
  DEBUG << "db::run()" << std::endl;
  while (!stop_) {
    q_.Pop(&vs);
    cnt ++;

    if (cnt % 1000 == 0)
      std::cout << "vs.size() = " << vs.size() << std::endl;

    // firstly, write all the content into file.
    pthread_mutex_lock(&mu_);

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
    pthread_mutex_unlock(&mu_);
  }
}

void EngineRace::start() {
  std::thread thd(&EngineRace::run, this);
  thd.detach();
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  if (key.size() != 8 || value.size() != 4096) {
    // check the key size.
    static bool have_find_larger_key = false;
    if (!have_find_larger_key) {
      DEBUG << "[WARN] have find key size = " << key.size() << ":" << " value size = " << value.size() << std::endl;
      have_find_larger_key = true;
    }
  }

  write_item w(&key, &value);
  q_.Push(&w);

  // wait the request writen to disk.
  std::unique_lock<std::mutex> l(w.lock_);
  w.cond_.wait(l, [&w] { return w.is_done; });
  return w.ret_code;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  pthread_mutex_lock(&mu_);
  Location location;
  RetCode ret = plate_.Find(key.ToString(), &location);
  if (ret == kSucc) {
    value->clear();
    ret = store_.Read(location, value);
  }
  pthread_mutex_unlock(&mu_);
  return ret;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  pthread_mutex_lock(&mu_);
  std::map<std::string, Location> locations;
  RetCode ret =  plate_.GetRangeLocation(lower.ToString(), upper.ToString(), &locations);
  if (ret != kSucc) {
    pthread_mutex_unlock(&mu_);
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
  pthread_mutex_unlock(&mu_);
  return ret;
}

}  // namespace polar_race

