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
  EngineRace *engine_example = new EngineRace(name);

  RetCode ret = engine_example->plate_.Init();
  if (ret != kSucc) {
    delete engine_example;
    return ret;
  }
  ret = engine_example->store_.Init();
  if (ret != kSucc) {
    delete engine_example;
    return ret;
  }

  if (0 != LockFile(name + "/" + kLockFile, &(engine_example->db_lock_))) {
    delete engine_example;
    return kIOError;
  }

  *eptr = engine_example;
  return kSucc;
}

EngineRace::~EngineRace() {
  if (db_lock_) {
    UnlockFile(db_lock_);
  }
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  pthread_mutex_lock(&mu_);
  Location location;
  // 这里分了两步，第一步是先把value放到文件中
  // 这里分了两步
  // 如果数据写烂了怎么办？
  printf("begin to append value into file.\n");
  RetCode ret = store_.Append(value.ToString(), &location);
  if (ret == kSucc) {
    // 这里再把key添加到hash文件里面
    printf("begin to insert into hash map.\n");
    ret = plate_.AddOrUpdate(key.ToString(), location);
  }
  pthread_mutex_unlock(&mu_);
  return ret;
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

