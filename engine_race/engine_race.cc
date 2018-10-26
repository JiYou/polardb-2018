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

  *eptr = engine_race;
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
  // firstly, write the file content.
  RetCode ret = store_.Append(value.ToString(), &location);
  if (ret == kSucc) {
    // then write the <key,pos> into hash map.
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

