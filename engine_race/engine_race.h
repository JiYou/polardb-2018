// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include "include/engine.h"
#include "engine_race/util.h"
#include "engine_race/door_plate.h"
#include "engine_race/data_store.h"

#include <pthread.h>
#include <string>

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir)
    : mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(NULL), plate_(dir), store_(dir) {
    }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

 private:
  pthread_mutex_t mu_;
  FileLock* db_lock_;
  DoorPlate plate_;
  DataStore store_;
};

}  // namespace polar_race
