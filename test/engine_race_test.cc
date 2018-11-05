#include "include/engine.h"
#include "engine_race/util.h"

#include <assert.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";

using namespace polar_race;

int main() {
  Engine *engine = NULL;

  RetCode ret = Engine::Open(kEnginePath, &engine);
  assert (ret == kSucc);

  std::string value;
  ret = engine->Read("xxxxxxxyyy", &value);
  printf("[WARN]: TEST_NOT_FOUND [PASS] can not find the item. ret = %d\n", ret);

  delete engine;
  return 0;
}
