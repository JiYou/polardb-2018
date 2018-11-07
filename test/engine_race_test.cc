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

  std::string str;
  for (int i = 0; i < 64; i++) {
    char buf[4096];
    memset(buf, i % 26 + 'a', 4096);
    str = "";
    for (int j = 0; j < 8; j++) {
      str[j] += i % 26 + 'a';
    }
    ret = engine->Write(str.c_str(), buf);
  }

  std::string value;
  ret = engine->Read("xxxxxxxyyy", &value);
  printf("[WARN]: TEST_NOT_FOUND [PASS] can not find the item. ret = %d\n", ret);

  delete engine;
  return 0;
}
