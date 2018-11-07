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

  static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

  std::string str;
  for (int i = 0; i < 67; i++) {
    char buf[4096];
    memset(buf, i % 26 + 'a', 4096);
    str = "";
    for (int j = 0; j < 8; j++) {
      str[j] = i % 26 + 'a';
    }
    std::cout << "i = " << i << " get_str = " << str << std::endl;
    ret = engine->Write(str.c_str(), buf);
  }

  std::string value;
  for (int i = 0; i < 67; i++) {
    str = "";
    for (int j = 0; j < 8; j++) {
      str[j] = i % 26 + 'a';
    }
    std::cout << "i = " << i << " get_str = " << str.c_str() << std::endl;
    ret = engine->Read(str.c_str(), &value);
    std::cout << "value = " << value << std::endl;
  }


  delete engine;
  return 0;
}
