#include "include/engine.h"
#include <assert.h>
#include <stdio.h>
#include <string>

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";

using namespace polar_race;

class DumpVisitor : public Visitor {
public:
  DumpVisitor(int* kcnt)
    : key_cnt_(kcnt) {}

  ~DumpVisitor() {}

  void Visit(const PolarString& key, const PolarString& value) {
    printf("Visit %s --> %s\n", key.data(), value.data());
    (*key_cnt_)++;
  }
private:
  int* key_cnt_;
};

int main() {
  Engine *engine = NULL;

  RetCode ret = Engine::Open(kEnginePath, &engine);
  assert (ret == kSucc);


  ret = engine->Write("aaa", "aaaaaaaaaaa");
  assert (ret == kSucc);
  ret = engine->Write("aab", "111111111111111111111111111111111111111111");
  ret = engine->Write("aac", "2222222");
  ret = engine->Write("aad", "33333333333333333333");
  ret = engine->Write("aae", "4");
  ret = engine->Write("aaf", "111111111111111111111111111111111111111111");
  ret = engine->Write("aag", "111111111111111111111111111111111111111111");

  ret = engine->Write("aah", "2222222");
  ret = engine->Write("aai", "33333333333333333333");
  ret = engine->Write("aaj", "4");

  ret = engine->Write("aak", "111111111111111111111111111111111111111111");
  ret = engine->Write("aal", "2222222");
  ret = engine->Write("aam", "33333333333333333333");
  ret = engine->Write("aan", "4");

  ret = engine->Write("aao", "111111111111111111111111111111111111111111");
  ret = engine->Write("aap", "2222222");
  ret = engine->Write("aaq", "33333333333333333333");

  ret = engine->Write("aar", "111111111111111111111111111111111111111111");
  ret = engine->Write("aas", "2222222");
  ret = engine->Write("aat", "33333333333333333333");
  ret = engine->Write("aau", "4");


  ret = engine->Write("aav", "111111111111111111111111111111111111111111");
  ret = engine->Write("aay", "2222222");
  ret = engine->Write("aaw", "33333333333333333333");
  ret = engine->Write("aax", "4");
  ret = engine->Write("aaz", "111111111111111111111111111111111111111111");



  ret = engine->Write("bbb", "bbbbbbbbbbbb");
  assert (ret == kSucc);

  ret = engine->Write("ccd", "cbbbbbbbbbbbb");
  std::string value;
  ret = engine->Read("aaa", &value);

  std::string x = "aa";
  for (char i = 'a'; i <= 'd'; i++) {
    auto k = x + i;
    ret = engine->Read(k, &value);
    printf("to find %s\n", k.c_str());
    assert (ret == kSucc);
  }

  for (char i = 'a'; i <= 'z'; i++) {
    auto k = x + i;
    ret = engine->Read(k, &value);
    printf("to find %s\n", k.c_str());
    assert (ret == kSucc);
  }

  printf("Read aaa value: %s\n", value.c_str());
  ret = engine->Read("bbb", &value);
  assert (ret == kSucc);
  printf("Read bbb value: %s\n", value.c_str());

  int key_cnt = 0;
  DumpVisitor vistor(&key_cnt);
  ret = engine->Range("b", "", vistor);
  assert (ret == kSucc);
  printf("Range key cnt: %d\n", key_cnt);

  ret = engine->Read("xxxxxxxyyy", &value);
  printf("[WARN]: can not find the item. ret = %d\n", ret);

  delete engine;
  return 0;
}
