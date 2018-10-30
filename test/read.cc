#include "include/engine.h"
#include <assert.h>
#include <stdio.h>
#include <string>
#include <iostream>

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
  char V[4096];
  memset(V, 'a', sizeof(V));
  int cnt = 0;
  constexpr int kMaxCnt = 2176520;
  std::string front = "a";
  for (char i = 'a'; i < 'z'; i++) {
    std::string A = front + i;
    for (char j = 'a'; j < 'z'; j++) {
      std::string B = A + j;
      for (char k = 'a'; k < 'z'; k++) {
        std::string C = B + k;
        for (char l = 'a'; l < 'z'; l++) {
          std::string D = C + l;
          for (char m = 'a'; m < 'z'; m++) {
            std::string E = D + m;
            for (char n = 'a'; n < 'z'; n++) {
              std::string F = E + n;
              for (char o = 'a'; o < 'z'; o++) {
                cnt ++;
                // 2176525
                if (cnt > kMaxCnt) return 0;
                if (cnt % 10000 == 0)  std::cout << "cnt = " << cnt << std::endl;
                std::string G = F + o;
                std::string X;
                ret = engine->Read(G, &X);
                auto cret = memcmp(V, X.c_str(), 4096);
                if (cret != 0) {
                  std::cout << G << std::endl;
                  std::cout << "ret = " << cret << std::endl;
                  for (int i = 0; i < X.length(); i++) {
                    if (X[i] != 'a') {
                      std::cout << "pos:" << i << ",val=" << X[i] << std::endl;
                    }
                  }
                }
                assert (cret == 0);
              }
            }
          }
        }
      }
    }
  }

  std::string value;
  ret = engine->Read("xxxxxxxyyy", &value);
  printf("[WARN]: can not find the item. ret = %d\n", ret);

  delete engine;
  return 0;
}
