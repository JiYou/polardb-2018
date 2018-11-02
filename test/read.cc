#include "include/engine.h"
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

static constexpr int kMaxThread = 64;
static constexpr int kMaxCnt = 2176520 / kMaxThread;

static std::mutex mu;
static std::condition_variable cond;
static std::atomic<int> write_cnt {0};

void read_thread(Engine *engine, char begin_char) {
  int cnt = 0;
  char V[4096];
  memset(V, 'a', sizeof(V));

  std::string front;
  front += begin_char;
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
                if (cnt > kMaxCnt) {
                  std::unique_lock<std::mutex> l(mu);
                  write_cnt++;
                  cond.notify_all();
                  return;
                }
                if (cnt % 1000 == 0) std::cout << "cnt = " << cnt << std::endl;
                std::string G = F + o;
                std::string X;
                auto ret = engine->Read(G, &X);
                assert (ret == kSucc);
                auto cret = memcmp(V, X.c_str(), 4096);
                if (cret != 0) {
                  std::cout << G << std::endl;
                  std::cout << "ret = " << cret << std::endl;
                  for (int i = 0; i < X.length(); i++) {
                    if (X[i] != 'a') {
                      std::cout << "pos:" << i << ",val=" << X[i] << std::endl;
                      assert (0);
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
}

int main() {
  Engine *engine = NULL;

  RetCode ret = Engine::Open(kEnginePath, &engine);
  assert (ret == kSucc);


  static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "~`!@#$%^&*()_+=-,./;:<>";

  for (int i = 0; i < kMaxThread; i++) {
    auto front_char = alphanum[i];
    std::thread thd(read_thread, engine, front_char);
    thd.detach();
  }

  std::unique_lock<std::mutex> l(mu);
  cond.wait(l, [&] { return write_cnt == kMaxThread; });

  std::string value;
  ret = engine->Read("xxxxxxxyyy", &value);
  printf("[WARN]: TEST_NOT_FOUND [PASS] can not find the item. ret = %d\n", ret);

  delete engine;
  return 0;
}
